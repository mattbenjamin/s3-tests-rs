//! Notification topics, and delivery to Kafka and HTTP endpoints.
//!
//! Two sources.  Four tests -- the account and cross-account topic
//! ones -- are translated from the vendored
//! `src/test/rgw/s3-tests/s3tests/functional/test_sns.py` and keep its
//! names.
//!
//! The rest derive from the Ceph RGW notification test program
//! `src/test/rgw/bucket_notification/test_bn.py`, which is 87 tests
//! across AMQP, Kafka, persistent topics and multisite.  These are the
//! Kafka subset, renamed to `test_notification_kafka_*` because the
//! correspondence is by behaviour rather than one-to-one:  filters,
//! event fields, multipart, copy, multi-delete, topic attributes.
//! Persistent topics, AMQP and multisite are deliberately not ported.
//! The Kafka tests need a Kafka broker, and are gated behind
//! `has_kafka`.  `rgw-vstart.sh --with-kafka` starts one where they look
//! by default;  `[kafka] broker` in the config names another.
//!
//! The HTTP subset of the same program, in `http_tests`, keeps its
//! names:  the five tests whose topics are not persistent, against an
//! endpoint the test starts in its own process
//! (`s3_tests_rs::notify_receiver`), so they need nothing started and
//! run wherever the suite does.  Two more deliver over HTTPS, to the
//! receiver under a CA of its own, with and without a client
//! certificate;  they need the gateway on this host's filesystem.
//!
//! `topic_config_tests` are RGW's own:  the endpoint arguments checked
//! when a topic is created -- `kafka-config` against the gateway's
//! `rgw_kafka_config_allowed_keys`, the HTTP client certificate, and
//! the escaping of a stored value.  Each probes whether its keys are
//! listed and asserts whichever case the gateway is in.

use s3_tests_rs::client::{
    get_iam_root_s3client,
    get_sns_root_client, get_sns_alt_root_client,
    get_iam_alt_root_s3client,
};
use s3_tests_rs::fixtures::get_new_bucket_name;

async fn nuke_topics(client: &aws_sdk_sns::Client, prefix: &str) {
    let mut next_token: Option<String> = None;
    loop {
        let mut req = client.list_topics();
        if let Some(ref token) = next_token {
            req = req.next_token(token);
        }
        let resp = match req.send().await {
            Ok(r) => r,
            Err(_) => break,
        };
        for topic in resp.topics() {
            if let Some(arn) = topic.topic_arn() {
                if arn.contains(prefix) {
                    let _ = client.delete_topic().topic_arn(arn).send().await;
                }
            }
        }
        next_token = resp.next_token().map(|s| s.to_string());
        if next_token.is_none() {
            break;
        }
    }
}

fn get_topic_prefix() -> String {
    let cfg = s3_tests_rs::config::get_config();
    cfg.bucket_prefix.replace("{random}", "").trim_matches('-').to_string()
}

#[tokio::test]
async fn test_account_topic() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let sns = get_sns_root_client();
    let name = get_new_bucket_name();

    let response = sns.create_topic().name(&name).send().await.unwrap();
    let arn = response.topic_arn().unwrap();
    assert!(arn.starts_with("arn:aws:sns:"), "ARN should start with arn:aws:sns:, got: {arn}");
    assert!(arn.ends_with(&format!(":{name}")), "ARN should end with :{name}, got: {arn}");

    let response = sns.list_topics().send().await.unwrap();
    let arns: Vec<&str> = response.topics().iter()
        .filter_map(|t| t.topic_arn())
        .collect();
    assert!(arns.contains(&arn), "created topic not in list");

    sns.set_topic_attributes()
        .topic_arn(arn)
        .attribute_name("Policy")
        .attribute_value("")
        .send()
        .await
        .unwrap();

    let response = sns.get_topic_attributes()
        .topic_arn(arn)
        .send()
        .await
        .unwrap();
    assert!(response.attributes().is_some());

    sns.delete_topic().topic_arn(arn).send().await.unwrap();

    let response = sns.list_topics().send().await.unwrap();
    let arns: Vec<&str> = response.topics().iter()
        .filter_map(|t| t.topic_arn())
        .collect();
    assert!(!arns.contains(&arn), "deleted topic still in list");

    let result = sns.get_topic_attributes()
        .topic_arn(arn)
        .send()
        .await;
    assert!(result.is_err(), "expected NotFound for deleted topic");

    // delete of already-deleted topic should succeed
    sns.delete_topic().topic_arn(arn).send().await.unwrap();

    nuke_topics(&sns, &get_topic_prefix()).await;
}

#[tokio::test]
async fn test_cross_account_topic() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let sns = get_sns_root_client();
    let sns_alt = get_sns_alt_root_client();
    let name = get_new_bucket_name();

    let arn = sns.create_topic().name(&name).send().await.unwrap()
        .topic_arn().unwrap().to_string();

    // alt user should not be authorized
    let result = sns_alt.get_topic_attributes().topic_arn(&arn).send().await;
    assert!(result.is_err(), "expected AuthorizationError for alt user get_topic_attributes");

    let result = sns_alt.set_topic_attributes()
        .topic_arn(&arn)
        .attribute_name("Policy")
        .attribute_value("")
        .send()
        .await;
    assert!(result.is_err(), "expected AuthorizationError for alt user set_topic_attributes");

    let result = sns_alt.delete_topic().topic_arn(&arn).send().await;
    assert!(result.is_err(), "expected AuthorizationError for alt user delete_topic");

    sns.delete_topic().topic_arn(&arn).send().await.unwrap();

    let response = sns_alt.list_topics().send().await.unwrap();
    let arns: Vec<&str> = response.topics().iter()
        .filter_map(|t| t.topic_arn())
        .collect();
    assert!(!arns.contains(&arn.as_str()), "deleted topic visible to alt user");

    nuke_topics(&sns, &get_topic_prefix()).await;
    nuke_topics(&sns_alt, &get_topic_prefix()).await;
}

#[tokio::test]
async fn test_account_topic_publish() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let sns = get_sns_root_client();
    let s3 = get_iam_root_s3client();
    let name = get_new_bucket_name();

    let topic_arn = sns.create_topic().name(&name).send().await.unwrap()
        .topic_arn().unwrap().to_string();

    let bucket = get_new_bucket_name();
    s3.create_bucket().bucket(&bucket).send().await.unwrap();

    s3.put_bucket_notification_configuration()
        .bucket(&bucket)
        .notification_configuration(
            aws_sdk_s3::types::NotificationConfiguration::builder()
                .topic_configurations(
                    aws_sdk_s3::types::TopicConfiguration::builder()
                        .id("id")
                        .topic_arn(&topic_arn)
                        .events(aws_sdk_s3::types::Event::S3ObjectCreated)
                        .build()
                        .unwrap(),
                )
                .build(),
        )
        .send()
        .await
        .unwrap();

    nuke_topics(&sns, &get_topic_prefix()).await;
}

#[tokio::test]
async fn test_cross_account_topic_publish() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let sns = get_sns_root_client();
    let s3_alt = get_iam_alt_root_s3client();
    let name = get_new_bucket_name();

    let topic_arn = sns.create_topic().name(&name).send().await.unwrap()
        .topic_arn().unwrap().to_string();

    let bucket = get_new_bucket_name();
    s3_alt.create_bucket().bucket(&bucket).send().await.unwrap();

    let config = aws_sdk_s3::types::NotificationConfiguration::builder()
        .topic_configurations(
            aws_sdk_s3::types::TopicConfiguration::builder()
                .id("id")
                .topic_arn(&topic_arn)
                .events(aws_sdk_s3::types::Event::S3ObjectCreated)
                .build()
                .unwrap(),
        )
        .build();

    // expect AccessDenied because no resource policy allows cross-account access
    let result = s3_alt
        .put_bucket_notification_configuration()
        .bucket(&bucket)
        .notification_configuration(config.clone())
        .send()
        .await;
    assert!(result.is_err(), "expected AccessDenied for cross-account publish");

    // get alt user's ARN
    let alt_iam = s3_tests_rs::client::get_iam_alt_root_client();
    let alt_user = alt_iam.get_user().send().await.unwrap();
    let alt_principal = alt_user.user().unwrap().arn();

    // add topic policy to allow the alt user
    let policy = serde_json::json!({
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Principal": {"AWS": alt_principal},
            "Action": "sns:Publish",
            "Resource": &topic_arn
        }]
    });
    sns.set_topic_attributes()
        .topic_arn(&topic_arn)
        .attribute_name("Policy")
        .attribute_value(policy.to_string())
        .send()
        .await
        .unwrap();

    // now cross-account publish should succeed
    s3_alt
        .put_bucket_notification_configuration()
        .bucket(&bucket)
        .notification_configuration(config)
        .send()
        .await
        .unwrap();

    nuke_topics(&sns, &get_topic_prefix()).await;
}

/* --- Kafka delivery tests --- */

#[cfg(feature = "has_kafka")]
mod kafka_tests {
    use super::*;
    use aws_sdk_s3::primitives::ByteStream;
    use rdkafka::consumer::{Consumer, StreamConsumer};
    use rdkafka::config::ClientConfig;
    use rdkafka::Message;
    use std::time::Duration;
    use tokio::time::timeout;

    fn get_kafka_broker() -> String {
        let cfg = s3_tests_rs::config::get_config();
        cfg.kafka_broker.clone()
    }

    fn make_consumer(broker: &str, topic: &str) -> StreamConsumer {
        let consumer: StreamConsumer = ClientConfig::new()
            .set("bootstrap.servers", broker)
            .set("group.id", &format!("s3tests-{}", rand::random::<u32>()))
            .set("auto.offset.reset", "earliest")
            .set("enable.auto.commit", "false")
            .create()
            .expect("failed to create Kafka consumer");
        consumer.subscribe(&[topic])
            .expect("failed to subscribe to topic");
        consumer
    }

    async fn wait_for_event(consumer: &StreamConsumer, key: &str, event_prefix: &str) -> serde_json::Value {
        timeout(Duration::from_secs(10), async {
            loop {
                match consumer.recv().await {
                    Ok(m) => {
                        if let Some(payload) = m.payload() {
                            if let Ok(event) = serde_json::from_slice::<serde_json::Value>(payload) {
                                if let Some(record) = event.get("Records")
                                    .and_then(|r| r.as_array())
                                    .and_then(|a| a.first())
                                {
                                    let obj_key = record["s3"]["object"]["key"]
                                        .as_str().unwrap_or("");
                                    let event_name = record["eventName"]
                                        .as_str().unwrap_or("");
                                    if obj_key == key && event_name.starts_with(event_prefix) {
                                        return event;
                                    }
                                }
                            }
                        }
                    }
                    Err(_) => continue,
                }
            }
        }).await.expect("timed out waiting for Kafka notification")
    }

    #[tokio::test]
    async fn test_notification_kafka_basic() {
        let _guard = s3_tests_rs::fixtures::TestGuard::setup();
        let sns = get_sns_root_client();
        let s3 = get_iam_root_s3client();
        let broker = get_kafka_broker();
        let topic_name = get_new_bucket_name();
        let bucket = get_new_bucket_name();

        let topic_arn = sns.create_topic()
            .name(&topic_name)
            .attributes("push-endpoint", format!("kafka://{}", broker))
            .attributes("kafka-ack-level", "broker")
            .send().await.unwrap()
            .topic_arn().unwrap().to_string();

        s3.create_bucket().bucket(&bucket).send().await.unwrap();

        s3.put_bucket_notification_configuration()
            .bucket(&bucket)
            .notification_configuration(
                aws_sdk_s3::types::NotificationConfiguration::builder()
                    .topic_configurations(
                        aws_sdk_s3::types::TopicConfiguration::builder()
                            .id("kafka-test")
                            .topic_arn(&topic_arn)
                            .events(aws_sdk_s3::types::Event::S3ObjectCreated)
                            .build()
                            .unwrap(),
                    )
                    .build(),
            )
            .send().await.unwrap();

        let consumer = make_consumer(&broker, &topic_name);

        let obj_key = "test-kafka-object";
        s3.put_object()
            .bucket(&bucket)
            .key(obj_key)
            .body(ByteStream::from_static(b"hello kafka"))
            .send().await.unwrap();

        let msg = wait_for_event(&consumer, obj_key, "ObjectCreated:").await;

        let record = &msg["Records"][0];
        assert_eq!(record["s3"]["object"]["key"].as_str().unwrap(), obj_key);
        assert_eq!(record["s3"]["bucket"]["name"].as_str().unwrap(), bucket);

        nuke_topics(&sns, &get_topic_prefix()).await;
    }

    /* a topic's kafka-config reaches its producer:  one setting only
     * the producer acts on delivers, and one that makes the event too
     * large for the producer does not */
    #[tokio::test]
    async fn test_notification_kafka_config() {
        use super::topic_config_tests::{allowed, create, UNLISTED};
        let _guard = s3_tests_rs::fixtures::TestGuard::setup();
        let sns = get_sns_root_client();
        let s3 = get_iam_root_s3client();
        let broker = get_kafka_broker();
        let endpoint = format!("kafka://{}", broker);

        for (key, value, delivers) in [("client.id", "s3-tests-rs", true),
                                       ("message.max.bytes", "1000", false)] {
            let config = serde_json::json!({ key: value }).to_string();
            if !allowed(&sns, key, value).await {
                let m = create(&sns, &[("push-endpoint", &endpoint), ("kafka-config", &config)])
                    .await.expect_err("an unlisted key was accepted");
                assert!(m.contains(UNLISTED), "{m}");
                continue;
            }
            let topic_name = get_new_bucket_name();
            let bucket = get_new_bucket_name();
            let topic_arn = sns.create_topic()
                .name(&topic_name)
                .attributes("push-endpoint", &endpoint)
                .attributes("kafka-ack-level", "broker")
                .attributes("kafka-config", &config)
                .send().await.unwrap()
                .topic_arn().unwrap().to_string();
            s3.create_bucket().bucket(&bucket).send().await.unwrap();
            s3.put_bucket_notification_configuration()
                .bucket(&bucket)
                .notification_configuration(
                    aws_sdk_s3::types::NotificationConfiguration::builder()
                        .topic_configurations(
                            aws_sdk_s3::types::TopicConfiguration::builder()
                                .id("kafka-config")
                                .topic_arn(&topic_arn)
                                .events(aws_sdk_s3::types::Event::S3ObjectCreated)
                                .build()
                                .unwrap(),
                        )
                        .build(),
                )
                .send().await.unwrap();
            let consumer = make_consumer(&broker, &topic_name);
            /* the event carries the metadata, so it is over 1000 bytes */
            let obj_key = "kafka-config".to_string();
            s3.put_object().bucket(&bucket).key(&obj_key)
                .metadata("pad", "p".repeat(1000))
                .body(ByteStream::from_static(b"kafka-config"))
                .send().await.unwrap();
            /* inside wait_for_event()'s own ten seconds, which panic */
            let got = timeout(Duration::from_secs(8),
                              wait_for_event(&consumer, &obj_key, "ObjectCreated:")).await;
            assert_eq!(got.is_ok(), delivers, "{key}={value}");
            s3.delete_object().bucket(&bucket).key(&obj_key).send().await.unwrap();
            sns.delete_topic().topic_arn(&topic_arn).send().await.unwrap();
            s3.delete_bucket().bucket(&bucket).send().await.unwrap();
        }
    }

    #[tokio::test]
    async fn test_notification_kafka_delete() {
        let _guard = s3_tests_rs::fixtures::TestGuard::setup();
        let sns = get_sns_root_client();
        let s3 = get_iam_root_s3client();
        let broker = get_kafka_broker();
        let topic_name = get_new_bucket_name();
        let bucket = get_new_bucket_name();

        let topic_arn = sns.create_topic()
            .name(&topic_name)
            .attributes("push-endpoint", format!("kafka://{}", broker))
            .attributes("kafka-ack-level", "broker")
            .send().await.unwrap()
            .topic_arn().unwrap().to_string();

        s3.create_bucket().bucket(&bucket).send().await.unwrap();

        s3.put_bucket_notification_configuration()
            .bucket(&bucket)
            .notification_configuration(
                aws_sdk_s3::types::NotificationConfiguration::builder()
                    .topic_configurations(
                        aws_sdk_s3::types::TopicConfiguration::builder()
                            .id("kafka-delete")
                            .topic_arn(&topic_arn)
                            .events(aws_sdk_s3::types::Event::S3ObjectRemoved)
                            .build()
                            .unwrap(),
                    )
                    .build(),
            )
            .send().await.unwrap();

        let obj_key = "test-kafka-delete";
        s3.put_object()
            .bucket(&bucket)
            .key(obj_key)
            .body(ByteStream::from_static(b"delete me"))
            .send().await.unwrap();

        let consumer = make_consumer(&broker, &topic_name);

        s3.delete_object()
            .bucket(&bucket)
            .key(obj_key)
            .send().await.unwrap();

        let msg = wait_for_event(&consumer, obj_key, "ObjectRemoved:").await;

        let record = &msg["Records"][0];
        assert_eq!(record["s3"]["object"]["key"].as_str().unwrap(), obj_key);

        nuke_topics(&sns, &get_topic_prefix()).await;
    }

    async fn setup_notification_with_ack(
        sns: &aws_sdk_sns::Client,
        s3: &aws_sdk_s3::Client,
        broker: &str,
        event: aws_sdk_s3::types::Event,
        ack_level: &str,
    ) -> (String, String, String) {
        let topic_name = get_new_bucket_name();
        let bucket = get_new_bucket_name();

        let topic_arn = sns.create_topic()
            .name(&topic_name)
            .attributes("push-endpoint", format!("kafka://{}", broker))
            .attributes("kafka-ack-level", ack_level)
            .send().await.unwrap()
            .topic_arn().unwrap().to_string();

        s3.create_bucket().bucket(&bucket).send().await.unwrap();

        s3.put_bucket_notification_configuration()
            .bucket(&bucket)
            .notification_configuration(
                aws_sdk_s3::types::NotificationConfiguration::builder()
                    .topic_configurations(
                        aws_sdk_s3::types::TopicConfiguration::builder()
                            .id("kafka-notif")
                            .topic_arn(&topic_arn)
                            .events(event)
                            .build()
                            .unwrap(),
                    )
                    .build(),
            )
            .send().await.unwrap();

        (topic_name, bucket, topic_arn)
    }

    async fn setup_notification(
        sns: &aws_sdk_sns::Client,
        s3: &aws_sdk_s3::Client,
        broker: &str,
        event: aws_sdk_s3::types::Event,
    ) -> (String, String, String) {
        setup_notification_with_ack(sns, s3, broker, event, "broker").await
    }

    #[tokio::test]
    async fn test_notification_kafka_event_fields() {
        let _guard = s3_tests_rs::fixtures::TestGuard::setup();
        let sns = get_sns_root_client();
        let s3 = get_iam_root_s3client();
        let broker = get_kafka_broker();

        let (topic_name, bucket, _) = setup_notification(
            &sns, &s3, &broker, aws_sdk_s3::types::Event::S3ObjectCreated,
        ).await;

        let consumer = make_consumer(&broker, &topic_name);

        let obj_key = "test-fields/subdir/obj.txt";
        let body = b"event field validation body";
        s3.put_object()
            .bucket(&bucket)
            .key(obj_key)
            .body(ByteStream::from_static(body))
            .send().await.unwrap();

        let msg = wait_for_event(&consumer, obj_key, "ObjectCreated:").await;
        let record = &msg["Records"][0];

        assert_eq!(record["eventVersion"].as_str().unwrap(), "2.2");
        assert_eq!(record["eventSource"].as_str().unwrap(), "ceph:s3");
        assert!(record["eventName"].as_str().unwrap().starts_with("ObjectCreated:"));
        assert_eq!(record["s3"]["bucket"]["name"].as_str().unwrap(), bucket);
        assert_eq!(record["s3"]["object"]["key"].as_str().unwrap(), obj_key);
        assert_eq!(record["s3"]["object"]["size"].as_u64().unwrap(), body.len() as u64);
        assert!(!record["s3"]["object"]["eTag"].as_str().unwrap_or("").is_empty());
        assert!(!record["userIdentity"]["principalId"].as_str().unwrap_or("").is_empty());
        assert!(!record["requestParameters"]["sourceIPAddress"].as_str().unwrap_or("").is_empty()
            || record["userIdentity"]["principalId"].as_str().is_some());

        nuke_topics(&sns, &get_topic_prefix()).await;
    }

    #[tokio::test]
    async fn test_notification_kafka_copy() {
        let _guard = s3_tests_rs::fixtures::TestGuard::setup();
        let sns = get_sns_root_client();
        let s3 = get_iam_root_s3client();
        let broker = get_kafka_broker();

        let (topic_name, bucket, _) = setup_notification(
            &sns, &s3, &broker, aws_sdk_s3::types::Event::S3ObjectCreated,
        ).await;

        let consumer = make_consumer(&broker, &topic_name);

        let src_key = "copy-source";
        s3.put_object()
            .bucket(&bucket)
            .key(src_key)
            .body(ByteStream::from_static(b"copy source data"))
            .send().await.unwrap();

        let _ = wait_for_event(&consumer, src_key, "ObjectCreated:").await;

        let dst_key = "copy-dest";
        s3.copy_object()
            .bucket(&bucket)
            .key(dst_key)
            .copy_source(format!("{}/{}", bucket, src_key))
            .send().await.unwrap();

        let msg = wait_for_event(&consumer, dst_key, "ObjectCreated:").await;
        let record = &msg["Records"][0];
        assert_eq!(record["s3"]["object"]["key"].as_str().unwrap(), dst_key);

        nuke_topics(&sns, &get_topic_prefix()).await;
    }

    #[tokio::test]
    async fn test_notification_kafka_multi_delete() {
        let _guard = s3_tests_rs::fixtures::TestGuard::setup();
        let sns = get_sns_root_client();
        let s3 = get_iam_root_s3client();
        let broker = get_kafka_broker();

        let (topic_name, bucket, _) = setup_notification(
            &sns, &s3, &broker, aws_sdk_s3::types::Event::S3ObjectRemoved,
        ).await;

        let keys = ["multi-del-1", "multi-del-2", "multi-del-3"];
        for key in &keys {
            s3.put_object()
                .bucket(&bucket)
                .key(*key)
                .body(ByteStream::from_static(b"x"))
                .send().await.unwrap();
        }

        let consumer = make_consumer(&broker, &topic_name);

        let objects: Vec<_> = keys.iter().map(|k|
            aws_sdk_s3::types::ObjectIdentifier::builder().key(*k).build().unwrap()
        ).collect();
        s3.delete_objects()
            .bucket(&bucket)
            .delete(aws_sdk_s3::types::Delete::builder()
                .set_objects(Some(objects))
                .build().unwrap())
            .send().await.unwrap();

        let mut received_keys = std::collections::HashSet::new();
        for _ in 0..keys.len() {
            let msg = timeout(Duration::from_secs(10), async {
                loop {
                    match consumer.recv().await {
                        Ok(m) => {
                            if let Some(payload) = m.payload() {
                                if let Ok(event) = serde_json::from_slice::<serde_json::Value>(payload) {
                                    if let Some(record) = event.get("Records")
                                        .and_then(|r| r.as_array())
                                        .and_then(|a| a.first())
                                    {
                                        if record["eventName"].as_str().unwrap_or("").starts_with("ObjectRemoved:") {
                                            return record["s3"]["object"]["key"].as_str().unwrap().to_string();
                                        }
                                    }
                                }
                            }
                        }
                        Err(_) => continue,
                    }
                }
            }).await.expect("timed out waiting for multi-delete notification");
            received_keys.insert(msg);
        }

        for key in &keys {
            assert!(received_keys.contains(*key), "missing delete event for {key}");
        }

        nuke_topics(&sns, &get_topic_prefix()).await;
    }

    #[tokio::test]
    async fn test_notification_kafka_no_match() {
        let _guard = s3_tests_rs::fixtures::TestGuard::setup();
        let sns = get_sns_root_client();
        let s3 = get_iam_root_s3client();
        let broker = get_kafka_broker();

        let (topic_name, bucket, _) = setup_notification(
            &sns, &s3, &broker, aws_sdk_s3::types::Event::S3ObjectRemoved,
        ).await;

        let consumer = make_consumer(&broker, &topic_name);

        s3.put_object()
            .bucket(&bucket)
            .key("should-not-notify")
            .body(ByteStream::from_static(b"no event expected"))
            .send().await.unwrap();

        let result = timeout(Duration::from_secs(3), async {
            loop {
                match consumer.recv().await {
                    Ok(m) => {
                        if let Some(payload) = m.payload() {
                            if let Ok(event) = serde_json::from_slice::<serde_json::Value>(payload) {
                                if event.get("Records").is_some() {
                                    return event;
                                }
                            }
                        }
                    }
                    Err(_) => continue,
                }
            }
        }).await;

        assert!(result.is_err(), "received unexpected notification for non-matching event type");

        nuke_topics(&sns, &get_topic_prefix()).await;
    }

    #[tokio::test]
    async fn test_notification_kafka_multipart() {
        let _guard = s3_tests_rs::fixtures::TestGuard::setup();
        let sns = get_sns_root_client();
        let s3 = get_iam_root_s3client();
        let broker = get_kafka_broker();

        let (topic_name, bucket, _) = setup_notification(
            &sns, &s3, &broker, aws_sdk_s3::types::Event::S3ObjectCreated,
        ).await;

        let consumer = make_consumer(&broker, &topic_name);

        let obj_key = "test-multipart-notify";
        let mpu = s3.create_multipart_upload()
            .bucket(&bucket)
            .key(obj_key)
            .send().await.unwrap();
        let upload_id = mpu.upload_id().unwrap();

        let part = s3.upload_part()
            .bucket(&bucket)
            .key(obj_key)
            .upload_id(upload_id)
            .part_number(1)
            .body(ByteStream::from_static(b"multipart notification test data"))
            .send().await.unwrap();

        s3.complete_multipart_upload()
            .bucket(&bucket)
            .key(obj_key)
            .upload_id(upload_id)
            .multipart_upload(
                aws_sdk_s3::types::CompletedMultipartUpload::builder()
                    .parts(
                        aws_sdk_s3::types::CompletedPart::builder()
                            .part_number(1)
                            .e_tag(part.e_tag().unwrap())
                            .build(),
                    )
                    .build(),
            )
            .send().await.unwrap();

        let msg = wait_for_event(&consumer, obj_key, "ObjectCreated:").await;
        let record = &msg["Records"][0];
        assert_eq!(record["s3"]["object"]["key"].as_str().unwrap(), obj_key);
        assert!(record["s3"]["object"]["size"].as_u64().unwrap() > 0);

        nuke_topics(&sns, &get_topic_prefix()).await;
    }

    #[tokio::test]
    async fn test_notification_kafka_filter_prefix() {
        let _guard = s3_tests_rs::fixtures::TestGuard::setup();
        let sns = get_sns_root_client();
        let s3 = get_iam_root_s3client();
        let broker = get_kafka_broker();
        let topic_name = get_new_bucket_name();
        let bucket = get_new_bucket_name();

        let topic_arn = sns.create_topic()
            .name(&topic_name)
            .attributes("push-endpoint", format!("kafka://{}", broker))
            .attributes("kafka-ack-level", "broker")
            .send().await.unwrap()
            .topic_arn().unwrap().to_string();

        s3.create_bucket().bucket(&bucket).send().await.unwrap();

        s3.put_bucket_notification_configuration()
            .bucket(&bucket)
            .notification_configuration(
                aws_sdk_s3::types::NotificationConfiguration::builder()
                    .topic_configurations(
                        aws_sdk_s3::types::TopicConfiguration::builder()
                            .id("prefix-filter")
                            .topic_arn(&topic_arn)
                            .events(aws_sdk_s3::types::Event::S3ObjectCreated)
                            .filter(
                                aws_sdk_s3::types::NotificationConfigurationFilter::builder()
                                    .key(
                                        aws_sdk_s3::types::S3KeyFilter::builder()
                                            .filter_rules(
                                                aws_sdk_s3::types::FilterRule::builder()
                                                    .name(aws_sdk_s3::types::FilterRuleName::Prefix)
                                                    .value("logs/")
                                                    .build(),
                                            )
                                            .build(),
                                    )
                                    .build(),
                            )
                            .build()
                            .unwrap(),
                    )
                    .build(),
            )
            .send().await.unwrap();

        let consumer = make_consumer(&broker, &topic_name);

        s3.put_object()
            .bucket(&bucket)
            .key("data/no-match.txt")
            .body(ByteStream::from_static(b"should not notify"))
            .send().await.unwrap();

        s3.put_object()
            .bucket(&bucket)
            .key("logs/access.log")
            .body(ByteStream::from_static(b"should notify"))
            .send().await.unwrap();

        let msg = wait_for_event(&consumer, "logs/access.log", "ObjectCreated:").await;
        assert_eq!(msg["Records"][0]["s3"]["object"]["key"].as_str().unwrap(), "logs/access.log");

        let no_match = timeout(Duration::from_secs(3), async {
            loop {
                match consumer.recv().await {
                    Ok(m) => {
                        if let Some(payload) = m.payload() {
                            if let Ok(event) = serde_json::from_slice::<serde_json::Value>(payload) {
                                if let Some(record) = event.get("Records")
                                    .and_then(|r| r.as_array())
                                    .and_then(|a| a.first())
                                {
                                    if record["s3"]["object"]["key"].as_str() == Some("data/no-match.txt") {
                                        return true;
                                    }
                                }
                            }
                        }
                    }
                    Err(_) => continue,
                }
            }
        }).await;
        assert!(no_match.is_err(), "received notification for non-matching prefix");

        nuke_topics(&sns, &get_topic_prefix()).await;
    }

    #[tokio::test]
    async fn test_notification_kafka_multiple_topics() {
        let _guard = s3_tests_rs::fixtures::TestGuard::setup();
        let sns = get_sns_root_client();
        let s3 = get_iam_root_s3client();
        let broker = get_kafka_broker();

        let create_topic = get_new_bucket_name();
        let delete_topic = get_new_bucket_name();
        let bucket = get_new_bucket_name();

        let create_arn = sns.create_topic()
            .name(&create_topic)
            .attributes("push-endpoint", format!("kafka://{}", broker))
            .attributes("kafka-ack-level", "broker")
            .send().await.unwrap()
            .topic_arn().unwrap().to_string();

        let delete_arn = sns.create_topic()
            .name(&delete_topic)
            .attributes("push-endpoint", format!("kafka://{}", broker))
            .attributes("kafka-ack-level", "broker")
            .send().await.unwrap()
            .topic_arn().unwrap().to_string();

        s3.create_bucket().bucket(&bucket).send().await.unwrap();

        s3.put_bucket_notification_configuration()
            .bucket(&bucket)
            .notification_configuration(
                aws_sdk_s3::types::NotificationConfiguration::builder()
                    .topic_configurations(
                        aws_sdk_s3::types::TopicConfiguration::builder()
                            .id("creates")
                            .topic_arn(&create_arn)
                            .events(aws_sdk_s3::types::Event::S3ObjectCreated)
                            .build()
                            .unwrap(),
                    )
                    .topic_configurations(
                        aws_sdk_s3::types::TopicConfiguration::builder()
                            .id("deletes")
                            .topic_arn(&delete_arn)
                            .events(aws_sdk_s3::types::Event::S3ObjectRemoved)
                            .build()
                            .unwrap(),
                    )
                    .build(),
            )
            .send().await.unwrap();

        let create_consumer = make_consumer(&broker, &create_topic);
        let delete_consumer = make_consumer(&broker, &delete_topic);

        let obj_key = "multi-topic-obj";
        s3.put_object()
            .bucket(&bucket)
            .key(obj_key)
            .body(ByteStream::from_static(b"hello"))
            .send().await.unwrap();

        let msg = wait_for_event(&create_consumer, obj_key, "ObjectCreated:").await;
        assert_eq!(msg["Records"][0]["s3"]["object"]["key"].as_str().unwrap(), obj_key);

        s3.delete_object().bucket(&bucket).key(obj_key).send().await.unwrap();

        let msg = wait_for_event(&delete_consumer, obj_key, "ObjectRemoved:").await;
        assert_eq!(msg["Records"][0]["s3"]["object"]["key"].as_str().unwrap(), obj_key);

        nuke_topics(&sns, &get_topic_prefix()).await;
    }

    #[tokio::test]
    async fn test_notification_kafka_cross_account_isolation() {
        let _guard = s3_tests_rs::fixtures::TestGuard::setup();
        let sns = get_sns_root_client();
        let sns_alt = get_sns_alt_root_client();
        let s3 = get_iam_root_s3client();
        let s3_alt = get_iam_alt_root_s3client();
        let broker = get_kafka_broker();

        let (topic_name, bucket, _) = setup_notification(
            &sns, &s3, &broker, aws_sdk_s3::types::Event::S3ObjectCreated,
        ).await;

        let (_, alt_bucket_name, _) = setup_notification(
            &sns_alt, &s3_alt, &broker, aws_sdk_s3::types::Event::S3ObjectCreated,
        ).await;

        let consumer = make_consumer(&broker, &topic_name);

        s3_alt.put_object()
            .bucket(&alt_bucket_name)
            .key("alt-obj")
            .body(ByteStream::from_static(b"alt account data"))
            .send().await.unwrap();

        s3.put_object()
            .bucket(&bucket)
            .key("my-obj")
            .body(ByteStream::from_static(b"my data"))
            .send().await.unwrap();

        let msg = wait_for_event(&consumer, "my-obj", "ObjectCreated:").await;
        assert_eq!(msg["Records"][0]["s3"]["bucket"]["name"].as_str().unwrap(), bucket);

        let leak = timeout(Duration::from_secs(3), async {
            loop {
                match consumer.recv().await {
                    Ok(m) => {
                        if let Some(payload) = m.payload() {
                            if let Ok(event) = serde_json::from_slice::<serde_json::Value>(payload) {
                                if let Some(record) = event.get("Records")
                                    .and_then(|r| r.as_array())
                                    .and_then(|a| a.first())
                                {
                                    if record["s3"]["object"]["key"].as_str() == Some("alt-obj") {
                                        return true;
                                    }
                                }
                            }
                        }
                    }
                    Err(_) => continue,
                }
            }
        }).await;
        assert!(leak.is_err(), "received event from alt account's bucket on our topic");

        nuke_topics(&sns, &get_topic_prefix()).await;
        nuke_topics(&sns_alt, &get_topic_prefix()).await;
    }

    /* --- ack-level=none (best-effort / fire-and-forget) --- */

    #[tokio::test]
    async fn test_notification_kafka_ack_none_basic() {
        let _guard = s3_tests_rs::fixtures::TestGuard::setup();
        let sns = get_sns_root_client();
        let s3 = get_iam_root_s3client();
        let broker = get_kafka_broker();

        let (topic_name, bucket, _) = setup_notification_with_ack(
            &sns, &s3, &broker, aws_sdk_s3::types::Event::S3ObjectCreated, "none",
        ).await;

        let consumer = make_consumer(&broker, &topic_name);

        let obj_key = "ack-none-basic";
        s3.put_object()
            .bucket(&bucket)
            .key(obj_key)
            .body(ByteStream::from_static(b"fire and forget"))
            .send().await.unwrap();

        let msg = wait_for_event(&consumer, obj_key, "ObjectCreated:").await;
        assert_eq!(msg["Records"][0]["s3"]["object"]["key"].as_str().unwrap(), obj_key);

        nuke_topics(&sns, &get_topic_prefix()).await;
    }

    #[tokio::test]
    async fn test_notification_kafka_ack_none_delete() {
        let _guard = s3_tests_rs::fixtures::TestGuard::setup();
        let sns = get_sns_root_client();
        let s3 = get_iam_root_s3client();
        let broker = get_kafka_broker();

        let (topic_name, bucket, _) = setup_notification_with_ack(
            &sns, &s3, &broker, aws_sdk_s3::types::Event::S3ObjectRemoved, "none",
        ).await;

        let obj_key = "ack-none-delete";
        s3.put_object()
            .bucket(&bucket)
            .key(obj_key)
            .body(ByteStream::from_static(b"will delete"))
            .send().await.unwrap();

        let consumer = make_consumer(&broker, &topic_name);

        s3.delete_object().bucket(&bucket).key(obj_key).send().await.unwrap();

        let msg = wait_for_event(&consumer, obj_key, "ObjectRemoved:").await;
        assert_eq!(msg["Records"][0]["s3"]["object"]["key"].as_str().unwrap(), obj_key);

        nuke_topics(&sns, &get_topic_prefix()).await;
    }

    #[tokio::test]
    async fn test_notification_kafka_ack_none_multi_object() {
        let _guard = s3_tests_rs::fixtures::TestGuard::setup();
        let sns = get_sns_root_client();
        let s3 = get_iam_root_s3client();
        let broker = get_kafka_broker();

        let (topic_name, bucket, _) = setup_notification_with_ack(
            &sns, &s3, &broker, aws_sdk_s3::types::Event::S3ObjectCreated, "none",
        ).await;

        let consumer = make_consumer(&broker, &topic_name);

        let keys = ["none-obj-1", "none-obj-2", "none-obj-3", "none-obj-4", "none-obj-5"];
        for key in &keys {
            s3.put_object()
                .bucket(&bucket)
                .key(*key)
                .body(ByteStream::from_static(b"x"))
                .send().await.unwrap();
        }

        let mut received = std::collections::HashSet::new();
        for _ in 0..keys.len() {
            let key = timeout(Duration::from_secs(10), async {
                loop {
                    match consumer.recv().await {
                        Ok(m) => {
                            if let Some(payload) = m.payload() {
                                if let Ok(event) = serde_json::from_slice::<serde_json::Value>(payload) {
                                    if let Some(record) = event.get("Records")
                                        .and_then(|r| r.as_array())
                                        .and_then(|a| a.first())
                                    {
                                        if record["eventName"].as_str().unwrap_or("").starts_with("ObjectCreated:") {
                                            return record["s3"]["object"]["key"].as_str().unwrap().to_string();
                                        }
                                    }
                                }
                            }
                        }
                        Err(_) => continue,
                    }
                }
            }).await.expect("timed out waiting for ack=none notification");
            received.insert(key);
        }

        for key in &keys {
            assert!(received.contains(*key), "missing ack=none event for {key}");
        }

        nuke_topics(&sns, &get_topic_prefix()).await;
    }

    /* --- suffix filter --- */

    #[tokio::test]
    async fn test_notification_kafka_filter_suffix() {
        let _guard = s3_tests_rs::fixtures::TestGuard::setup();
        let sns = get_sns_root_client();
        let s3 = get_iam_root_s3client();
        let broker = get_kafka_broker();
        let topic_name = get_new_bucket_name();
        let bucket = get_new_bucket_name();

        let topic_arn = sns.create_topic()
            .name(&topic_name)
            .attributes("push-endpoint", format!("kafka://{}", broker))
            .attributes("kafka-ack-level", "broker")
            .send().await.unwrap()
            .topic_arn().unwrap().to_string();

        s3.create_bucket().bucket(&bucket).send().await.unwrap();

        s3.put_bucket_notification_configuration()
            .bucket(&bucket)
            .notification_configuration(
                aws_sdk_s3::types::NotificationConfiguration::builder()
                    .topic_configurations(
                        aws_sdk_s3::types::TopicConfiguration::builder()
                            .id("suffix-filter")
                            .topic_arn(&topic_arn)
                            .events(aws_sdk_s3::types::Event::S3ObjectCreated)
                            .filter(
                                aws_sdk_s3::types::NotificationConfigurationFilter::builder()
                                    .key(
                                        aws_sdk_s3::types::S3KeyFilter::builder()
                                            .filter_rules(
                                                aws_sdk_s3::types::FilterRule::builder()
                                                    .name(aws_sdk_s3::types::FilterRuleName::Suffix)
                                                    .value(".jpg")
                                                    .build(),
                                            )
                                            .build(),
                                    )
                                    .build(),
                            )
                            .build()
                            .unwrap(),
                    )
                    .build(),
            )
            .send().await.unwrap();

        let consumer = make_consumer(&broker, &topic_name);

        s3.put_object()
            .bucket(&bucket)
            .key("readme.txt")
            .body(ByteStream::from_static(b"no match"))
            .send().await.unwrap();

        s3.put_object()
            .bucket(&bucket)
            .key("photo.jpg")
            .body(ByteStream::from_static(b"match"))
            .send().await.unwrap();

        let msg = wait_for_event(&consumer, "photo.jpg", "ObjectCreated:").await;
        assert_eq!(msg["Records"][0]["s3"]["object"]["key"].as_str().unwrap(), "photo.jpg");

        let no_match = timeout(Duration::from_secs(3), async {
            loop {
                match consumer.recv().await {
                    Ok(m) => {
                        if let Some(payload) = m.payload() {
                            if let Ok(event) = serde_json::from_slice::<serde_json::Value>(payload) {
                                if let Some(record) = event.get("Records")
                                    .and_then(|r| r.as_array())
                                    .and_then(|a| a.first())
                                {
                                    if record["s3"]["object"]["key"].as_str() == Some("readme.txt") {
                                        return true;
                                    }
                                }
                            }
                        }
                    }
                    Err(_) => continue,
                }
            }
        }).await;
        assert!(no_match.is_err(), "received notification for non-matching suffix");

        nuke_topics(&sns, &get_topic_prefix()).await;
    }

    /* --- all events on single topic --- */

    #[tokio::test]
    async fn test_notification_kafka_all_events() {
        let _guard = s3_tests_rs::fixtures::TestGuard::setup();
        let sns = get_sns_root_client();
        let s3 = get_iam_root_s3client();
        let broker = get_kafka_broker();
        let topic_name = get_new_bucket_name();
        let bucket = get_new_bucket_name();

        let topic_arn = sns.create_topic()
            .name(&topic_name)
            .attributes("push-endpoint", format!("kafka://{}", broker))
            .attributes("kafka-ack-level", "broker")
            .send().await.unwrap()
            .topic_arn().unwrap().to_string();

        s3.create_bucket().bucket(&bucket).send().await.unwrap();

        s3.put_bucket_notification_configuration()
            .bucket(&bucket)
            .notification_configuration(
                aws_sdk_s3::types::NotificationConfiguration::builder()
                    .topic_configurations(
                        aws_sdk_s3::types::TopicConfiguration::builder()
                            .id("all-events")
                            .topic_arn(&topic_arn)
                            .events(aws_sdk_s3::types::Event::S3ObjectCreated)
                            .events(aws_sdk_s3::types::Event::S3ObjectRemoved)
                            .build()
                            .unwrap(),
                    )
                    .build(),
            )
            .send().await.unwrap();

        let consumer = make_consumer(&broker, &topic_name);

        let obj_key = "all-events-obj";
        s3.put_object()
            .bucket(&bucket)
            .key(obj_key)
            .body(ByteStream::from_static(b"create then delete"))
            .send().await.unwrap();

        let create_msg = wait_for_event(&consumer, obj_key, "ObjectCreated:").await;
        assert_eq!(create_msg["Records"][0]["s3"]["object"]["key"].as_str().unwrap(), obj_key);

        s3.delete_object().bucket(&bucket).key(obj_key).send().await.unwrap();

        let delete_msg = wait_for_event(&consumer, obj_key, "ObjectRemoved:").await;
        assert_eq!(delete_msg["Records"][0]["s3"]["object"]["key"].as_str().unwrap(), obj_key);

        nuke_topics(&sns, &get_topic_prefix()).await;
    }

    /* --- overwrite, large object, notification CRUD, combined filters, topic attrs --- */

    #[tokio::test]
    async fn test_notification_kafka_overwrite() {
        let _guard = s3_tests_rs::fixtures::TestGuard::setup();
        let sns = get_sns_root_client();
        let s3 = get_iam_root_s3client();
        let broker = get_kafka_broker();

        let (topic_name, bucket, _) = setup_notification(
            &sns, &s3, &broker, aws_sdk_s3::types::Event::S3ObjectCreated,
        ).await;

        let consumer = make_consumer(&broker, &topic_name);

        let obj_key = "overwrite-me";
        s3.put_object()
            .bucket(&bucket)
            .key(obj_key)
            .body(ByteStream::from_static(b"version 1"))
            .send().await.unwrap();

        let msg1 = wait_for_event(&consumer, obj_key, "ObjectCreated:").await;
        let size1 = msg1["Records"][0]["s3"]["object"]["size"].as_u64().unwrap();
        assert_eq!(size1, 9);

        s3.put_object()
            .bucket(&bucket)
            .key(obj_key)
            .body(ByteStream::from_static(b"version 2 with more data"))
            .send().await.unwrap();

        let msg2 = wait_for_event(&consumer, obj_key, "ObjectCreated:").await;
        let size2 = msg2["Records"][0]["s3"]["object"]["size"].as_u64().unwrap();
        assert_eq!(size2, 24);
        assert_ne!(
            msg1["Records"][0]["s3"]["object"]["eTag"].as_str(),
            msg2["Records"][0]["s3"]["object"]["eTag"].as_str(),
            "overwrite should produce different etag"
        );

        nuke_topics(&sns, &get_topic_prefix()).await;
    }

    #[tokio::test]
    async fn test_notification_kafka_large_object() {
        let _guard = s3_tests_rs::fixtures::TestGuard::setup();
        let sns = get_sns_root_client();
        let s3 = get_iam_root_s3client();
        let broker = get_kafka_broker();

        let (topic_name, bucket, _) = setup_notification(
            &sns, &s3, &broker, aws_sdk_s3::types::Event::S3ObjectCreated,
        ).await;

        let consumer = make_consumer(&broker, &topic_name);

        let obj_key = "large-object";
        let body = vec![0x42u8; 1024 * 1024]; // 1 MB
        s3.put_object()
            .bucket(&bucket)
            .key(obj_key)
            .body(ByteStream::from(body))
            .send().await.unwrap();

        let msg = wait_for_event(&consumer, obj_key, "ObjectCreated:").await;
        let size = msg["Records"][0]["s3"]["object"]["size"].as_u64().unwrap();
        assert_eq!(size, 1024 * 1024, "event should report exact object size");

        nuke_topics(&sns, &get_topic_prefix()).await;
    }

    #[tokio::test]
    async fn test_notification_kafka_config_crud() {
        let _guard = s3_tests_rs::fixtures::TestGuard::setup();
        let sns = get_sns_root_client();
        let s3 = get_iam_root_s3client();
        let broker = get_kafka_broker();

        let (topic_name, bucket, topic_arn) = setup_notification(
            &sns, &s3, &broker, aws_sdk_s3::types::Event::S3ObjectCreated,
        ).await;

        let config = s3.get_bucket_notification_configuration()
            .bucket(&bucket)
            .send().await.unwrap();
        let topics = config.topic_configurations();
        assert_eq!(topics.len(), 1);
        assert_eq!(topics[0].topic_arn(), &topic_arn);

        s3.put_bucket_notification_configuration()
            .bucket(&bucket)
            .notification_configuration(
                aws_sdk_s3::types::NotificationConfiguration::builder().build(),
            )
            .send().await.unwrap();

        let config = s3.get_bucket_notification_configuration()
            .bucket(&bucket)
            .send().await.unwrap();
        assert!(config.topic_configurations().is_empty(),
            "cleared notification config should have no topics");

        let consumer = make_consumer(&broker, &topic_name);

        s3.put_object()
            .bucket(&bucket)
            .key("after-clear")
            .body(ByteStream::from_static(b"no notification"))
            .send().await.unwrap();

        let result = timeout(Duration::from_secs(3), async {
            loop {
                match consumer.recv().await {
                    Ok(m) => {
                        if let Some(payload) = m.payload() {
                            if let Ok(event) = serde_json::from_slice::<serde_json::Value>(payload) {
                                if event.get("Records").is_some() {
                                    return event;
                                }
                            }
                        }
                    }
                    Err(_) => continue,
                }
            }
        }).await;
        assert!(result.is_err(), "should not receive event after clearing notification config");

        nuke_topics(&sns, &get_topic_prefix()).await;
    }

    #[tokio::test]
    async fn test_notification_kafka_filter_prefix_and_suffix() {
        let _guard = s3_tests_rs::fixtures::TestGuard::setup();
        let sns = get_sns_root_client();
        let s3 = get_iam_root_s3client();
        let broker = get_kafka_broker();
        let topic_name = get_new_bucket_name();
        let bucket = get_new_bucket_name();

        let topic_arn = sns.create_topic()
            .name(&topic_name)
            .attributes("push-endpoint", format!("kafka://{}", broker))
            .attributes("kafka-ack-level", "broker")
            .send().await.unwrap()
            .topic_arn().unwrap().to_string();

        s3.create_bucket().bucket(&bucket).send().await.unwrap();

        s3.put_bucket_notification_configuration()
            .bucket(&bucket)
            .notification_configuration(
                aws_sdk_s3::types::NotificationConfiguration::builder()
                    .topic_configurations(
                        aws_sdk_s3::types::TopicConfiguration::builder()
                            .id("combo-filter")
                            .topic_arn(&topic_arn)
                            .events(aws_sdk_s3::types::Event::S3ObjectCreated)
                            .filter(
                                aws_sdk_s3::types::NotificationConfigurationFilter::builder()
                                    .key(
                                        aws_sdk_s3::types::S3KeyFilter::builder()
                                            .filter_rules(
                                                aws_sdk_s3::types::FilterRule::builder()
                                                    .name(aws_sdk_s3::types::FilterRuleName::Prefix)
                                                    .value("images/")
                                                    .build(),
                                            )
                                            .filter_rules(
                                                aws_sdk_s3::types::FilterRule::builder()
                                                    .name(aws_sdk_s3::types::FilterRuleName::Suffix)
                                                    .value(".png")
                                                    .build(),
                                            )
                                            .build(),
                                    )
                                    .build(),
                            )
                            .build()
                            .unwrap(),
                    )
                    .build(),
            )
            .send().await.unwrap();

        let consumer = make_consumer(&broker, &topic_name);

        // prefix match but wrong suffix — should not fire
        s3.put_object()
            .bucket(&bucket)
            .key("images/photo.jpg")
            .body(ByteStream::from_static(b"wrong suffix"))
            .send().await.unwrap();

        // right suffix but wrong prefix — should not fire
        s3.put_object()
            .bucket(&bucket)
            .key("docs/diagram.png")
            .body(ByteStream::from_static(b"wrong prefix"))
            .send().await.unwrap();

        // both match — should fire
        s3.put_object()
            .bucket(&bucket)
            .key("images/icon.png")
            .body(ByteStream::from_static(b"match"))
            .send().await.unwrap();

        let msg = wait_for_event(&consumer, "images/icon.png", "ObjectCreated:").await;
        assert_eq!(msg["Records"][0]["s3"]["object"]["key"].as_str().unwrap(), "images/icon.png");

        // verify the non-matching ones didn't arrive
        let leak = timeout(Duration::from_secs(3), async {
            loop {
                match consumer.recv().await {
                    Ok(m) => {
                        if let Some(payload) = m.payload() {
                            if let Ok(event) = serde_json::from_slice::<serde_json::Value>(payload) {
                                if let Some(record) = event.get("Records")
                                    .and_then(|r| r.as_array())
                                    .and_then(|a| a.first())
                                {
                                    let key = record["s3"]["object"]["key"].as_str().unwrap_or("");
                                    if key == "images/photo.jpg" || key == "docs/diagram.png" {
                                        return key.to_string();
                                    }
                                }
                            }
                        }
                    }
                    Err(_) => continue,
                }
            }
        }).await;
        assert!(leak.is_err(), "received event for non-matching prefix+suffix combo");

        nuke_topics(&sns, &get_topic_prefix()).await;
    }

    #[tokio::test]
    async fn test_notification_kafka_topic_attributes() {
        let _guard = s3_tests_rs::fixtures::TestGuard::setup();
        let sns = get_sns_root_client();
        let broker = get_kafka_broker();
        let topic_name = get_new_bucket_name();

        let topic_arn = sns.create_topic()
            .name(&topic_name)
            .attributes("push-endpoint", format!("kafka://{}", broker))
            .attributes("kafka-ack-level", "broker")
            .send().await.unwrap()
            .topic_arn().unwrap().to_string();

        let attrs = sns.get_topic_attributes()
            .topic_arn(&topic_arn)
            .send().await.unwrap();

        let attr_map = attrs.attributes().unwrap();
        assert_eq!(attr_map.get("TopicArn").map(|s| s.as_str()), Some(topic_arn.as_str()));

        let push_ep = attr_map.get("push-endpoint")
            .or_else(|| attr_map.get("EndPoint"));
        assert!(push_ep.is_some(), "topic attributes should include push endpoint");
        if let Some(ep) = push_ep {
            assert!(ep.contains("kafka://"), "endpoint should be kafka URL, got: {ep}");
        }

        sns.set_topic_attributes()
            .topic_arn(&topic_arn)
            .attribute_name("Policy")
            .attribute_value(r#"{"Version":"2012-10-17","Statement":[]}"#)
            .send().await.unwrap();

        let attrs2 = sns.get_topic_attributes()
            .topic_arn(&topic_arn)
            .send().await.unwrap();
        let policy = attrs2.attributes().unwrap().get("Policy");
        assert!(policy.is_some(), "Policy attribute should be set");

        sns.delete_topic().topic_arn(&topic_arn).send().await.unwrap();
        nuke_topics(&sns, &get_topic_prefix()).await;
    }

    /* --- metadata and tag filters --- */

    use aws_smithy_xml::decode::{try_data, Document, ScopedDecoder, XmlDecodeError};
    use aws_smithy_xml::encode::{ScopeWriter, XmlWriter};

    /* An element of a NotificationConfiguration, decoded.  The SDK's
     * schema for it is closed, so which elements carry text is known --
     * NOTIFICATION_LEAVES -- and every other element holds elements. */
    struct XmlNode {
        name: String,
        text: Option<String>,
        children: Vec<XmlNode>,
    }

    const NOTIFICATION_LEAVES: [&str; 7] =
        ["Id", "Topic", "Queue", "CloudFunction", "Event", "Name", "Value"];

    fn decode_node(el: &mut ScopedDecoder<'_, '_>) -> Result<XmlNode, XmlDecodeError> {
        let name = el.start_el().local().to_string();
        if NOTIFICATION_LEAVES.contains(&name.as_str()) {
            let text = try_data(el)?.into_owned();
            return Ok(XmlNode { name, text: Some(text), children: Vec::new() });
        }
        let mut children = Vec::new();
        while let Some(mut c) = el.next_tag() {
            children.push(decode_node(&mut c)?);
        }
        Ok(XmlNode { name, text: None, children })
    }

    fn encode_children(scope: &mut ScopeWriter<'_, '_>, node: &XmlNode) {
        for c in &node.children {
            let mut child = scope.start_el(&c.name).finish();
            match &c.text {
                Some(t) => child.data(t),
                None => encode_children(&mut child, c),
            }
            child.finish();
        }
    }

    /* Move each Filter's x-amz-meta-* rules from <S3Key> into
     * <S3Metadata>, and its other non-key rules into <S3Tags>, within
     * the same Filter.  Returns whether anything moved. */
    fn move_filter_rules(node: &mut XmlNode) -> bool {
        let mut moved = false;
        for c in &mut node.children {
            moved |= move_filter_rules(c);
        }
        if node.name != "Filter" {
            return moved;
        }
        let Some(at) = node.children.iter().position(|c| c.name == "S3Key") else {
            return moved;
        };
        let rule_name = |r: &XmlNode| -> String {
            r.children.iter().find(|c| c.name == "Name")
                .and_then(|c| c.text.clone()).unwrap_or_default()
        };
        let (mut key, mut metadata, mut tags) = (Vec::new(), Vec::new(), Vec::new());
        for r in node.children.remove(at).children {
            let n = rule_name(&r);
            if n.starts_with("x-amz-meta-") {
                metadata.push(r);
            } else if n != "prefix" && n != "suffix" {
                tags.push(r);
            } else {
                key.push(r);
            }
        }
        if metadata.is_empty() && tags.is_empty() {
            node.children.insert(at, XmlNode { name: "S3Key".into(), text: None, children: key });
            return moved;
        }
        let mut rebuilt = Vec::new();
        if !key.is_empty() {
            rebuilt.push(XmlNode { name: "S3Key".into(), text: None, children: key });
        }
        if !metadata.is_empty() {
            rebuilt.push(XmlNode { name: "S3Metadata".into(), text: None, children: metadata });
        }
        if !tags.is_empty() {
            rebuilt.push(XmlNode { name: "S3Tags".into(), text: None, children: tags });
        }
        for (k, n) in rebuilt.into_iter().enumerate() {
            node.children.insert(at + k, n);
        }
        true
    }

    /* The AWS SDK only generates <S3Key> filter rules. RGW's metadata
     * and tag filter extensions use <S3Metadata> and <S3Tags> elements.
     * This interceptor rewrites the PutBucketNotificationConfiguration
     * XML body to move x-amz-meta-* rules into <S3Metadata> and
     * non-key rules into <S3Tags>. */
    #[derive(Debug, Clone)]
    struct NotificationFilterRewriter;

    impl aws_smithy_runtime_api::client::interceptors::Intercept for NotificationFilterRewriter {
        fn name(&self) -> &'static str { "NotificationFilterRewriter" }

        fn modify_before_signing(
            &self,
            context: &mut aws_smithy_runtime_api::client::interceptors::context::BeforeTransmitInterceptorContextMut<'_>,
            _runtime_components: &aws_smithy_runtime_api::client::runtime_components::RuntimeComponents,
            _cfg: &mut aws_smithy_types::config_bag::ConfigBag,
        ) -> Result<(), aws_smithy_runtime_api::box_error::BoxError> {
            let request = context.request_mut();
            let body_bytes = request.body().bytes()
                .ok_or("no body")?
                .to_vec();
            let body_str = String::from_utf8(body_bytes)?;

            let mut doc = Document::new(&body_str);
            let Ok(mut root) = doc.root_element() else {
                return Ok(());
            };
            if !root.start_el().matches("NotificationConfiguration") {
                return Ok(());
            }
            let xmlns = root.start_el().attr("xmlns").map(str::to_string);
            let mut config = decode_node(&mut root)?;
            if !move_filter_rules(&mut config) {
                return Ok(());
            }

            let mut new_body = String::new();
            let mut w = XmlWriter::new(&mut new_body);
            let mut el = w.start_el(&config.name);
            if let Some(ns) = &xmlns {
                el = el.write_ns(ns, None);
            }
            let mut scope = el.finish();
            encode_children(&mut scope, &config);
            scope.finish();

            eprintln!("NotificationFilterRewriter: rewritten body:\n{}", new_body);
            let new_len = new_body.len();
            *request.body_mut() = aws_smithy_types::body::SdkBody::from(new_body);
            request.headers_mut().insert("content-length", new_len.to_string());
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_notification_kafka_filter_metadata() {
        let _guard = s3_tests_rs::fixtures::TestGuard::setup();
        let sns = get_sns_root_client();
        let s3 = get_iam_root_s3client();
        let broker = get_kafka_broker();
        let topic_name = get_new_bucket_name();
        let bucket = get_new_bucket_name();

        let topic_arn = sns.create_topic()
            .name(&topic_name)
            .attributes("push-endpoint", format!("kafka://{}", broker))
            .attributes("kafka-ack-level", "broker")
            .send().await.unwrap()
            .topic_arn().unwrap().to_string();

        s3.create_bucket().bucket(&bucket).send().await.unwrap();

        s3.put_bucket_notification_configuration()
            .bucket(&bucket)
            .notification_configuration(
                aws_sdk_s3::types::NotificationConfiguration::builder()
                    .topic_configurations(
                        aws_sdk_s3::types::TopicConfiguration::builder()
                            .id("meta-filter")
                            .topic_arn(&topic_arn)
                            .events(aws_sdk_s3::types::Event::S3ObjectCreated)
                            .filter(
                                aws_sdk_s3::types::NotificationConfigurationFilter::builder()
                                    .key(
                                        aws_sdk_s3::types::S3KeyFilter::builder()
                                            .filter_rules(
                                                aws_sdk_s3::types::FilterRule::builder()
                                                    .name(aws_sdk_s3::types::FilterRuleName::from("x-amz-meta-color"))
                                                    .value("blue")
                                                    .build(),
                                            )
                                            .build(),
                                    )
                                    .build(),
                            )
                            .build()
                            .unwrap(),
                    )
                    .build(),
            )
            .customize()
            .interceptor(NotificationFilterRewriter)
            .send().await.unwrap();

        let consumer = make_consumer(&broker, &topic_name);

        s3.put_object()
            .bucket(&bucket)
            .key("red-obj")
            .metadata("color", "red")
            .body(ByteStream::from_static(b"wrong metadata"))
            .send().await.unwrap();

        s3.put_object()
            .bucket(&bucket)
            .key("blue-obj")
            .metadata("color", "blue")
            .body(ByteStream::from_static(b"matching metadata"))
            .send().await.unwrap();

        let msg = wait_for_event(&consumer, "blue-obj", "ObjectCreated:").await;
        assert_eq!(msg["Records"][0]["s3"]["object"]["key"].as_str().unwrap(), "blue-obj");

        let leak = timeout(Duration::from_secs(3), async {
            loop {
                match consumer.recv().await {
                    Ok(m) => {
                        if let Some(payload) = m.payload() {
                            if let Ok(event) = serde_json::from_slice::<serde_json::Value>(payload) {
                                if let Some(record) = event.get("Records")
                                    .and_then(|r| r.as_array())
                                    .and_then(|a| a.first())
                                {
                                    if record["s3"]["object"]["key"].as_str() == Some("red-obj") {
                                        return true;
                                    }
                                }
                            }
                        }
                    }
                    Err(_) => continue,
                }
            }
        }).await;
        assert!(leak.is_err(), "received event for non-matching metadata value");

        nuke_topics(&sns, &get_topic_prefix()).await;
    }

    #[tokio::test]
    async fn test_notification_kafka_filter_tags() {
        let _guard = s3_tests_rs::fixtures::TestGuard::setup();
        let sns = get_sns_root_client();
        let s3 = get_iam_root_s3client();
        let broker = get_kafka_broker();
        let topic_name = get_new_bucket_name();
        let bucket = get_new_bucket_name();

        let topic_arn = sns.create_topic()
            .name(&topic_name)
            .attributes("push-endpoint", format!("kafka://{}", broker))
            .attributes("kafka-ack-level", "broker")
            .send().await.unwrap()
            .topic_arn().unwrap().to_string();

        s3.create_bucket().bucket(&bucket).send().await.unwrap();

        s3.put_bucket_notification_configuration()
            .bucket(&bucket)
            .notification_configuration(
                aws_sdk_s3::types::NotificationConfiguration::builder()
                    .topic_configurations(
                        aws_sdk_s3::types::TopicConfiguration::builder()
                            .id("tag-filter")
                            .topic_arn(&topic_arn)
                            .events(aws_sdk_s3::types::Event::S3ObjectCreated)
                            .filter(
                                aws_sdk_s3::types::NotificationConfigurationFilter::builder()
                                    .key(
                                        aws_sdk_s3::types::S3KeyFilter::builder()
                                            .filter_rules(
                                                aws_sdk_s3::types::FilterRule::builder()
                                                    .name(aws_sdk_s3::types::FilterRuleName::from("environment"))
                                                    .value("production")
                                                    .build(),
                                            )
                                            .build(),
                                    )
                                    .build(),
                            )
                            .build()
                            .unwrap(),
                    )
                    .build(),
            )
            .customize()
            .interceptor(NotificationFilterRewriter)
            .send().await.unwrap();

        let consumer = make_consumer(&broker, &topic_name);

        s3.put_object()
            .bucket(&bucket)
            .key("staging-obj")
            .tagging("environment=staging")
            .body(ByteStream::from_static(b"wrong tag"))
            .send().await.unwrap();

        s3.put_object()
            .bucket(&bucket)
            .key("prod-obj")
            .tagging("environment=production")
            .body(ByteStream::from_static(b"matching tag"))
            .send().await.unwrap();

        let msg = wait_for_event(&consumer, "prod-obj", "ObjectCreated:").await;
        assert_eq!(msg["Records"][0]["s3"]["object"]["key"].as_str().unwrap(), "prod-obj");

        let leak = timeout(Duration::from_secs(3), async {
            loop {
                match consumer.recv().await {
                    Ok(m) => {
                        if let Some(payload) = m.payload() {
                            if let Ok(event) = serde_json::from_slice::<serde_json::Value>(payload) {
                                if let Some(record) = event.get("Records")
                                    .and_then(|r| r.as_array())
                                    .and_then(|a| a.first())
                                {
                                    if record["s3"]["object"]["key"].as_str() == Some("staging-obj") {
                                        return true;
                                    }
                                }
                            }
                        }
                    }
                    Err(_) => continue,
                }
            }
        }).await;
        assert!(leak.is_err(), "received event for non-matching tag value");

        nuke_topics(&sns, &get_topic_prefix()).await;
    }
}

/* Endpoint arguments a topic stores and checks at creation:  a Kafka
 * topic's `kafka-config`, the librdkafka settings passed through to its
 * producer, gated by the gateway's `rgw_kafka_config_allowed_keys`;  the
 * client certificate an HTTP topic must give whole;  and the escaping
 * that keeps `%`, `&` and `+` in a stored value.  Creating a topic does
 * not connect, so these need no broker.  A client cannot read the
 * gateway's allowlist, so each test first probes with a topic whether
 * its keys are listed, and asserts whichever case it is in. */
mod topic_config_tests {
    use std::collections::HashMap;

    use super::*;
    use aws_sdk_sns::error::ProvideErrorMetadata;

    const KAFKA_ENDPOINT: &str = "kafka://127.0.0.1:9092";
    pub(super) const UNLISTED: &str = "is not in rgw_kafka_config_allowed_keys";

    /* a topic with `attributes`, its ARN, or the message it is refused with */
    pub(super) async fn create(sns: &aws_sdk_sns::Client, attributes: &[(&str, &str)])
                    -> Result<String, String> {
        let mut req = sns.create_topic().name(get_new_bucket_name());
        for (k, v) in attributes {
            req = req.attributes(*k, *v);
        }
        match req.send().await {
            Ok(r) => Ok(r.topic_arn().unwrap().to_string()),
            Err(e) => Err(e.into_service_error().message().unwrap_or_default().to_string()),
        }
    }

    /* whether rgw_kafka_config_allowed_keys lists `key`:  a Kafka topic
     * setting it alone, to a value librdkafka accepts, is created, or
     * refused for that reason.  The allowlist is checked first, so a
     * secret refused for its transport is listed */
    pub(super) async fn allowed(sns: &aws_sdk_sns::Client, key: &str, value: &str) -> bool {
        let config = serde_json::json!({ key: value }).to_string();
        match create(sns, &[("push-endpoint", KAFKA_ENDPOINT), ("kafka-config", &config)]).await {
            Ok(arn) => {
                sns.delete_topic().topic_arn(&arn).send().await.unwrap();
                true
            }
            Err(m) if m.contains(UNLISTED) => false,
            Err(m) if m.contains("secure") => true,
            Err(m) => panic!("probing {key}={value}:  refused for another reason:  {m}"),
        }
    }

    /* the topic's EndPoint attribute, as JSON */
    async fn endpoint(sns: &aws_sdk_sns::Client, arn: &str) -> serde_json::Value {
        let attrs = sns.get_topic_attributes().topic_arn(arn).send().await.unwrap();
        let ep = attrs.attributes().unwrap().get("EndPoint").expect("an EndPoint attribute");
        serde_json::from_str(ep).expect("EndPoint is JSON")
    }

    fn percent_decode(s: &str) -> String {
        let b = s.as_bytes();
        let mut out = Vec::with_capacity(b.len());
        let mut i = 0;
        while i < b.len() {
            if b[i] == b'%' && i + 2 < b.len() {
                out.push(u8::from_str_radix(&s[i + 1..i + 3], 16).expect("a percent escape"));
                i += 3;
            } else {
                out.push(if b[i] == b'+' { b' ' } else { b[i] });
                i += 1;
            }
        }
        String::from_utf8(out).unwrap()
    }

    /* EndpointArgs read as the gateway reads them back
     * (RGWHTTPArgs::parse()):  split on '&', each entry url-decoded, the
     * name ending at the first '=' */
    fn endpoint_args(ep: &serde_json::Value) -> HashMap<String, String> {
        ep["EndpointArgs"].as_str().unwrap_or_default().split('&')
            .filter(|e| !e.is_empty())
            .map(|e| {
                let e = percent_decode(e);
                match e.split_once('=') {
                    Some((k, v)) => (k.to_string(), v.to_string()),
                    None => (e, String::new()),
                }
            })
            .collect()
    }

    #[cfg_attr(feature = "fails_on_aws", ignore = "fails on aws:  no push-endpoint")]
    #[tokio::test]
    async fn test_topic_kafka_config_unlisted_key() {
        let _guard = s3_tests_rs::fixtures::TestGuard::setup();
        let sns = get_sns_root_client();
        /* refused either way:  unlisted, or, listed, unknown to librdkafka */
        let m = create(&sns, &[("push-endpoint", KAFKA_ENDPOINT),
                               ("kafka-config", r#"{"s3-tests-rs.unlisted": "1"}"#)]).await
            .expect_err("a topic with an unlisted kafka-config key was created");
        assert!(m.contains(UNLISTED) || m.contains("s3-tests-rs.unlisted"),
                "refused for another reason:  {m}");
    }

    #[cfg_attr(feature = "fails_on_aws", ignore = "fails on aws:  no push-endpoint")]
    #[tokio::test]
    async fn test_topic_kafka_config_bad_value() {
        let _guard = s3_tests_rs::fixtures::TestGuard::setup();
        let sns = get_sns_root_client();
        let listed = allowed(&sns, "linger.ms", "5").await;
        let m = create(&sns, &[("push-endpoint", KAFKA_ENDPOINT),
                               ("kafka-config", r#"{"linger.ms": "soon"}"#)]).await
            .expect_err("a topic with a value librdkafka rejects was created");
        if listed {
            assert!(m.contains("linger.ms") && !m.contains(UNLISTED),
                    "not librdkafka's refusal:  {m}");
        } else {
            assert!(m.contains(UNLISTED), "linger.ms is unlisted, but refused otherwise:  {m}");
        }
    }

    #[cfg_attr(feature = "fails_on_aws", ignore = "fails on aws:  no push-endpoint")]
    #[tokio::test]
    async fn test_topic_kafka_config_not_an_object() {
        let _guard = s3_tests_rs::fixtures::TestGuard::setup();
        let sns = get_sns_root_client();
        for config in [r#"not json"#, r#"["client.id"]"#, r#"{"client.id": {"a": 1}}"#,
                       r#"{"client.id": null}"#] {
            let m = create(&sns, &[("push-endpoint", KAFKA_ENDPOINT),
                                   ("kafka-config", config)]).await
                .expect_err(&format!("a topic with kafka-config {config} was created"));
            assert!(m.contains("kafka-config"), "{config}:  refused for another reason:  {m}");
        }
    }

    #[cfg_attr(feature = "fails_on_aws", ignore = "fails on aws:  no push-endpoint")]
    #[tokio::test]
    async fn test_topic_kafka_config_numbers_and_booleans() {
        let _guard = s3_tests_rs::fixtures::TestGuard::setup();
        let sns = get_sns_root_client();
        if !allowed(&sns, "linger.ms", "5").await {
            let m = create(&sns, &[("push-endpoint", KAFKA_ENDPOINT),
                                   ("kafka-config", r#"{"linger.ms": 5}"#)]).await
                .expect_err("an unlisted key was accepted");
            assert!(m.contains(UNLISTED), "{m}");
            return;
        }
        /* NooBaa's kafka_options_object carries numbers as numbers */
        let arn = create(&sns, &[("push-endpoint", KAFKA_ENDPOINT),
                                 ("kafka-config", r#"{"linger.ms": 5}"#)]).await
            .expect("a number value refused");
        sns.delete_topic().topic_arn(&arn).send().await.unwrap();
    }

    #[cfg_attr(feature = "fails_on_aws", ignore = "fails on aws:  no push-endpoint")]
    #[tokio::test]
    async fn test_topic_kafka_config_set_attributes() {
        let _guard = s3_tests_rs::fixtures::TestGuard::setup();
        let sns = get_sns_root_client();
        let arn = create(&sns, &[("push-endpoint", KAFKA_ENDPOINT)]).await.unwrap();
        let set = |value: &'static str| sns.set_topic_attributes().topic_arn(&arn)
            .attribute_name("kafka-config").attribute_value(value).send();
        if !allowed(&sns, "client.id", "s3-tests-rs").await {
            let e = set(r#"{"client.id": "first"}"#).await
                .expect_err("SetTopicAttributes set an unlisted key");
            let m = e.into_service_error().message().unwrap_or_default().to_string();
            assert!(m.contains(UNLISTED), "{m}");
            sns.delete_topic().topic_arn(&arn).send().await.unwrap();
            return;
        }
        set(r#"{"client.id": "first"}"#).await.unwrap();
        let args = endpoint_args(&endpoint(&sns, &arn).await);
        assert_eq!(args.get("kafka-config").map(String::as_str), Some(r#"{"client.id": "first"}"#));

        /* the set is replaced whole */
        set(r#"{"client.id": "second"}"#).await.unwrap();
        let args = endpoint_args(&endpoint(&sns, &arn).await);
        assert_eq!(args.get("kafka-config").map(String::as_str), Some(r#"{"client.id": "second"}"#));
        assert_eq!(args.keys().filter(|k| *k == "kafka-config").count(), 1);

        /* and checked as at creation */
        let e = set(r#"{"s3-tests-rs.unlisted": "1"}"#).await
            .expect_err("SetTopicAttributes set an unlisted key");
        let m = e.into_service_error().message().unwrap_or_default().to_string();
        assert!(m.contains(UNLISTED) || m.contains("s3-tests-rs.unlisted"), "{m}");

        set("").await.unwrap();
        let args = endpoint_args(&endpoint(&sns, &arn).await);
        assert_eq!(args.get("kafka-config").map(String::as_str), Some(""));
        sns.delete_topic().topic_arn(&arn).send().await.unwrap();
    }

    #[cfg_attr(feature = "fails_on_aws", ignore = "fails on aws:  no push-endpoint")]
    #[tokio::test]
    async fn test_topic_kafka_config_secret() {
        let _guard = s3_tests_rs::fixtures::TestGuard::setup();
        let sns = get_sns_root_client();
        if !allowed(&sns, "sasl.password", "x").await
            || !allowed(&sns, "security.protocol", "SASL_SSL").await {
            let m = create(&sns, &[("push-endpoint", KAFKA_ENDPOINT),
                                   ("kafka-config", r#"{"sasl.password": "x"}"#)]).await
                .expect_err("an unlisted key was accepted");
            assert!(m.contains(UNLISTED), "{m}");
            return;
        }
        const INSECURE: &str = "secure";
        let secure_transport = s3_tests_rs::config::get_config().default_is_secure;

        /* over TLS to the broker:  a secret, marked so */
        let tls = create(&sns, &[("push-endpoint", KAFKA_ENDPOINT),
            ("kafka-config", r#"{"security.protocol": "SASL_SSL", "sasl.password": "x"}"#)]).await;
        /* over cleartext to the broker */
        let plain = create(&sns, &[("push-endpoint", KAFKA_ENDPOINT),
            ("kafka-config", r#"{"security.protocol": "SASL_PLAINTEXT", "sasl.password": "x"}"#)]).await;
        /* use-ssl and no security.protocol */
        let use_ssl = create(&sns, &[("push-endpoint", KAFKA_ENDPOINT), ("use-ssl", "true"),
            ("kafka-config", r#"{"sasl.password": "x"}"#)]).await;

        match &tls {
            Ok(arn) => {
                assert_eq!(endpoint(&sns, arn).await["HasStoredSecret"], true,
                           "a kafka-config secret is not marked");
            }
            Err(m) => {
                /* only rgw_allow_notification_secrets_in_cleartext, unset,
                 * refuses a secret over a request without TLS */
                assert!(!secure_transport, "refused over TLS:  {m}");
                assert!(m.contains(INSECURE), "{m}");
            }
        }
        /* one option governs both the request and the broker:  with
         * the request in cleartext, the cleartext broker is refused
         * exactly when the TLS broker is */
        if !secure_transport {
            assert_eq!(plain.is_ok(), tls.is_ok(), "{plain:?} against {tls:?}");
        }
        if let Err(m) = &plain {
            assert!(m.contains(INSECURE), "{m}");
        }
        assert_eq!(use_ssl.is_ok(), tls.is_ok(), "{use_ssl:?} against {tls:?}");
        for arn in [tls, plain, use_ssl].into_iter().flatten() {
            sns.delete_topic().topic_arn(&arn).send().await.unwrap();
        }
    }

    #[cfg_attr(feature = "fails_on_aws", ignore = "fails on aws:  no push-endpoint")]
    #[tokio::test]
    async fn test_topic_http_client_certificate_whole() {
        let _guard = s3_tests_rs::fixtures::TestGuard::setup();
        let sns = get_sns_root_client();
        for (k, v) in [("ssl-certificate-location", "/nonexistent/client.pem"),
                       ("ssl-key-location", "/nonexistent/client-key.pem")] {
            let m = create(&sns, &[("push-endpoint", "https://127.0.0.1:1"), (k, v)]).await
                .expect_err(&format!("an HTTP topic with only {k} was created"));
            assert!(m.contains("ssl-certificate-location"), "{k}:  {m}");
        }
        let arn = create(&sns, &[("push-endpoint", "https://127.0.0.1:1"),
                                 ("ssl-certificate-location", "/nonexistent/client.pem"),
                                 ("ssl-key-location", "/nonexistent/client-key.pem")]).await
            .expect("an HTTP topic with a whole client certificate");
        sns.delete_topic().topic_arn(&arn).send().await.unwrap();
    }

    /* a user name and a password are a pair, whichever of the URL and the
     * attributes gives each;  half of one is refused, and the gateway
     * keeps answering (radosgw asserted on it) */
    #[cfg_attr(feature = "fails_on_aws", ignore = "fails on aws:  no push-endpoint")]
    #[tokio::test]
    async fn test_topic_credentials_are_a_pair() {
        let _guard = s3_tests_rs::fixtures::TestGuard::setup();
        let sns = get_sns_root_client();
        let still_up = || async {
            sns.list_topics().send().await.expect("the gateway stopped answering");
        };
        for attrs in [&[("push-endpoint", KAFKA_ENDPOINT), ("password", "x")][..],
                      &[("push-endpoint", KAFKA_ENDPOINT), ("user-name", "u")][..],
                      &[("push-endpoint", "kafka://u:p@127.0.0.1:9092"), ("user-name", "")][..]] {
            let m = create(&sns, attrs).await
                .expect_err(&format!("a topic with half a credential was created:  {attrs:?}"));
            assert!(m.contains("together"), "{attrs:?}:  refused for another reason:  {m}");
            still_up().await;
        }
        let arn = create(&sns, &[("push-endpoint", KAFKA_ENDPOINT)]).await.unwrap();
        let e = sns.set_topic_attributes().topic_arn(&arn)
            .attribute_name("password").attribute_value("x").send().await
            .expect_err("SetTopicAttributes stored a password with no user");
        let m = e.into_service_error().message().unwrap_or_default().to_string();
        assert!(m.contains("together"), "{m}");
        still_up().await;
        sns.delete_topic().topic_arn(&arn).send().await.unwrap();
    }

    #[cfg_attr(feature = "fails_on_aws", ignore = "fails on aws:  no push-endpoint")]
    #[tokio::test]
    async fn test_topic_endpoint_args_escaped() {
        let _guard = s3_tests_rs::fixtures::TestGuard::setup();
        let sns = get_sns_root_client();
        let value = "/a&b=c%41+d";
        let arn = create(&sns, &[("push-endpoint", "https://127.0.0.1:1"),
                                 ("ca-location", value), ("cloudevents", "true")]).await
            .unwrap();
        let args = endpoint_args(&endpoint(&sns, &arn).await);
        assert_eq!(args.get("ca-location").map(String::as_str), Some(value));
        assert_eq!(args.get("cloudevents").map(String::as_str), Some("true"));

        let value = "/x+y&z%";
        sns.set_topic_attributes().topic_arn(&arn).attribute_name("ca-location")
            .attribute_value(value).send().await.unwrap();
        let args = endpoint_args(&endpoint(&sns, &arn).await);
        assert_eq!(args.get("ca-location").map(String::as_str), Some(value));
        assert_eq!(args.get("cloudevents").map(String::as_str), Some("true"));
        sns.delete_topic().topic_arn(&arn).send().await.unwrap();
    }
}

/* The HTTP subset of test_bn.py, against an endpoint in the test process
 * (s3_tests_rs::notify_receiver).  The five tests whose topics are not
 * persistent;  test_lifecycle_http, test_lifecycle_abort_mpu and
 * test_metadata_filter_http make theirs persistent and wait on the
 * queue, so they go with persistent delivery (work plan, Future work 6).
 * Each keeps test_bn.py's name. */
mod http_tests {
    use std::collections::HashMap;
    use std::time::Duration;

    use aws_sdk_s3::primitives::ByteStream;
    use aws_sdk_s3::types::{Delete, NotificationConfiguration, ObjectIdentifier,
                            TopicConfiguration};
    use md5::{Digest, Md5};
    use rand::{Rng, RngCore};

    use s3_tests_rs::client::{get_iam_root_s3client, get_sns_root_client};
    use s3_tests_rs::fixtures::get_new_bucket_name;
    use s3_tests_rs::notify_receiver::{HttpReceiver, Received};

    const META_PREFIX: &str = "x-amz-meta-";

    /* test_bn.py writes str(os.urandom(n)), a Python repr about four
     * times n long;  n random bytes is the intent */
    fn random_content(n: usize) -> Vec<u8> {
        let mut v = vec![0u8; n];
        rand::thread_rng().fill_bytes(&mut v);
        v
    }

    fn md5_hex(b: &[u8]) -> String {
        hex::encode(Md5::digest(b))
    }

    /* a topic pushing to `receiver`, and a notification on `bucket` for
     * `events`, empty meaning every event (PSTopicS3, PSNotificationS3) */
    async fn topic_and_notification(
        sns: &aws_sdk_sns::Client,
        s3: &aws_sdk_s3::Client,
        bucket: &str,
        receiver: &HttpReceiver,
        attributes: &[(&str, &str)],
        events: Vec<aws_sdk_s3::types::Event>,
    ) -> String {
        let mut req = sns.create_topic().name(format!("{bucket}_topic"))
            .attributes("push-endpoint", receiver.endpoint());
        for (k, v) in attributes {
            req = req.attributes(*k, *v);
        }
        let topic_arn = req.send().await.unwrap().topic_arn().unwrap().to_string();
        s3.put_bucket_notification_configuration()
            .bucket(bucket)
            .notification_configuration(
                NotificationConfiguration::builder()
                    .topic_configurations(
                        TopicConfiguration::builder()
                            .id(format!("{bucket}_notif"))
                            .topic_arn(&topic_arn)
                            .set_events(Some(events))
                            .build()
                            .unwrap(),
                    )
                    .build(),
            )
            .send().await.unwrap();
        topic_arn
    }

    /* the bucket's objects as test_bn.py's bucket.list() gives them:
     * name and ETag, unquoted */
    async fn list_keys(s3: &aws_sdk_s3::Client, bucket: &str) -> Vec<(String, String)> {
        let resp = s3.list_objects_v2().bucket(bucket).send().await.unwrap();
        resp.contents().iter()
            .map(|o| (o.key().unwrap().to_string(),
                      o.e_tag().unwrap_or_default().trim_matches('"').to_string()))
            .collect()
    }

    /* verify_s3_records_by_elements, for a list of POSTs:  for each key,
     * a record of the right kind, with an eTag, metadata keys under the
     * prefix and, given sizes, the object's size;  given etags, the
     * listed ETag is among them;  and, exact, no superfluous record */
    fn verify_records(
        received: &[Received],
        bucket: &str,
        keys: &[(String, String)],
        exact_match: bool,
        deletions: bool,
        expected_sizes: Option<&HashMap<String, u64>>,
        etags: Option<&[String]>,
    ) {
        for (name, etag) in keys {
            let mut found: Option<u64> = None;
            'posts: for post in received {
                for record in post.body["Records"].as_array().expect("a Records list") {
                    let object = &record["s3"]["object"];
                    assert!(object.get("eTag").is_some(), "a record without an eTag: {record}");
                    if record["s3"]["bucket"]["name"] != bucket || object["key"] != name.as_str() {
                        continue;
                    }
                    if let Some(etags) = etags {
                        assert!(etags.contains(etag), "{name}'s ETag {etag} is not its content's MD5");
                    }
                    for meta in object["metadata"].as_array().into_iter().flatten() {
                        assert!(meta["key"].as_str().unwrap_or_default().starts_with(META_PREFIX),
                                "metadata outside {META_PREFIX}: {meta}");
                    }
                    let event = record["eventName"].as_str().unwrap_or_default();
                    let kind = if deletions { "ObjectRemoved" } else { "ObjectCreated" };
                    if event.starts_with(kind) {
                        found = Some(object["size"].as_u64().unwrap_or(0));
                        break 'posts;
                    }
                }
            }
            let size = found.unwrap_or_else(|| panic!(
                "no {} event found for key: {name};  {} POSTs, the first:  {}",
                if deletions { "deletion" } else { "creation" }, received.len(),
                received.first().map_or("none".into(), |p| p.body.to_string())));
            if let Some(sizes) = expected_sizes {
                assert_eq!(Some(&size), sizes.get(name), "{name}'s size in its event");
            }
        }
        if exact_match {
            assert_eq!(received.len(), keys.len(), "superfluous records are found");
        }
    }

    async fn wait_for_the_messages() {
        /* test_bn.py:  "wait for 5sec for the messages" */
        tokio::time::sleep(Duration::from_secs(5)).await;
    }

    /* notification('http', conn, cloudevents=...) */
    async fn notification_http(cloudevents: bool) {
        let _guard = s3_tests_rs::fixtures::TestGuard::setup();
        let sns = get_sns_root_client();
        let s3 = get_iam_root_s3client();
        let bucket = get_new_bucket_name();
        s3.create_bucket().bucket(&bucket).send().await.unwrap();
        let receiver = HttpReceiver::start().await;
        let attrs: &[(&str, &str)] = if cloudevents { &[("cloudevents", "true")] } else { &[] };
        let topic_arn = topic_and_notification(&sns, &s3, &bucket, &receiver, attrs, vec![]).await;

        let number_of_objects = if cloudevents { 10 } else { 100 };
        let mut etags = Vec::new();
        let mut sizes = HashMap::new();
        let mut puts = Vec::new();
        for i in 0..number_of_objects {
            let content = random_content(1024 * 1024);
            etags.push(md5_hex(&content));
            sizes.insert(i.to_string(), content.len() as u64);
            let (s3, bucket) = (s3.clone(), bucket.clone());
            puts.push(tokio::spawn(async move {
                s3.put_object().bucket(&bucket).key(i.to_string())
                    .body(ByteStream::from(content)).send().await.unwrap();
            }));
        }
        for p in puts {
            p.await.unwrap();
        }
        wait_for_the_messages().await;

        let keys = list_keys(&s3, &bucket).await;
        let md5 = !s3_tests_rs::features::non_md5_etag().await;
        let received = receiver.take();
        if cloudevents {
            check_cloudevents(&received);
        }
        verify_records(&received, &bucket, &keys, true, false, Some(&sizes),
                       md5.then_some(etags.as_slice()));

        let mut deletes = Vec::new();
        for (name, _) in &keys {
            let (s3, bucket, name) = (s3.clone(), bucket.clone(), name.clone());
            deletes.push(tokio::spawn(async move {
                s3.delete_object().bucket(&bucket).key(&name).send().await.unwrap();
            }));
        }
        for d in deletes {
            d.await.unwrap();
        }
        wait_for_the_messages().await;
        let received = receiver.take();
        if cloudevents {
            check_cloudevents(&received);
        }
        verify_records(&received, &bucket, &keys, true, true, Some(&sizes),
                       md5.then_some(etags.as_slice()));

        remove_notifications(&s3, &bucket).await;
        sns.delete_topic().topic_arn(&topic_arn).send().await.unwrap();
        s3.delete_bucket().bucket(&bucket).send().await.unwrap();
    }

    /* HTTPPostHandler's checks under cloudevents=True, per POST */
    fn check_cloudevents(received: &[Received]) {
        for post in received {
            let h = &post.headers;
            let record = &post.body["Records"][0];
            let get = |k: &str| h.get(k).map(String::as_str).unwrap_or_default();
            assert_eq!(get("ce-specversion"), "1.0");
            assert_eq!(get("ce-id"), format!("{}.{}",
                record["responseElements"]["x-amz-request-id"].as_str().unwrap_or_default(),
                record["responseElements"]["x-amz-id-2"].as_str().unwrap_or_default()));
            assert_eq!(get("ce-source"), format!("ceph:s3.{}.{}",
                record["awsRegion"].as_str().unwrap_or_default(),
                record["s3"]["bucket"]["name"].as_str().unwrap_or_default()));
            assert_eq!(get("ce-type"), format!("com.amazonaws.{}",
                record["eventName"].as_str().unwrap_or_default()));
            assert_eq!(get("content-type"), "application/json");
            assert_eq!(get("ce-subject"), record["s3"]["object"]["key"].as_str().unwrap_or_default());
            /* dateutil's parse() gives a datetime, which holds
             * microseconds:  ce-time carries nanoseconds and eventTime
             * microseconds, and the two compare at that resolution */
            let parse = |t: &str| {
                let d = chrono::DateTime::parse_from_rfc3339(t)
                    .unwrap_or_else(|e| panic!("not a time ({e}): {t:?}"));
                chrono::DurationRound::duration_trunc(d, chrono::TimeDelta::microseconds(1))
                    .unwrap()
            };
            assert_eq!(parse(get("ce-time")),
                       parse(record["eventTime"].as_str().unwrap_or_default()));
        }
    }

    /* PSNotificationS3.del_config():  an empty configuration */
    async fn remove_notifications(s3: &aws_sdk_s3::Client, bucket: &str) {
        s3.put_bucket_notification_configuration().bucket(bucket)
            .notification_configuration(NotificationConfiguration::builder().build())
            .send().await.unwrap();
    }

    #[cfg_attr(feature = "fails_on_aws", ignore = "fails on aws:  no push-endpoint")]
    #[tokio::test]
    async fn test_notification_http() {
        notification_http(false).await;
    }

    #[cfg_attr(feature = "fails_on_aws", ignore = "fails on aws:  no push-endpoint")]
    #[tokio::test]
    async fn test_notification_cloudevents() {
        notification_http(true).await;
    }

    #[cfg_attr(feature = "fails_on_aws", ignore = "fails on aws:  no push-endpoint")]
    #[tokio::test]
    async fn test_opaque_data() {
        let _guard = s3_tests_rs::fixtures::TestGuard::setup();
        let sns = get_sns_root_client();
        let s3 = get_iam_root_s3client();
        let bucket = get_new_bucket_name();
        s3.create_bucket().bucket(&bucket).send().await.unwrap();
        let receiver = HttpReceiver::start().await;
        let opaque_data = "http://1.2.3.4:8888";
        let topic_arn = topic_and_notification(&sns, &s3, &bucket, &receiver,
                                               &[("OpaqueData", opaque_data)], vec![]).await;

        let mut puts = Vec::new();
        for i in 0..10 {
            let (s3, bucket) = (s3.clone(), bucket.clone());
            puts.push(tokio::spawn(async move {
                s3.put_object().bucket(&bucket).key(i.to_string())
                    .body(ByteStream::from_static(b"bar")).send().await.unwrap();
            }));
        }
        for p in puts {
            p.await.unwrap();
        }
        wait_for_the_messages().await;

        for post in receiver.take() {
            assert_eq!(post.body["Records"][0]["opaqueData"].as_str(), Some(opaque_data));
        }

        for (name, _) in list_keys(&s3, &bucket).await {
            s3.delete_object().bucket(&bucket).key(&name).send().await.unwrap();
        }
        remove_notifications(&s3, &bucket).await;
        sns.delete_topic().topic_arn(&topic_arn).send().await.unwrap();
        s3.delete_bucket().bucket(&bucket).send().await.unwrap();
    }

    #[cfg_attr(feature = "fails_on_aws", ignore = "fails on aws:  no push-endpoint")]
    #[tokio::test]
    async fn test_notification_multi_delete() {
        let _guard = s3_tests_rs::fixtures::TestGuard::setup();
        let sns = get_sns_root_client();
        let s3 = get_iam_root_s3client();
        let bucket = get_new_bucket_name();
        s3.create_bucket().bucket(&bucket).send().await.unwrap();
        let receiver = HttpReceiver::start().await;
        let topic_arn = topic_and_notification(&sns, &s3, &bucket, &receiver, &[],
            vec![aws_sdk_s3::types::Event::S3ObjectRemoved]).await;

        let mut sizes = HashMap::new();
        let mut puts = Vec::new();
        for i in 0..10 {
            let content = random_content(rand::thread_rng().gen_range(1..=1024));
            sizes.insert(i.to_string(), content.len() as u64);
            let (s3, bucket) = (s3.clone(), bucket.clone());
            puts.push(tokio::spawn(async move {
                s3.put_object().bucket(&bucket).key(i.to_string())
                    .body(ByteStream::from(content)).send().await.unwrap();
            }));
        }
        for p in puts {
            p.await.unwrap();
        }
        let keys = list_keys(&s3, &bucket).await;

        /* delete_all_objects():  one multi-object delete */
        let objects = keys.iter()
            .map(|(k, _)| ObjectIdentifier::builder().key(k).build().unwrap())
            .collect::<Vec<_>>();
        s3.delete_objects().bucket(&bucket)
            .delete(Delete::builder().set_objects(Some(objects)).build().unwrap())
            .send().await.unwrap();
        wait_for_the_messages().await;

        verify_records(&receiver.take(), &bucket, &keys, true, true, Some(&sizes), None);

        sns.delete_topic().topic_arn(&topic_arn).send().await.unwrap();
        remove_notifications(&s3, &bucket).await;
        s3.delete_bucket().bucket(&bucket).send().await.unwrap();
    }

    /* multipart_endpoint_agnostic('http', conn):  despite the name, one
     * 20 MiB PUT, as boto2's set_contents_from_string is */
    #[cfg_attr(feature = "fails_on_aws", ignore = "fails on aws:  no push-endpoint")]
    #[tokio::test]
    async fn test_multipart_http() {
        let _guard = s3_tests_rs::fixtures::TestGuard::setup();
        let sns = get_sns_root_client();
        let s3 = get_iam_root_s3client();
        let bucket = get_new_bucket_name();
        s3.create_bucket().bucket(&bucket).send().await.unwrap();
        let receiver = HttpReceiver::start().await;
        let topic_arn = topic_and_notification(&sns, &s3, &bucket, &receiver, &[], vec![]).await;

        s3.put_object().bucket(&bucket).key("obj")
            .body(ByteStream::from(random_content(20 * 1024 * 1024))).send().await.unwrap();
        wait_for_the_messages().await;

        let keys = list_keys(&s3, &bucket).await;
        verify_records(&receiver.take(), &bucket, &keys, true, false, None, None);

        remove_notifications(&s3, &bucket).await;
        sns.delete_topic().topic_arn(&topic_arn).send().await.unwrap();
        for (name, _) in keys {
            s3.delete_object().bucket(&bucket).key(&name).send().await.unwrap();
        }
        s3.delete_bucket().bucket(&bucket).send().await.unwrap();
    }
}

/* Event types test_bn.py does not exercise, against the same in-process
 * endpoint:  the suite's own tests, not translations.  Each checks the
 * event and the case AWS defines as sending none. */
mod http_event_types {
    use std::time::Duration;

    use aws_sdk_s3::primitives::ByteStream;
    use aws_sdk_s3::types::{Event, NotificationConfiguration, ObjectCannedAcl,
                            ObjectOwnership, OwnershipControls, OwnershipControlsRule,
                            Tag, Tagging, TopicConfiguration, BucketCannedAcl};

    use s3_tests_rs::client::{get_iam_root_s3client, get_sns_root_client};
    use s3_tests_rs::fixtures::get_new_bucket_name;
    use s3_tests_rs::notify_receiver::{HttpReceiver, Received};

    async fn notify_on(sns: &aws_sdk_sns::Client, s3: &aws_sdk_s3::Client, bucket: &str,
                       receiver: &HttpReceiver, events: Vec<Event>) -> String {
        let topic_arn = sns.create_topic().name(format!("{bucket}_topic"))
            .attributes("push-endpoint", receiver.endpoint())
            .send().await.unwrap().topic_arn().unwrap().to_string();
        s3.put_bucket_notification_configuration().bucket(bucket)
            .notification_configuration(NotificationConfiguration::builder()
                .topic_configurations(TopicConfiguration::builder()
                    .id(format!("{bucket}_notif")).topic_arn(&topic_arn)
                    .set_events(Some(events)).build().unwrap())
                .build())
            .send().await.unwrap();
        topic_arn
    }

    fn records(received: &[Received]) -> Vec<serde_json::Value> {
        received.iter()
            .flat_map(|p| p.body["Records"].as_array().cloned().unwrap_or_default())
            .collect()
    }

    async fn settle() {
        tokio::time::sleep(Duration::from_secs(2)).await;
    }

    #[cfg_attr(feature = "fails_on_aws", ignore = "fails on aws:  no push-endpoint")]
    #[tokio::test]
    async fn test_notification_object_tagging_http() {
        let _guard = s3_tests_rs::fixtures::TestGuard::setup();
        let (sns, s3) = (get_sns_root_client(), get_iam_root_s3client());
        let bucket = get_new_bucket_name();
        s3.create_bucket().bucket(&bucket).send().await.unwrap();
        let receiver = HttpReceiver::start().await;
        let topic_arn = notify_on(&sns, &s3, &bucket, &receiver,
            vec![Event::from("s3:ObjectTagging:*")]).await;

        let put = s3.put_object().bucket(&bucket).key("tagged")
            .body(ByteStream::from_static(b"tagged body")).send().await.unwrap();
        let etag = put.e_tag().unwrap().trim_matches('"').to_string();
        settle().await;
        assert!(receiver.take().is_empty(), "a creation sent an event to a tagging-only configuration");

        s3.put_object_tagging().bucket(&bucket).key("tagged")
            .tagging(Tagging::builder().tag_set(Tag::builder().key("k").value("v").build().unwrap())
                     .build().unwrap())
            .send().await.unwrap();
        settle().await;
        let put_events = records(&receiver.take());
        assert_eq!(put_events.len(), 1, "PutObjectTagging:  {put_events:?}");
        let r = &put_events[0];
        assert_eq!(r["eventName"], "ObjectTagging:Put");
        assert_eq!(r["s3"]["object"]["key"], "tagged");
        assert_eq!(r["s3"]["object"]["size"], 11);
        assert_eq!(r["s3"]["object"]["eTag"], etag.as_str());

        s3.delete_object_tagging().bucket(&bucket).key("tagged").send().await.unwrap();
        settle().await;
        let del_events = records(&receiver.take());
        assert_eq!(del_events.len(), 1, "DeleteObjectTagging:  {del_events:?}");
        assert_eq!(del_events[0]["eventName"], "ObjectTagging:Delete");
        assert_eq!(del_events[0]["s3"]["object"]["key"], "tagged");

        s3.delete_object().bucket(&bucket).key("tagged").send().await.unwrap();
        sns.delete_topic().topic_arn(&topic_arn).send().await.unwrap();
        s3.delete_bucket().bucket(&bucket).send().await.unwrap();
    }

    /* The bucket ARN is one of the two forms rgw_notification_bucket_arn_region
     * allows:  AWS's, arn:aws:s3:::<bucket>, or the zonegroup in the
     * region field, which must then be the record's awsRegion.  Never an
     * account. */
    #[cfg_attr(feature = "fails_on_aws", ignore = "fails on aws:  no push-endpoint")]
    #[tokio::test]
    async fn test_notification_bucket_arn_http() {
        let _guard = s3_tests_rs::fixtures::TestGuard::setup();
        let (sns, s3) = (get_sns_root_client(), get_iam_root_s3client());
        let bucket = get_new_bucket_name();
        s3.create_bucket().bucket(&bucket).send().await.unwrap();
        let receiver = HttpReceiver::start().await;
        let topic_arn = notify_on(&sns, &s3, &bucket, &receiver,
            vec![Event::S3ObjectCreated]).await;

        s3.put_object().bucket(&bucket).key("arn")
            .body(ByteStream::from_static(b"arn")).send().await.unwrap();
        settle().await;
        let got = records(&receiver.take());
        assert_eq!(got.len(), 1, "{got:?}");
        let arn = got[0]["s3"]["bucket"]["arn"].as_str().unwrap_or_default().to_string();
        let region = got[0]["awsRegion"].as_str().unwrap_or_default();
        let aws = format!("arn:aws:s3:::{bucket}");
        let zonegroup = format!("arn:aws:s3:{region}::{bucket}");
        assert!(arn == aws || (!region.is_empty() && arn == zonegroup),
                "bucket ARN {arn:?} is neither {aws:?} nor {zonegroup:?}");

        s3.delete_object().bucket(&bucket).key("arn").send().await.unwrap();
        sns.delete_topic().topic_arn(&topic_arn).send().await.unwrap();
        s3.delete_bucket().bucket(&bucket).send().await.unwrap();
    }

    /* x-amz-id-2 names the gateway that made the change:  not empty, and
     * the same for two events from one gateway */
    #[cfg_attr(feature = "fails_on_aws", ignore = "fails on aws:  no push-endpoint")]
    #[tokio::test]
    async fn test_notification_host_id_http() {
        let _guard = s3_tests_rs::fixtures::TestGuard::setup();
        let (sns, s3) = (get_sns_root_client(), get_iam_root_s3client());
        let bucket = get_new_bucket_name();
        s3.create_bucket().bucket(&bucket).send().await.unwrap();
        let receiver = HttpReceiver::start().await;
        let topic_arn = notify_on(&sns, &s3, &bucket, &receiver,
            vec![Event::S3ObjectCreated]).await;

        for key in ["one", "two"] {
            s3.put_object().bucket(&bucket).key(key)
                .body(ByteStream::from_static(b"host")).send().await.unwrap();
        }
        settle().await;
        let got = records(&receiver.take());
        assert_eq!(got.len(), 2, "{got:?}");
        let ids: Vec<&str> = got.iter()
            .map(|r| r["responseElements"]["x-amz-id-2"].as_str().unwrap_or_default())
            .collect();
        assert!(!ids[0].is_empty(), "x-amz-id-2 is empty:  {got:?}");
        assert_eq!(ids[0], ids[1], "two events from one gateway name different hosts");

        for key in ["one", "two"] {
            s3.delete_object().bucket(&bucket).key(key).send().await.unwrap();
        }
        sns.delete_topic().topic_arn(&topic_arn).send().await.unwrap();
        s3.delete_bucket().bucket(&bucket).send().await.unwrap();
    }

    /* an event a request caused carries the client's address:  an IP
     * address, whichever one rgw_remote_addr_param selects */
    #[cfg_attr(feature = "fails_on_aws", ignore = "fails on aws:  no push-endpoint")]
    #[tokio::test]
    async fn test_notification_source_ip_http() {
        let _guard = s3_tests_rs::fixtures::TestGuard::setup();
        let (sns, s3) = (get_sns_root_client(), get_iam_root_s3client());
        let bucket = get_new_bucket_name();
        s3.create_bucket().bucket(&bucket).send().await.unwrap();
        let receiver = HttpReceiver::start().await;
        let topic_arn = notify_on(&sns, &s3, &bucket, &receiver,
            vec![Event::S3ObjectCreated]).await;

        s3.put_object().bucket(&bucket).key("ip")
            .body(ByteStream::from_static(b"ip")).send().await.unwrap();
        settle().await;
        let got = records(&receiver.take());
        assert_eq!(got.len(), 1, "{got:?}");
        let ip = got[0]["requestParameters"]["sourceIPAddress"].as_str().unwrap_or_default();
        assert!(ip.parse::<std::net::IpAddr>().is_ok(),
                "sourceIPAddress {ip:?} is not an IP address");

        s3.delete_object().bucket(&bucket).key("ip").send().await.unwrap();
        sns.delete_topic().topic_arn(&topic_arn).send().await.unwrap();
        s3.delete_bucket().bucket(&bucket).send().await.unwrap();
    }

    #[cfg_attr(feature = "fails_on_aws", ignore = "fails on aws:  no push-endpoint")]
    #[tokio::test]
    async fn test_notification_object_acl_http() {
        let _guard = s3_tests_rs::fixtures::TestGuard::setup();
        let (sns, s3) = (get_sns_root_client(), get_iam_root_s3client());
        let bucket = get_new_bucket_name();
        s3.create_bucket().bucket(&bucket).send().await.unwrap();
        s3.put_bucket_ownership_controls().bucket(&bucket)
            .ownership_controls(OwnershipControls::builder()
                .rules(OwnershipControlsRule::builder()
                    .object_ownership(ObjectOwnership::ObjectWriter).build().unwrap())
                .build().unwrap())
            .send().await.unwrap();
        let receiver = HttpReceiver::start().await;
        let topic_arn = notify_on(&sns, &s3, &bucket, &receiver,
            vec![Event::from("s3:ObjectAcl:Put")]).await;

        s3.put_object().bucket(&bucket).key("acl")
            .body(ByteStream::from_static(b"acl body")).send().await.unwrap();
        settle().await;
        assert!(receiver.take().is_empty(), "a creation sent an event to an ACL-only configuration");

        s3.put_object_acl().bucket(&bucket).key("acl").acl(ObjectCannedAcl::PublicRead)
            .send().await.unwrap();
        settle().await;
        let changed = records(&receiver.take());
        assert_eq!(changed.len(), 1, "an ACL change:  {changed:?}");
        assert_eq!(changed[0]["eventName"], "ObjectAcl:Put");
        assert_eq!(changed[0]["s3"]["object"]["key"], "acl");

        /* "An event is not generated when a request results in no change
         * to an object's ACL" */
        s3.put_object_acl().bucket(&bucket).key("acl").acl(ObjectCannedAcl::PublicRead)
            .send().await.unwrap();
        settle().await;
        assert!(receiver.take().is_empty(), "the same ACL again sent an event");

        /* the event is an object's;  a bucket's ACL sends none */
        s3.put_bucket_acl().bucket(&bucket).acl(BucketCannedAcl::PublicRead)
            .send().await.unwrap();
        settle().await;
        assert!(receiver.take().is_empty(), "a bucket ACL sent an event");

        s3.delete_object().bucket(&bucket).key("acl").send().await.unwrap();
        sns.delete_topic().topic_arn(&topic_arn).send().await.unwrap();
        s3.delete_bucket().bucket(&bucket).send().await.unwrap();
    }

    /* HTTPS to a receiver under a CA of its own:  the topic's ca-location
     * names that CA, and with mTLS its ssl-certificate-location and
     * ssl-key-location the client certificate.  Each case also delivers
     * without the setting, which the receiver's handshake refuses, so a
     * setting the gateway ignored would fail the test.  The files are
     * the test process's, so the gateway must share its filesystem. */
    async fn https_delivers(attributes: &[(&str, &str)], receiver: &HttpReceiver) -> bool {
        let (sns, s3) = (get_sns_root_client(), get_iam_root_s3client());
        let bucket = get_new_bucket_name();
        s3.create_bucket().bucket(&bucket).send().await.unwrap();
        let mut req = sns.create_topic().name(format!("{bucket}_topic"))
            .attributes("push-endpoint", receiver.endpoint());
        for (k, v) in attributes {
            req = req.attributes(*k, *v);
        }
        let topic_arn = req.send().await.unwrap().topic_arn().unwrap().to_string();
        s3.put_bucket_notification_configuration().bucket(&bucket)
            .notification_configuration(NotificationConfiguration::builder()
                .topic_configurations(TopicConfiguration::builder()
                    .id(format!("{bucket}_notif")).topic_arn(&topic_arn)
                    .events(Event::from("s3:ObjectCreated:*")).build().unwrap())
                .build())
            .send().await.unwrap();
        let handshakes = receiver.failed_handshakes();
        s3.put_object().bucket(&bucket).key("https")
            .body(ByteStream::from_static(b"https body")).send().await.unwrap();
        settle().await;
        let got = records(&receiver.take());
        let delivered = got.iter().any(|r| r["s3"]["bucket"]["name"] == bucket.as_str()
                                           && r["s3"]["object"]["key"] == "https");
        if !delivered {
            assert!(receiver.failed_handshakes() > handshakes,
                    "no event, and no refused handshake either");
        }
        s3.delete_object().bucket(&bucket).key("https").send().await.unwrap();
        sns.delete_topic().topic_arn(&topic_arn).send().await.unwrap();
        s3.delete_bucket().bucket(&bucket).send().await.unwrap();
        delivered
    }

    #[cfg_attr(feature = "fails_on_aws", ignore = "fails on aws:  no push-endpoint")]
    #[tokio::test]
    async fn test_notification_https_ca_location() {
        let _guard = s3_tests_rs::fixtures::TestGuard::setup();
        let receiver = HttpReceiver::start_tls(false).await;
        let ca = receiver.tls_files().ca.to_str().unwrap().to_string();
        assert!(!https_delivers(&[], &receiver).await,
                "delivered to a server whose CA the gateway was not given");
        assert!(https_delivers(&[("ca-location", &ca)], &receiver).await,
                "not delivered with ca-location");
    }

    #[cfg_attr(feature = "fails_on_aws", ignore = "fails on aws:  no push-endpoint")]
    #[tokio::test]
    async fn test_notification_https_client_certificate() {
        let _guard = s3_tests_rs::fixtures::TestGuard::setup();
        let receiver = HttpReceiver::start_tls(true).await;
        let files = receiver.tls_files();
        let (ca, cert, key) = (files.ca.to_str().unwrap().to_string(),
                               files.client_cert.to_str().unwrap().to_string(),
                               files.client_key.to_str().unwrap().to_string());
        assert!(!https_delivers(&[("ca-location", &ca)], &receiver).await,
                "delivered to a server requiring a client certificate, without one");
        assert!(https_delivers(&[("ca-location", &ca), ("ssl-certificate-location", &cert),
                                 ("ssl-key-location", &key)], &receiver).await,
                "not delivered with a client certificate");

        /* the client certificate does not depend on verifying the
         * server:  NooBaa's rejectUnauthorized: false with local_file_cert
         * and local_file_key */
        assert!(!https_delivers(&[("verify-ssl", "false")], &receiver).await,
                "delivered without a client certificate, the server unverified");
        assert!(https_delivers(&[("verify-ssl", "false"), ("ssl-certificate-location", &cert),
                                 ("ssl-key-location", &key)], &receiver).await,
                "not delivered with a client certificate, the server unverified");
    }
}
