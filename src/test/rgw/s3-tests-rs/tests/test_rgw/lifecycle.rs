//! How lifecycle ages a noncurrent version on nsfs.
//!
//! RGW ages a noncurrent version from the mtime of the version that
//! replaced it, which is when it stopped being current only where
//! nothing but RGW sets mtime.  On nsfs the successor is a file, and
//! touch -d, cp -p or a restore from backup can set its mtime into the
//! past, which expired the version early (work-plan stage N-LC).  nsfs
//! returns the time of the demote, as NooBaa does, and lifecycle ages
//! the version by that.  The successor's mtime is set through the
//! `set-mtime` driver hint, since S3 cannot set it, and
//! `inject-noncurrent-since-none` takes the demote time away, so the
//! same run shows the aging lifecycle falls back to.

use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{
    BucketLifecycleConfiguration, ExpirationStatus, LifecycleRule,
    LifecycleRuleFilter, NoncurrentVersionExpiration,
};
use aws_sdk_s3::Client;

use s3_tests_rs::admin::driver_hint_results;
use s3_tests_rs::client::get_client;
use s3_tests_rs::config::get_config;
use s3_tests_rs::features::features;
use s3_tests_rs::fixtures::{
    check_configure_versioning_retry, get_new_bucket, TestGuard,
};

const KEEP_DAYS: i32 = 30;
const AGO: u64 = 86400;
const NONE_HINT: &str = "inject-noncurrent-since-none";

/// Two keys of two versions each;  `keep/a`'s current version's mtime set
/// `AGO` seconds back;  `keep/` expiring a noncurrent version after
/// `KEEP_DAYS`, `control/` after one day.
async fn stand_up(client: &Client, bucket: &str) {
    check_configure_versioning_retry(client, bucket, "Enabled", "Enabled")
        .await;
    for key in ["keep/a", "control/a"] {
        for body in [&b"one"[..], &b"two"[..]] {
            client.put_object().bucket(bucket).key(key)
                .body(ByteStream::from(body.to_vec()))
                .send().await
                .expect("could not write a version");
        }
    }

    let ago = AGO.to_string();
    let r = driver_hint_results("set-mtime", &[("bucket", bucket),
                                               ("key", "keep/a"),
                                               ("ago", &ago)])
        .await
        .expect("set-mtime returned no result");
    let set: u64 = r.get("mtime").and_then(|s| s.parse().ok())
        .expect("set-mtime returned no mtime");
    let head = client.head_object().bucket(bucket).key("keep/a")
        .send().await
        .expect("could not HEAD keep/a");
    let seen = head.last_modified().expect("HEAD returned no Last-Modified")
        .secs() as u64;
    assert_eq!(seen, set, "the current version's mtime was not set");

    let rule = |id: &str, prefix: &str, days: i32| {
        LifecycleRule::builder()
            .id(id)
            .filter(LifecycleRuleFilter::builder().prefix(prefix).build())
            .noncurrent_version_expiration(
                NoncurrentVersionExpiration::builder()
                    .noncurrent_days(days)
                    .build())
            .status(ExpirationStatus::Enabled)
            .build()
            .unwrap()
    };
    client.put_bucket_lifecycle_configuration().bucket(bucket)
        .lifecycle_configuration(BucketLifecycleConfiguration::builder()
            .rules(rule("keep", "keep/", KEEP_DAYS))
            .rules(rule("control", "control/", 1))
            .build()
            .unwrap())
        .send().await
        .expect("could not set the lifecycle configuration");
}

async fn versions(client: &Client, bucket: &str, prefix: &str) -> usize {
    client.list_object_versions().bucket(bucket).prefix(prefix)
        .send().await
        .expect("could not list versions")
        .versions()
        .len()
}

fn fired(r: &std::collections::HashMap<String, String>) -> u64 {
    r.get("fired").and_then(|s| s.parse().ok())
        .expect("the hint returned no count")
}

/// A version whose successor's mtime was set a day back is kept, and is
/// expired where the demote time is taken away.
///
/// `KEEP_DAYS` is 30 intervals under `rgw_lc_debug_interval`:  longer
/// than the test waits, far shorter than `AGO`.  So aged by the demote,
/// `keep/a`'s noncurrent version is kept;  aged by its successor's
/// mtime, it is expired.  Two buckets, set up alike, wait out the same
/// interval:  in the first the demote time decides, and in the second
/// `inject-noncurrent-since-none` takes it away, and its count must rise.
/// In both, `control/a`'s noncurrent version must go, which shows
/// lifecycle processed the bucket in the wait.  The hint is disarmed
/// before anything is asserted.
#[cfg_attr(not(feature = "rgw_admin"), ignore = "requires rgw_admin feature")]
#[cfg_attr(feature = "fails_on_aws", ignore = "fails on aws")]
#[tokio::test]
async fn test_lifecycle_noncurrent_aged_by_demote_not_successor_mtime() {
    let _guard = TestGuard::setup();

    let Some(f) = features().await else {
        eprintln!("no features returned;  nothing to check");
        return;
    };
    if f.backend.as_deref() != Some("nsfs") {
        eprintln!("set-mtime is nsfs-only;  skipped");
        return;
    }

    let client = get_client();
    let lc_interval = get_config().lc_debug_interval;
    assert!(KEEP_DAYS as u64 * lc_interval < AGO,
            "rgw_lc_debug_interval {lc_interval} is too long for this test");

    let demote = get_new_bucket(Some(&client)).await;
    let fallback = get_new_bucket(Some(&client)).await;
    let armed = driver_hint_results(NONE_HINT, &[("enable", "true"),
                                                 ("bucket", &fallback)])
        .await
        .expect("the hint did not arm");
    let before = fired(&armed);
    stand_up(&client, &demote).await;
    stand_up(&client, &fallback).await;

    tokio::time::sleep(std::time::Duration::from_secs(7 * lc_interval)).await;

    let disarmed = driver_hint_results(NONE_HINT, &[("enable", "false"),
                                                    ("bucket", &fallback)])
        .await
        .expect("the hint did not disarm");
    let after = fired(&disarmed);

    for bucket in [&demote, &fallback] {
        assert_eq!(versions(&client, bucket, "control/").await, 1,
                   "{bucket}:  the control's noncurrent version was not \
                    expired;  lifecycle did not process the bucket");
    }
    assert_eq!(versions(&client, &demote, "keep/").await, 2,
               "a noncurrent version was expired by its successor's mtime");
    assert!(after > before, "{NONE_HINT} did not fire");
    assert_eq!(versions(&client, &fallback, "keep/").await, 1,
               "with no demote time, the noncurrent version was not aged \
                by its successor's mtime");
}
