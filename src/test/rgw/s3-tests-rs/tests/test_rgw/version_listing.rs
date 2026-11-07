//! A version listing taken while a write is publishing.
//!
//! nsfs serves `ListObjectVersions` from a bucket cache it fills from
//! the directory on first use.  A versioned write publishes its object
//! by a link and a rename, and wrote the object's `version_id` attribute
//! after them:  a listing that filled the cache in that moment read no
//! attribute, cached the object as the null version, and that entry
//! named nothing on disk.  It listed until the gateway restarted, and
//! each delete of it succeeded and removed nothing --
//! `versioning::test_versioned_concurrent_object_create_and_remove`
//! failed "versions remain after cleanup" on GPFS this way.
//!
//! The moment is too short to reach by timing, so a write is held in
//! it (`inject-hold-after-publish`, keyed by bucket so the tests beside
//! it are not touched).  `inject-version-id-after-publish` restores the
//! order the write used, which is how the second test shows the first
//! can fail.

use std::time::{Duration, Instant};

use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{BucketVersioningStatus, VersioningConfiguration};

use s3_tests_rs::admin::driver_hint_results;
use s3_tests_rs::client::get_client;
use s3_tests_rs::features::features;
use s3_tests_rs::fixtures::{get_new_bucket_name, TestGuard};

/// A new bucket with versioning enabled.
async fn versioned_bucket(client: &aws_sdk_s3::Client) -> String {
    let bucket = get_new_bucket_name();
    client.create_bucket().bucket(&bucket).send().await
        .expect("could not create the bucket");
    client.put_bucket_versioning()
        .bucket(&bucket)
        .versioning_configuration(VersioningConfiguration::builder()
            .status(BucketVersioningStatus::Enabled).build())
        .send().await
        .expect("could not enable versioning");
    bucket
}

/// Arms or releases a bucket-keyed hint, returning its results.
async fn hint(name: &str, bucket: &str, on: bool)
    -> Option<std::collections::HashMap<String, String>> {
    driver_hint_results(name,
        &[("enable", if on { "true" } else { "false" }), ("bucket", bucket)])
        .await
}

/// Waits until a write in `bucket` is held after its publish.
async fn wait_until_held(bucket: &str) {
    let until = Instant::now() + Duration::from_secs(20);
    loop {
        let r = hint("inject-hold-after-publish", bucket, true).await
            .expect("the hold hint returned once and should return again");
        if r.get("waiting").map(String::as_str) == Some("true") {
            return;
        }
        assert!(Instant::now() < until,
            "no write in {bucket} reached the hold after its publish");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// The version ids listed for `key`, delete markers excluded.
async fn listed_versions(client: &aws_sdk_s3::Client, bucket: &str, key: &str)
    -> Vec<String> {
    let resp = client.list_object_versions().bucket(bucket).send().await
        .expect("could not list versions");
    resp.versions().iter()
        .filter(|v| v.key() == Some(key))
        .map(|v| v.version_id().unwrap_or_default().to_string())
        .collect()
}

/// Whether this gateway is nsfs with driver hints, which both tests need.
async fn nsfs_with_hints() -> bool {
    let Some(f) = features().await else {
        eprintln!("no features returned;  nothing to check");
        return false;
    };
    if f.backend.as_deref() != Some("nsfs") {
        eprintln!("inject-hold-after-publish is nsfs-only;  skipped");
        return false;
    }
    true
}

/// A listing that fills the cache while a write is in place, and before
/// anything after its publish has run, lists the object under the
/// version id the write returns.
#[cfg_attr(not(feature = "rgw_admin"), ignore = "requires rgw_admin feature")]
#[tokio::test]
async fn test_a_listing_during_a_publish_has_its_version_id() {
    let _guard = TestGuard::setup();
    if !nsfs_with_hints().await {
        return;
    }

    let client = get_client();
    let bucket = versioned_bucket(&client).await;
    let key = "published";

    if hint("inject-hold-after-publish", &bucket, true).await.is_none() {
        eprintln!("driver hint endpoint unavailable;  nothing to check");
        return;
    }
    let put = {
        let (c, b) = (client.clone(), bucket.clone());
        tokio::spawn(async move {
            c.put_object().bucket(&b).key(key)
                .body(ByteStream::from_static(b"abc"))
                .send().await
        })
    };
    wait_until_held(&bucket).await;

    /* the first listing of the bucket, so it fills the cache from disk */
    let during = listed_versions(&client, &bucket, key).await;
    hint("inject-hold-after-publish", &bucket, false).await
        .expect("the hold hint returned once and should return again");
    let put = put.await.expect("the write's task failed")
        .expect("the held write failed");
    let version_id = put.version_id().unwrap_or_default().to_string();

    assert!(!version_id.is_empty(), "a versioned write returned no version id");
    assert_eq!(during, vec![version_id.clone()],
        "a listing taken just after the publish did not list the object \
         under the version id the write returned");
    assert_eq!(listed_versions(&client, &bucket, key).await, vec![version_id],
        "the listing after the write is not the one version it wrote");
}

/// The order the write used -- the `version_id` attribute after the
/// publish -- lists the object as the null version in that moment, and
/// leaves a null entry beside the real one.  A delete of that entry
/// removes it, though nothing on disk is behind it.
///
/// The first half shows the test above can fail:  the same hold, with
/// the attribute written after it, gives the listing it rejects.
#[cfg_attr(not(feature = "rgw_admin"), ignore = "requires rgw_admin feature")]
#[tokio::test]
async fn test_a_version_with_nothing_behind_it_is_deleted() {
    let _guard = TestGuard::setup();
    if !nsfs_with_hints().await {
        return;
    }

    let client = get_client();
    let bucket = versioned_bucket(&client).await;
    let key = "published";

    if hint("inject-version-id-after-publish", &bucket, true).await.is_none() {
        eprintln!("driver hint endpoint unavailable;  nothing to check");
        return;
    }
    hint("inject-hold-after-publish", &bucket, true).await
        .expect("the hold hint is unavailable although another is not");
    let put = {
        let (c, b) = (client.clone(), bucket.clone());
        tokio::spawn(async move {
            c.put_object().bucket(&b).key(key)
                .body(ByteStream::from_static(b"abc"))
                .send().await
        })
    };
    wait_until_held(&bucket).await;

    let during = listed_versions(&client, &bucket, key).await;
    hint("inject-hold-after-publish", &bucket, false).await
        .expect("the hold hint returned once and should return again");
    let put = put.await.expect("the write's task failed")
        .expect("the held write failed");
    hint("inject-version-id-after-publish", &bucket, false).await
        .expect("the order hint returned once and should return again");
    let version_id = put.version_id().unwrap_or_default().to_string();

    assert_eq!(during, vec!["null".to_string()],
        "with the version id written after the publish, a listing just \
         after it should have read the object as the null version;  the \
         injection did not reproduce the defect, so the test above shows \
         nothing");

    let mut after = listed_versions(&client, &bucket, key).await;
    after.sort();
    let mut expected = vec!["null".to_string(), version_id.clone()];
    expected.sort();
    assert_eq!(after, expected,
        "the write should have left the null entry beside its own version");

    client.delete_object().bucket(&bucket).key(key).version_id("null")
        .send().await
        .expect("the delete of the null entry failed");
    assert_eq!(listed_versions(&client, &bucket, key).await, vec![version_id],
        "the null entry, with nothing on disk behind it, still lists after \
         its delete");
}
