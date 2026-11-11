//! A file changed in a bucket by a filesystem client -- written over NFS
//! or SMB, or by a local tool -- through neither the S3 path nor the
//! gateway's listing cache.
//!
//! NooBaa lists such a file at once:  its directory cache checks each
//! directory's inode and mtime on every listing, and every entry it
//! returns is stat'd (noobaa-core `68ca22d33`, `namespace_fs.js:346`).
//! RGW's listing cache is filled once per bucket and never checked
//! again, so a change after a bucket's first listing is missing from its
//! listings until a restart (DECISIONS 2026-11-11, the revalidation
//! option, not built).  Those tests are ignored on nsfs;  `--ignored`
//! runs them.  GET and HEAD read the file itself and are tested as
//! they stand.
//!
//! The ETag after an in-place overwrite follows NooBaa's `_get_etag()`
//! (`namespace_fs.js:2866`):  a stored digest is returned whatever the
//! file now holds;  a file with none gets a token from its current inode
//! and mtime, which an overwrite changes.  NooBaa with MD5 off stores no
//! token, so an object RGW wrote under `rgw_non_md5_etag` should change
//! too;  ours keeps the token recorded at its PUT (punch list
//! 2026-11-11).  An object RGW wrote also carries the checksum its PUT
//! sent, which GET returns and SDKs validate;  NooBaa stores none, and
//! after an overwrite outside ours fails every read (punch list
//! 2026-11-11).
//!
//! The gateway makes the changes, through the `sideload` driver hint,
//! as the symlink tests make links.  Under impersonation they are made
//! as the main user, as that identity's filesystem client would.

use std::collections::HashMap;
use std::time::Duration;

use aws_sdk_s3::primitives::ByteStream;
use s3_tests_rs::admin::{driver_hint_results, q};
use s3_tests_rs::client::get_client;
use s3_tests_rs::config::get_config;
use s3_tests_rs::features::features;
use s3_tests_rs::fixtures::{get_new_bucket, TestGuard};

/// `None` from the accessor means unknown, so skip rather than assert.
async fn nsfs_only() -> bool {
    match features().await {
        Some(f) => f.backend.as_deref() == Some("nsfs"),
        None => false,
    }
}

async fn feature(name: &str) -> bool {
    match features().await {
        Some(f) => f.get_bool(name).unwrap_or(false),
        None => false,
    }
}

/// One `sideload` operation;  `None` where the hint is not reachable,
/// so the caller skips.
async fn sideload(op: &str, bucket: &str, key: &str, extra: &[(&str, &str)])
    -> Option<HashMap<String, String>> {
    let me = get_config().main_user_id.clone();
    let mut params = vec![("op", op), ("bucket", bucket), ("key", key)];
    params.extend_from_slice(extra);
    if feature("impersonate").await {
        params.push(("as", me.as_str()));
    }
    driver_hint_results("sideload", &params).await
}

/// `body` written at `key`, as a filesystem client would;  `None` where
/// the hint is not reachable.
async fn write(bucket: &str, key: &str, body: &str) -> Option<()> {
    let body = q(body);
    let r = sideload("write", bucket, key, &[("body", body.as_str())]).await?;
    assert!(r.contains_key("ino"), "the hint wrote {key} but reports no inode");
    Some(())
}

async fn put(bucket: &str, key: &str, body: &str) {
    get_client().put_object().bucket(bucket).key(key)
        .body(ByteStream::from(body.as_bytes().to_vec()))
        .send().await
        .unwrap_or_else(|e| panic!("PUT {key}: {e:?}"));
}

async fn body(bucket: &str, key: &str) -> String {
    let out = get_client().get_object().bucket(bucket).key(key)
        .send().await
        .unwrap_or_else(|e| panic!("GET {key}: {e:?}"));
    String::from_utf8(out.body.collect().await.unwrap().into_bytes().to_vec())
        .unwrap()
}

/// HEAD's size and ETag.
async fn head(bucket: &str, key: &str) -> (i64, String) {
    let out = get_client().head_object().bucket(bucket).key(key)
        .send().await
        .unwrap_or_else(|e| panic!("HEAD {key}: {e:?}"));
    (out.content_length().unwrap_or(-1),
     out.e_tag().unwrap_or_default().to_string())
}

/// Every key and its size, and the common prefixes, listed with
/// `delimiter`.
async fn list(bucket: &str, delimiter: Option<&str>)
    -> (HashMap<String, i64>, Vec<String>) {
    let mut req = get_client().list_objects_v2().bucket(bucket);
    if let Some(d) = delimiter {
        req = req.delimiter(d);
    }
    let out = req.send().await
        .unwrap_or_else(|e| panic!("LIST {bucket}: {e:?}"));
    let keys = out.contents().iter()
        .filter_map(|o| Some((o.key()?.to_string(), o.size().unwrap_or(-1))))
        .collect();
    let prefixes = out.common_prefixes().iter()
        .filter_map(|p| p.prefix().map(str::to_string))
        .collect();
    (keys, prefixes)
}

/// A bucket holding `seen`, PUT and then listed, so that its listing
/// cache is filled before the test changes anything.
async fn listed_bucket() -> String {
    let bucket = get_new_bucket(Some(&get_client())).await;
    put(&bucket, "seen", "seen").await;
    let (keys, _) = list(&bucket, None).await;
    assert!(keys.contains_key("seen"), "the bucket's first listing lacks seen");
    bucket
}

/// A file written before the bucket's first listing is listed with its
/// size, and read.  The first listing reads the directory, so this holds
/// with or without revalidation.
#[cfg_attr(not(feature = "rgw_admin"), ignore = "requires rgw_admin feature")]
#[tokio::test]
async fn test_sideloaded_before_the_first_listing_is_listed_and_read() {
    let _guard = TestGuard::setup();
    if !nsfs_only().await {
        eprintln!("not nsfs;  nothing to check");
        return;
    }
    let bucket = get_new_bucket(Some(&get_client())).await;
    if write(&bucket, "early.txt", "written outside").await.is_none() {
        eprintln!("sideload hint unavailable;  nothing to check");
        return;
    }
    let (keys, _) = list(&bucket, None).await;
    assert_eq!(keys.get("early.txt"), Some(&15), "listing: {keys:?}");
    assert_eq!(body(&bucket, "early.txt").await, "written outside");
    assert_eq!(head(&bucket, "early.txt").await.0, 15);
}

/// A file written after the bucket was listed is in its next listing.
#[cfg_attr(not(feature = "rgw_admin"), ignore = "requires rgw_admin feature")]
#[cfg_attr(feature = "fails_on_nsfs",
           ignore = "nsfs: the listing cache does not revalidate (DECISIONS 2026-11-11)")]
#[tokio::test]
async fn test_sideloaded_after_a_listing_is_listed() {
    let _guard = TestGuard::setup();
    if !nsfs_only().await {
        eprintln!("not nsfs;  nothing to check");
        return;
    }
    let bucket = listed_bucket().await;
    if write(&bucket, "late.txt", "written outside").await.is_none() {
        eprintln!("sideload hint unavailable;  nothing to check");
        return;
    }
    let (keys, _) = list(&bucket, None).await;
    assert_eq!(keys.get("late.txt"), Some(&15), "listing: {keys:?}");
    assert!(keys.contains_key("seen"), "listing: {keys:?}");
}

/// A file written under directories made outside, after the bucket was
/// listed, is in its next listing, and a delimited listing shows the new
/// top directory as a common prefix.
#[cfg_attr(not(feature = "rgw_admin"), ignore = "requires rgw_admin feature")]
#[cfg_attr(feature = "fails_on_nsfs",
           ignore = "nsfs: the listing cache does not revalidate (DECISIONS 2026-11-11)")]
#[tokio::test]
async fn test_sideloaded_under_new_directories_after_a_listing_is_listed() {
    let _guard = TestGuard::setup();
    if !nsfs_only().await {
        eprintln!("not nsfs;  nothing to check");
        return;
    }
    let bucket = listed_bucket().await;
    if write(&bucket, "d1/d2/deep.txt", "deep").await.is_none() {
        eprintln!("sideload hint unavailable;  nothing to check");
        return;
    }
    let (keys, _) = list(&bucket, None).await;
    assert_eq!(keys.get("d1/d2/deep.txt"), Some(&4), "listing: {keys:?}");
    let (top, prefixes) = list(&bucket, Some("/")).await;
    assert!(prefixes.contains(&"d1/".to_string()),
            "delimited listing: {top:?}, prefixes {prefixes:?}");
    assert_eq!(body(&bucket, "d1/d2/deep.txt").await, "deep");
}

/// A file removed outside is NoSuchKey to GET at once.
#[cfg_attr(not(feature = "rgw_admin"), ignore = "requires rgw_admin feature")]
#[tokio::test]
async fn test_removed_outside_reads_as_no_such_key() {
    let _guard = TestGuard::setup();
    if !nsfs_only().await {
        eprintln!("not nsfs;  nothing to check");
        return;
    }
    let bucket = listed_bucket().await;
    if sideload("unlink", &bucket, "seen", &[]).await.is_none() {
        eprintln!("sideload hint unavailable;  nothing to check");
        return;
    }
    let r = get_client().get_object().bucket(&bucket).key("seen").send().await;
    s3_tests_rs::assert_s3_err!(r, 404, "NoSuchKey");
}

/// A file removed outside, after the bucket was listed, is gone from its
/// next listing.
#[cfg_attr(not(feature = "rgw_admin"), ignore = "requires rgw_admin feature")]
#[cfg_attr(feature = "fails_on_nsfs",
           ignore = "nsfs: the listing cache does not revalidate (DECISIONS 2026-11-11)")]
#[tokio::test]
async fn test_removed_outside_after_a_listing_is_not_listed() {
    let _guard = TestGuard::setup();
    if !nsfs_only().await {
        eprintln!("not nsfs;  nothing to check");
        return;
    }
    let bucket = listed_bucket().await;
    put(&bucket, "kept", "kept").await;
    if sideload("unlink", &bucket, "seen", &[]).await.is_none() {
        eprintln!("sideload hint unavailable;  nothing to check");
        return;
    }
    let (keys, _) = list(&bucket, None).await;
    assert!(!keys.contains_key("seen"), "listing: {keys:?}");
    assert!(keys.contains_key("kept"), "listing: {keys:?}");
}

/// A file written outside under a temporary name and then renamed, as
/// Samba and rsync write, is listed under its final name only.
#[cfg_attr(not(feature = "rgw_admin"), ignore = "requires rgw_admin feature")]
#[cfg_attr(feature = "fails_on_nsfs",
           ignore = "nsfs: the listing cache does not revalidate (DECISIONS 2026-11-11)")]
#[tokio::test]
async fn test_renamed_outside_after_a_listing_is_listed_by_its_new_name() {
    let _guard = TestGuard::setup();
    if !nsfs_only().await {
        eprintln!("not nsfs;  nothing to check");
        return;
    }
    let bucket = listed_bucket().await;
    if write(&bucket, "report.partial", "final contents").await.is_none() {
        eprintln!("sideload hint unavailable;  nothing to check");
        return;
    }
    let r = sideload("rename", &bucket, "report.partial", &[("to", "report.txt")])
        .await
        .expect("the hint wrote the file but did not rename it");
    assert!(r.contains_key("ino"), "the rename left nothing at report.txt");
    let (keys, _) = list(&bucket, None).await;
    assert_eq!(keys.get("report.txt"), Some(&14), "listing: {keys:?}");
    assert!(!keys.contains_key("report.partial"), "listing: {keys:?}");
}

/// A file written outside and then overwritten in place reads with its
/// new contents and size at once.
#[cfg_attr(not(feature = "rgw_admin"), ignore = "requires rgw_admin feature")]
#[tokio::test]
async fn test_a_sideloaded_file_overwritten_in_place_reads_its_new_contents() {
    let _guard = TestGuard::setup();
    if !nsfs_only().await {
        eprintln!("not nsfs;  nothing to check");
        return;
    }
    let bucket = get_new_bucket(Some(&get_client())).await;
    if write(&bucket, "plain.txt", "first").await.is_none() {
        eprintln!("sideload hint unavailable;  nothing to check");
        return;
    }
    write(&bucket, "plain.txt", "rewritten outside").await
        .expect("the hint wrote the file once but not again");
    assert_eq!(body(&bucket, "plain.txt").await, "rewritten outside");
    assert_eq!(head(&bucket, "plain.txt").await.0, 17);
}

/// An object RGW wrote, then overwritten in place outside, reads with its
/// new contents.  The PUT stored the SDK's checksum, which GET returns
/// and the SDK validates, so a checksum that no longer describes the
/// file fails every read of it;  NooBaa stores and returns none.
#[cfg_attr(not(feature = "rgw_admin"), ignore = "requires rgw_admin feature")]
#[cfg_attr(feature = "fails_on_nsfs",
           ignore = "nsfs: a stored checksum outlives an overwrite outside (punch list 2026-11-11)")]
#[tokio::test]
async fn test_an_object_overwritten_in_place_reads_its_new_contents() {
    let _guard = TestGuard::setup();
    if !nsfs_only().await {
        eprintln!("not nsfs;  nothing to check");
        return;
    }
    let bucket = listed_bucket().await;
    if write(&bucket, "seen", "rewritten outside").await.is_none() {
        eprintln!("sideload hint unavailable;  nothing to check");
        return;
    }
    assert_eq!(body(&bucket, "seen").await, "rewritten outside");
    assert_eq!(head(&bucket, "seen").await.0, 17);
}

/// An object overwritten in place outside, after the bucket was listed,
/// is listed with its new size.  With the revalidation option a listing
/// checks an entry at most once per interval, so this will run where the
/// interval is 0 and skip elsewhere.
#[cfg_attr(not(feature = "rgw_admin"), ignore = "requires rgw_admin feature")]
#[cfg_attr(feature = "fails_on_nsfs",
           ignore = "nsfs: the listing cache does not revalidate (DECISIONS 2026-11-11)")]
#[tokio::test]
async fn test_overwritten_in_place_after_a_listing_is_listed_with_its_new_size() {
    let _guard = TestGuard::setup();
    if !nsfs_only().await {
        eprintln!("not nsfs;  nothing to check");
        return;
    }
    let bucket = listed_bucket().await;
    if write(&bucket, "seen", "rewritten outside").await.is_none() {
        eprintln!("sideload hint unavailable;  nothing to check");
        return;
    }
    let (keys, _) = list(&bucket, None).await;
    assert_eq!(keys.get("seen"), Some(&17), "listing: {keys:?}");
}

/// An object carrying a stored digest keeps that ETag after an in-place
/// overwrite outside, as NooBaa's does (`_get_etag()` returns
/// `user.content_md5` whatever the file holds).  Under
/// `rgw_non_md5_etag` a PUT stores no digest;  the next test covers it.
#[cfg_attr(not(feature = "rgw_admin"), ignore = "requires rgw_admin feature")]
#[tokio::test]
async fn test_a_stored_digest_survives_an_overwrite_in_place() {
    let _guard = TestGuard::setup();
    if !nsfs_only().await {
        eprintln!("not nsfs;  nothing to check");
        return;
    }
    if feature("non_md5_etag").await {
        eprintln!("rgw_non_md5_etag:  a PUT stores no digest;  nothing to check");
        return;
    }
    let bucket = get_new_bucket(Some(&get_client())).await;
    put(&bucket, "digested", "first").await;
    let (_, before) = head(&bucket, "digested").await;
    if write(&bucket, "digested", "second, longer").await.is_none() {
        eprintln!("sideload hint unavailable;  nothing to check");
        return;
    }
    let (size, after) = head(&bucket, "digested").await;
    assert_eq!(size, 14);
    assert_eq!(after, before, "the stored digest was not returned");
}

/// A file with no stored ETag gets a token from its inode and mtime, and
/// an in-place overwrite outside changes it, as NooBaa's does.
#[cfg_attr(not(feature = "rgw_admin"), ignore = "requires rgw_admin feature")]
#[tokio::test]
async fn test_a_sideloaded_files_etag_follows_an_overwrite_in_place() {
    let _guard = TestGuard::setup();
    if !nsfs_only().await {
        eprintln!("not nsfs;  nothing to check");
        return;
    }
    let bucket = get_new_bucket(Some(&get_client())).await;
    if write(&bucket, "plain.txt", "first").await.is_none() {
        eprintln!("sideload hint unavailable;  nothing to check");
        return;
    }
    let (_, before) = head(&bucket, "plain.txt").await;
    /* so the mtime moves on a filesystem with coarse timestamps */
    tokio::time::sleep(Duration::from_millis(20)).await;
    write(&bucket, "plain.txt", "second, longer").await
        .expect("the hint wrote the file once but not again");
    let (size, after) = head(&bucket, "plain.txt").await;
    assert_eq!(size, 14);
    assert_ne!(after, before, "the ETag did not follow the overwrite");
}

/// An object RGW wrote under `rgw_non_md5_etag` should get a new ETag
/// after an in-place overwrite outside, as NooBaa with MD5 off computes
/// its token from the current stat;  ours returns the token recorded at
/// the PUT.
#[cfg_attr(not(feature = "rgw_admin"), ignore = "requires rgw_admin feature")]
#[cfg_attr(feature = "fails_on_nsfs",
           ignore = "nsfs: a recorded non-MD5 etag outlives an overwrite outside (punch list 2026-11-11)")]
#[tokio::test]
async fn test_a_non_md5_etag_follows_an_overwrite_in_place() {
    let _guard = TestGuard::setup();
    if !nsfs_only().await {
        eprintln!("not nsfs;  nothing to check");
        return;
    }
    if !feature("non_md5_etag").await {
        eprintln!("not rgw_non_md5_etag;  the previous test covers a digest");
        return;
    }
    let bucket = get_new_bucket(Some(&get_client())).await;
    put(&bucket, "tokened", "first").await;
    let (_, before) = head(&bucket, "tokened").await;
    tokio::time::sleep(Duration::from_millis(20)).await;
    if write(&bucket, "tokened", "second, longer").await.is_none() {
        eprintln!("sideload hint unavailable;  nothing to check");
        return;
    }
    let (size, after) = head(&bucket, "tokened").await;
    assert_eq!(size, 14);
    assert_ne!(after, before, "the recorded token outlived the overwrite");
}
