//! What the listing cache's two revalidation intervals bound (DECISIONS
//! 2026-11-11):  an object overwritten in place outside the gateway is
//! listed as it is within the entry interval, and a file created outside
//! beside the gateway's own write is listed within the provisional
//! interval.
//!
//! The defaults are 30 s and 5 min, so each test shortens its interval
//! through the `listing-intervals` driver hint, which changes it for the
//! whole daemon:  these tests run alone, after baseline (profile
//! `listing-intervals`).  The override lapses by itself, and each test
//! also drops it when done.

use std::time::Duration;

use s3_tests_rs::admin::driver_hint_results;
use s3_tests_rs::fixtures::TestGuard;

use crate::sideload::{feature, list, listed_bucket, nsfs_only, put, write};

/// The intervals set, for 30 s;  `None` where the hint is not
/// reachable, so the caller skips.
async fn set_intervals(params: &[(&str, &str)]) -> Option<()> {
    let mut p = params.to_vec();
    p.push(("for", "30"));
    driver_hint_results("listing-intervals", &p).await.map(|_| ())
}

async fn reset_intervals() {
    let _ = driver_hint_results("listing-intervals", &[("reset", "true")]).await;
}

/// An object overwritten in place moves no directory's mtime, so only the
/// per-entry check finds it, within the entry interval.
#[cfg_attr(not(feature = "rgw_admin"), ignore = "requires rgw_admin feature")]
#[tokio::test]
async fn test_an_overwrite_in_place_is_listed_within_the_entry_interval() {
    let _guard = TestGuard::setup();
    if !nsfs_only().await {
        eprintln!("not nsfs;  nothing to check");
        return;
    }
    if !feature("listing_revalidate").await {
        eprintln!("the gateway trusts its listing cache;  nothing to check");
        return;
    }
    if set_intervals(&[("entry", "1")]).await.is_none() {
        eprintln!("listing-intervals hint unavailable;  nothing to check");
        return;
    }
    let bucket = listed_bucket().await;
    if write(&bucket, "seen", "rewritten outside").await.is_none() {
        reset_intervals().await;
        eprintln!("sideload hint unavailable;  nothing to check");
        return;
    }
    tokio::time::sleep(Duration::from_millis(1500)).await;
    let (keys, _) = list(&bucket, None).await;
    reset_intervals().await;
    assert_eq!(keys.get("seen"), Some(&17), "listing: {keys:?}");
}

/// A file created outside in a directory the gateway then wrote to is
/// absorbed into the directory's new record, and listed once the
/// provisional interval has passed.
#[cfg_attr(not(feature = "rgw_admin"), ignore = "requires rgw_admin feature")]
#[tokio::test]
async fn test_an_outside_create_beside_a_put_is_listed_after_the_provisional_interval() {
    let _guard = TestGuard::setup();
    if !nsfs_only().await {
        eprintln!("not nsfs;  nothing to check");
        return;
    }
    if !feature("listing_revalidate").await {
        eprintln!("the gateway trusts its listing cache;  nothing to check");
        return;
    }
    if set_intervals(&[("provisional", "1")]).await.is_none() {
        eprintln!("listing-intervals hint unavailable;  nothing to check");
        return;
    }
    let bucket = listed_bucket().await;
    if write(&bucket, "beside.txt", "beside").await.is_none() {
        reset_intervals().await;
        eprintln!("sideload hint unavailable;  nothing to check");
        return;
    }
    put(&bucket, "by-rgw", "by-rgw").await;
    tokio::time::sleep(Duration::from_millis(1500)).await;
    let (keys, _) = list(&bucket, None).await;
    reset_intervals().await;
    assert!(keys.contains_key("beside.txt"), "listing: {keys:?}");
    assert!(keys.contains_key("by-rgw"), "listing: {keys:?}");
}
