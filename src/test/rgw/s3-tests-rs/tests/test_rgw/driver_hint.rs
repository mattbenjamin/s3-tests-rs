//! The driver hint interface itself.
//!
//! `DELETE /admin/driver/hint` lets a test reach a driver-internal control
//! that has no S3 spelling -- arm a fallback path, drop a cache, inject a
//! failure.  Half of that is acting and half is answering:  a hint reports
//! results, and without them a test could arm the buffered copy and then
//! only assume it ran.
//!
//! These cover the wiring rather than any one hint's meaning.  The
//! endpoint is dev-gated (`rgw_driver_debug_apis`) and needs the
//! `driver-hint` capability, so against anything but a development
//! gateway it answers 403 and every test here skips -- the same rule the
//! features query follows, where absent means unknown and never false.

use std::collections::HashMap;
use std::panic::{resume_unwind, AssertUnwindSafe};

use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{BucketVersioningStatus, VersioningConfiguration};
use futures_util::FutureExt;

use s3_tests_rs::admin::driver_hint_results;
use s3_tests_rs::client::get_client;
use s3_tests_rs::features::{feature_bool, features};
use s3_tests_rs::fixtures::{get_new_bucket, PrivateUser, TestGuard};

/* this module's user's POSIX identity, under impersonation:  the
 * fixture identity impersonation.rs binds its users to */
const UID_BUFCOPY: &str = "70101";
const GID_BUFCOPY: &str = "70010";

/// Both surfaces read the same `get_features()`, so a disagreement means
/// one of them is rendering it wrong.  Some values are live counts --
/// `hint_unmarked_buckets` -- which tests running beside this one change,
/// so `/admin/features` is read before and after the hint, and a value
/// the two reads give differently moved meanwhile and is not compared.
#[cfg_attr(not(feature = "rgw_admin"), ignore = "requires rgw_admin feature")]
#[tokio::test]
async fn test_driver_hint_results_round_trip() {
    let _guard = TestGuard::setup();

    let before = features().await;
    let Some(results) = driver_hint_results("fs-features", &[]).await else {
        eprintln!("driver hint endpoint unavailable;  nothing to check");
        return;
    };

    assert!(!results.is_empty(),
        "fs-features answered with no results at all");

    let (Some(before), Some(after)) = (before, features().await) else {
        eprintln!("no features reported;  cross-check skipped");
        return;
    };
    for (k, v) in &results {
        if let (Some(b), Some(a)) = (before.get_str(k), after.get_str(k)) {
            if b != a {
                continue;
            }
            assert_eq!(b, v,
                "/admin/features and the fs-features hint disagree on {k}");
        }
    }
}

/// The hint arms the buffered copy for one user and reports how many
/// bytes that user's copies moved through it.
///
/// Armed with `user=`, the hint forces and counts this test's user's
/// copies alone, so other tests' copies -- the suite runs in parallel --
/// neither take the fallback nor reach the count, and the count is exact.
/// The main user's copy in between is the control for that:  it is
/// another user's, so it must add nothing.
#[cfg_attr(not(feature = "rgw_admin"), ignore = "requires rgw_admin feature")]
#[tokio::test]
async fn test_driver_hint_buffered_copy_accounts_bytes() {
    let _guard = TestGuard::setup();

    let Some(f) = features().await else {
        eprintln!("no features reported;  nothing to check");
        return;
    };
    if f.backend.as_deref() != Some("nsfs") {
        eprintln!("inject-buffered-copy is nsfs-only;  skipped");
        return;
    }

    let Some(user) = PrivateUser::create("bufcopy").await else {
        eprintln!("could not make this test's own user");
        return;
    };
    /* under impersonation a user with no identity is denied;  without
     * it the row is not consulted */
    if feature_bool("impersonate").await == Some(true) {
        assert!(user.bind(UID_BUFCOPY, GID_BUFCOPY, None).await,
            "binding this test's user failed");
    }

    /* arm, and zero the counter */
    let Some(armed) = driver_hint_results(
        "inject-buffered-copy",
        &[("enable", "true"), ("user", &user.id)]).await else {
        eprintln!("driver hint endpoint unavailable;  nothing to check");
        return;
    };
    assert_eq!(armed.get("enabled").map(String::as_str), Some("true"),
        "the hint did not report itself armed");
    assert!(armed.contains_key("buffered_bytes"),
        "the hint reported no byte count");

    /* The control.  Arming zeroes the counter, so arming again with no
     * copy in between has to report zero -- otherwise the assertion
     * below could pass on bytes this test did not cause, and would prove
     * nothing about which path the copy took. */
    let zeroed = driver_hint_results(
        "inject-buffered-copy",
        &[("enable", "true"), ("user", &user.id)]).await
        .expect("hint answered once and should answer again");
    assert_eq!(zeroed.get("buffered_bytes").map(String::as_str), Some("0"),
        "the byte count did not reset;  a nonzero result below would not \
         be evidence");

    const SIZE: usize = 1 << 20;
    let body = vec![b'x'; SIZE];

    /* another user's copy, which the bundle must not see */
    let main = get_client();
    let other = get_new_bucket(Some(&main)).await;
    main.put_object()
        .bucket(&other).key("src")
        .body(ByteStream::from(body.clone()))
        .send().await.unwrap();
    main.copy_object()
        .bucket(&other).key("dst")
        .copy_source(format!("{other}/src"))
        .send().await.unwrap();
    let after_other = driver_hint_results(
        "inject-buffered-copy",
        &[("enable", "true"), ("user", &user.id)]).await
        .expect("hint answered once and should answer again");
    assert_eq!(after_other.get("buffered_bytes").map(String::as_str),
        Some("0"),
        "another user's copy was counted:  the hint is not scoped to \
         its user");

    let client = &user.client;
    let bucket = get_new_bucket(Some(client)).await;
    client.put_object()
        .bucket(&bucket).key("src")
        .body(ByteStream::from(body))
        .send().await.unwrap();

    client.copy_object()
        .bucket(&bucket).key("dst")
        .copy_source(format!("{bucket}/src"))
        .send().await.unwrap();

    let after = driver_hint_results(
        "inject-buffered-copy",
        &[("enable", "false"), ("user", &user.id)]).await
        .expect("hint answered once and should answer again");
    let moved: u64 = after.get("buffered_bytes")
        .expect("no byte count after the copy")
        .parse().expect("byte count is not a number");

    assert_eq!(moved, SIZE as u64,
        "buffered copy accounted {moved} bytes for a {SIZE}-byte object;  \
         the copy did not take the fallback, or another copy was counted");
    assert_eq!(after.get("enabled").map(String::as_str), Some("false"),
        "the hint did not report itself disarmed");
}

/// `pool-hops` for `user`:  its hops to the helper pool since the last
/// call, by kind, reset.  `None` when the gateway has no such hint.
async fn pool_hops(user: &str) -> Option<HashMap<String, u64>> {
    let out = driver_hint_results("pool-hops", &[("user", user)]).await?;
    let mut n = HashMap::new();
    for k in ["link", "attr_write", "attr_read", "other"] {
        n.insert(k.to_string(), out.get(k)?.parse().ok()?);
    }
    Some(n)
}

async fn set_no_ring(user: &str, on: bool) {
    let _ = driver_hint_results(
        "inject-no-ring",
        &[("user", user), ("enable", if on { "true" } else { "false" })]).await;
}

/// A put, an overwrite and a versioned delete of `key`, as `user`.
async fn put_overwrite_delete(user: &PrivateUser, bucket: &str, key: &str) {
    let c = &user.client;
    for body in ["first", "second"] {
        c.put_object().bucket(bucket).key(key)
            .body(ByteStream::from_static(body.as_bytes()))
            .send().await.unwrap();
    }
    c.delete_object().bucket(bucket).key(key).send().await.unwrap();
}

/// N-D3, end to end:  under impersonation an identity's links and
/// attribute operations run on the ring.  A put, an overwrite and a
/// versioned delete -- the publish link, the demotes, the CAS unlink, a
/// delete marker and its attributes -- make no attribute-read hop to the
/// helper pool, and, where the gateway reports `ring_metadata_changes`,
/// no link or attribute-write hop either.  On GPFS it reports false:
/// metadata changes stay on the pool, whose eight helpers keep them under
/// GPFS's concurrency cliff, so links and attribute writes there hop.
///
/// Two controls.  The main user's operations in between are another
/// user's, so `pool-hops` must not count them:  the hint is scoped to
/// its user.  And with the ring withheld for this user
/// (`inject-no-ring`) the same sequence must hop for each kind, which
/// shows the counter can see a hop and that the fallback, today's path,
/// still serves:  the requests succeed either way.
#[cfg_attr(not(feature = "rgw_admin"), ignore = "requires rgw_admin feature")]
#[tokio::test]
async fn test_driver_hint_links_and_attributes_take_no_hop() {
    let _guard = TestGuard::setup();

    let Some(f) = features().await else {
        eprintln!("no features reported;  nothing to check");
        return;
    };
    if f.backend.as_deref() != Some("nsfs")
        || f.get_bool("impersonate") != Some(true) {
        eprintln!("the ring serves an identity only under impersonation;  \
                   skipped");
        return;
    }
    /* the gateway says which way its metadata changes go;  absent, as
     * before N-D3's GPFS switch, means the ring */
    let meta_on_ring = f.get_bool("ring_metadata_changes").unwrap_or(true);

    let Some(user) = PrivateUser::create("nohop").await else {
        eprintln!("could not make this test's own user");
        return;
    };
    assert!(user.bind(UID_BUFCOPY, GID_BUFCOPY, None).await,
        "binding this test's user failed");
    /* a leak from a killed run heals here rather than deciding this one */
    set_no_ring(&user.id, false).await;
    if pool_hops(&user.id).await.is_none() {
        eprintln!("no pool-hops hint;  nothing to check");
        return;
    }

    let bucket = get_new_bucket(Some(&user.client)).await;
    user.client.put_bucket_versioning().bucket(&bucket)
        .versioning_configuration(VersioningConfiguration::builder()
            .status(BucketVersioningStatus::Enabled).build())
        .send().await.unwrap();
    let _ = pool_hops(&user.id).await;

    /* the scope's control:  another user's put and delete are not this
     * user's hops */
    let main = get_client();
    let other = get_new_bucket(Some(&main)).await;
    main.put_object().bucket(&other).key("k")
        .body(ByteStream::from_static(b"main")).send().await.unwrap();
    main.delete_object().bucket(&other).key("k").send().await.unwrap();
    let none = pool_hops(&user.id).await.unwrap();
    assert!(none.values().all(|&n| n == 0),
        "another user's requests were counted:  {none:?}");

    put_overwrite_delete(&user, &bucket, "ring/k").await;
    let ring = pool_hops(&user.id).await.unwrap();
    assert_eq!(ring["attr_read"], 0,
        "an attribute read took the pool:  {ring:?}");
    if meta_on_ring {
        assert_eq!(ring["link"], 0, "a link took the pool:  {ring:?}");
        assert_eq!(ring["attr_write"], 0,
            "an attribute write took the pool:  {ring:?}");
    } else {
        assert!(ring["link"] > 0 && ring["attr_write"] > 0,
            "the gateway reports metadata changes on the pool, and none \
             took it:  {ring:?}");
    }

    /* the negative control, cleared on the panicking path too */
    set_no_ring(&user.id, true).await;
    let out = AssertUnwindSafe(
        put_overwrite_delete(&user, &bucket, "pool/k")).catch_unwind().await;
    set_no_ring(&user.id, false).await;
    if let Err(panic) = out {
        resume_unwind(panic);
    }
    let pool = pool_hops(&user.id).await.unwrap();
    assert!(pool["link"] > 0 && pool["attr_write"] > 0 && pool["attr_read"] > 0,
        "with the ring withheld the same requests made no hop of some \
         kind, so the zeros above are not evidence:  {pool:?}");

    let _ = driver_hint_results("pool-hops",
        &[("user", &user.id), ("enable", "false")]).await;
}
