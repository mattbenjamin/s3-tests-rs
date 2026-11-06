use aws_sdk_s3::primitives::ByteStream;
use s3_tests_rs::client::{
    build_iam_client_with_session_token, build_s3_client, build_s3_client_with_session_token,
    build_sts_client, get_iam_alt_root_client, get_iam_alt_root_s3client,
    get_iam_root_client, get_iam_root_s3client, get_sts_client,
};
use s3_tests_rs::access_model::{Access, Class};
use s3_tests_rs::admin::{fs_access, FsAccess};
use s3_tests_rs::config::get_config;
use s3_tests_rs::fixtures::{get_new_bucket, get_new_bucket_name};
use s3_tests_rs::webidentity::WebIdRole;

#[tokio::test]
async fn test_assume_role() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let iam_client = get_iam_root_client();
    let sts_client = get_sts_client();
    let cfg = get_config();

    let role_name = format!("{}AssumeTestRole", cfg.iam_name_prefix);

    let trust_policy = serde_json::json!({
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Principal": {"AWS": "*"},
            "Action": "sts:AssumeRole"
        }]
    })
    .to_string();

    let create_resp = iam_client
        .create_role()
        .role_name(&role_name)
        .assume_role_policy_document(&trust_policy)
        .path(&cfg.iam_path_prefix)
        .send()
        .await
        .unwrap();
    let role_arn = create_resp.role().unwrap().arn().to_string();

    let s3_full_access = serde_json::json!({
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Action": "s3:*",
            "Resource": "*"
        }]
    })
    .to_string();

    iam_client
        .put_role_policy()
        .role_name(&role_name)
        .policy_name("S3FullAccess")
        .policy_document(&s3_full_access)
        .send()
        .await
        .unwrap();

    let assume_resp = sts_client
        .assume_role()
        .role_arn(&role_arn)
        .role_session_name("test-session")
        .send()
        .await
        .unwrap();

    let creds = assume_resp.credentials().unwrap();
    let temp_access_key = creds.access_key_id();
    let temp_secret_key = creds.secret_access_key();
    let temp_session_token = creds.session_token();

    let s3_client =
        build_s3_client_with_session_token(temp_access_key, temp_secret_key, temp_session_token);

    let bucket = get_new_bucket_name();
    s3_client
        .create_bucket()
        .bucket(&bucket)
        .send()
        .await
        .unwrap();

    s3_client
        .put_object()
        .bucket(&bucket)
        .key("sts-test-object")
        .body(ByteStream::from_static(b"hello from assumed role"))
        .send()
        .await
        .unwrap();

    let get_resp = s3_client
        .get_object()
        .bucket(&bucket)
        .key("sts-test-object")
        .send()
        .await
        .unwrap();
    let body = get_resp.body.collect().await.unwrap().into_bytes();
    assert_eq!(&body[..], b"hello from assumed role");

    s3_client
        .delete_object()
        .bucket(&bucket)
        .key("sts-test-object")
        .send()
        .await
        .unwrap();

    s3_client
        .delete_bucket()
        .bucket(&bucket)
        .send()
        .await
        .unwrap();

    iam_client
        .delete_role_policy()
        .role_name(&role_name)
        .policy_name("S3FullAccess")
        .send()
        .await
        .unwrap();

    iam_client
        .delete_role()
        .role_name(&role_name)
        .send()
        .await
        .unwrap();
}

#[tokio::test]
async fn test_account_role_policy_allow() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let iam = get_iam_root_client();
    let cfg = get_config();
    let path = &cfg.iam_path_prefix;
    let user_name = format!("{}RPUser", cfg.iam_name_prefix);
    let role_name = format!("{}RPRole", cfg.iam_name_prefix);

    let user = iam.create_user().user_name(&user_name).path(path).send().await.unwrap();
    let user_arn = user.user().unwrap().arn().to_string();

    let trust_policy = serde_json::json!({
        "Version": "2012-10-17",
        "Statement": [{"Effect": "Allow", "Action": "sts:AssumeRole",
                        "Principal": {"AWS": user_arn}}]
    }).to_string();

    let role = iam.create_role().role_name(&role_name).path(path)
        .assume_role_policy_document(&trust_policy).send().await.unwrap();
    let role_arn = role.role().unwrap().arn().to_string();

    let key = iam.create_access_key().user_name(&user_name).send().await.unwrap()
        .access_key().unwrap().clone();
    let sts = build_sts_client(key.access_key_id(), key.secret_access_key());

    let assume = sts.assume_role().role_arn(&role_arn)
        .role_session_name("test").send().await.unwrap();
    let creds = assume.credentials().unwrap();
    let s3 = build_s3_client_with_session_token(
        creds.access_key_id(), creds.secret_access_key(), creds.session_token());

    /* denied without role policy */
    let err = s3.list_buckets().send().await;
    assert!(err.is_err());

    /* add role policy */
    let policy = serde_json::json!({
        "Version": "2012-10-17",
        "Statement": [{"Effect": "Allow", "Action": "s3:ListAllMyBuckets", "Resource": "*"}]
    }).to_string();
    iam.put_role_policy().role_name(&role_name).policy_name("AllowList")
        .policy_document(&policy).send().await.unwrap();

    /* now allowed */
    let resp = s3.list_buckets().send().await;
    assert!(resp.is_ok(), "should be allowed after role policy");

    /* clean up */
    iam.delete_role_policy().role_name(&role_name).policy_name("AllowList").send().await.unwrap();
    iam.delete_access_key().user_name(&user_name).access_key_id(key.access_key_id()).send().await.unwrap();
    iam.delete_user().user_name(&user_name).send().await.unwrap();
    iam.delete_role().role_name(&role_name).send().await.unwrap();
}

#[tokio::test]
async fn test_cross_account_assume_role() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let iam_root = get_iam_root_client();
    let iam_alt = get_iam_alt_root_client();
    let cfg = get_config();
    let path = &cfg.iam_path_prefix;
    let user_name = format!("{}XAUser", cfg.iam_name_prefix);
    let role_name = format!("{}XARole", cfg.iam_name_prefix);

    /* create user in alt account */
    let user = iam_alt.create_user().user_name(&user_name).path(path).send().await.unwrap();
    let user_arn = user.user().unwrap().arn().to_string();
    let key = iam_alt.create_access_key().user_name(&user_name).send().await.unwrap()
        .access_key().unwrap().clone();

    /* create role in main account trusting the alt user */
    let trust_policy = serde_json::json!({
        "Version": "2012-10-17",
        "Statement": [{"Effect": "Allow", "Action": "sts:AssumeRole",
                        "Principal": {"AWS": user_arn}}]
    }).to_string();
    let role = iam_root.create_role().role_name(&role_name).path(path)
        .assume_role_policy_document(&trust_policy).send().await.unwrap();
    let role_arn = role.role().unwrap().arn().to_string();

    /* alt user assumes main account role */
    let sts = build_sts_client(key.access_key_id(), key.secret_access_key());
    let assume = sts.assume_role().role_arn(&role_arn)
        .role_session_name("xacct").send().await.unwrap();
    let creds = assume.credentials().unwrap();

    /* create bucket with main account root */
    let roots3 = get_iam_root_s3client();
    let bucket = get_new_bucket(Some(&roots3)).await;

    let s3 = build_s3_client_with_session_token(
        creds.access_key_id(), creds.secret_access_key(), creds.session_token());

    /* denied without role policy */
    let err = s3.list_objects_v2().bucket(&bucket).send().await;
    assert!(err.is_err());

    /* add role policy allowing s3:ListBucket */
    let policy = serde_json::json!({
        "Version": "2012-10-17",
        "Statement": [{"Effect": "Allow", "Action": "s3:ListBucket", "Resource": "*"}]
    }).to_string();
    iam_root.put_role_policy().role_name(&role_name).policy_name("AllowList")
        .policy_document(&policy).send().await.unwrap();

    /* now allowed (same-account bucket) */
    let resp = s3.list_objects_v2().bucket(&bucket).send().await;
    assert!(resp.is_ok(), "should be allowed with role policy on same-account bucket");

    /* clean up */
    roots3.delete_bucket().bucket(&bucket).send().await.unwrap();
    iam_root.delete_role_policy().role_name(&role_name).policy_name("AllowList").send().await.unwrap();
    iam_root.delete_role().role_name(&role_name).send().await.unwrap();
    iam_alt.delete_access_key().user_name(&user_name).access_key_id(key.access_key_id()).send().await.unwrap();
    iam_alt.delete_user().user_name(&user_name).send().await.unwrap();
}

#[tokio::test]
async fn test_cross_account_role_create_bucket() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let iam_root = get_iam_root_client();
    let iam_alt = get_iam_alt_root_client();
    let cfg = get_config();
    let path = &cfg.iam_path_prefix;
    let user_name = format!("{}XCBUser", cfg.iam_name_prefix);
    let role_name = format!("{}XCBRole", cfg.iam_name_prefix);
    let bucket_name = get_new_bucket_name();

    let user = iam_alt.create_user().user_name(&user_name).path(path).send().await.unwrap();
    let user_arn = user.user().unwrap().arn().to_string();
    let key = iam_alt.create_access_key().user_name(&user_name).send().await.unwrap()
        .access_key().unwrap().clone();

    let trust_policy = serde_json::json!({
        "Version": "2012-10-17",
        "Statement": [{"Effect": "Allow", "Action": "sts:AssumeRole",
                        "Principal": {"AWS": user_arn}}]
    }).to_string();
    let role = iam_root.create_role().role_name(&role_name).path(path)
        .assume_role_policy_document(&trust_policy).send().await.unwrap();
    let role_arn = role.role().unwrap().arn().to_string();

    let sts = build_sts_client(key.access_key_id(), key.secret_access_key());
    let assume = sts.assume_role().role_arn(&role_arn)
        .role_session_name("xacct-cb").send().await.unwrap();
    let creds = assume.credentials().unwrap();

    let s3 = build_s3_client_with_session_token(
        creds.access_key_id(), creds.secret_access_key(), creds.session_token());

    /* denied without role policy */
    let err = s3.create_bucket().bucket(&bucket_name).send().await;
    assert!(err.is_err());

    /* add role policy allowing CreateBucket */
    let policy = serde_json::json!({
        "Version": "2012-10-17",
        "Statement": [{"Effect": "Allow", "Action": ["s3:CreateBucket", "s3:PutBucketAcl"], "Resource": "*"}]
    }).to_string();
    iam_root.put_role_policy().role_name(&role_name).policy_name("AllowCreate")
        .policy_document(&policy).send().await.unwrap();

    /* now allowed */
    s3.create_bucket().bucket(&bucket_name).send().await.unwrap();

    /* clean up */
    let roots3 = get_iam_root_s3client();
    roots3.delete_bucket().bucket(&bucket_name).send().await.unwrap();
    iam_root.delete_role_policy().role_name(&role_name).policy_name("AllowCreate").send().await.unwrap();
    iam_root.delete_role().role_name(&role_name).send().await.unwrap();
    iam_alt.delete_access_key().user_name(&user_name).access_key_id(key.access_key_id()).send().await.unwrap();
    iam_alt.delete_user().user_name(&user_name).send().await.unwrap();
}

#[tokio::test]
async fn test_cross_account_role_get_role() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let iam_root = get_iam_root_client();
    let iam_alt = get_iam_alt_root_client();
    let cfg = get_config();
    let path = &cfg.iam_path_prefix;
    let user_name = format!("{}XGRUser", cfg.iam_name_prefix);
    let role_name = format!("{}XGRRole", cfg.iam_name_prefix);

    let user = iam_alt.create_user().user_name(&user_name).path(path).send().await.unwrap();
    let user_arn = user.user().unwrap().arn().to_string();
    let key = iam_alt.create_access_key().user_name(&user_name).send().await.unwrap()
        .access_key().unwrap().clone();

    let trust_policy = serde_json::json!({
        "Version": "2012-10-17",
        "Statement": [{"Effect": "Allow", "Action": "sts:AssumeRole",
                        "Principal": {"AWS": user_arn}}]
    }).to_string();
    let role = iam_root.create_role().role_name(&role_name).path(path)
        .assume_role_policy_document(&trust_policy).send().await.unwrap();
    let role_arn = role.role().unwrap().arn().to_string();

    let sts = build_sts_client(key.access_key_id(), key.secret_access_key());
    let assume = sts.assume_role().role_arn(&role_arn)
        .role_session_name("xacct-gr").send().await.unwrap();
    let creds = assume.credentials().unwrap();

    let session_iam = build_iam_client_with_session_token(
        creds.access_key_id(), creds.secret_access_key(), creds.session_token());

    /* denied without role policy */
    let err = session_iam.get_role().role_name(&role_name).send().await;
    assert!(err.is_err());

    /* add role policy allowing GetRole */
    let policy = serde_json::json!({
        "Version": "2012-10-17",
        "Statement": [{"Effect": "Allow", "Action": "iam:GetRole", "Resource": "*"}]
    }).to_string();
    iam_root.put_role_policy().role_name(&role_name).policy_name("AllowGetRole")
        .policy_document(&policy).send().await.unwrap();

    /* now allowed */
    let resp = session_iam.get_role().role_name(&role_name).send().await;
    assert!(resp.is_ok(), "should be allowed with role policy for GetRole");

    /* clean up */
    iam_root.delete_role_policy().role_name(&role_name).policy_name("AllowGetRole").send().await.unwrap();
    iam_root.delete_role().role_name(&role_name).send().await.unwrap();
    iam_alt.delete_access_key().user_name(&user_name).access_key_id(key.access_key_id()).send().await.unwrap();
    iam_alt.delete_user().user_name(&user_name).send().await.unwrap();
}

/* AssumeRoleWithWebIdentity against a provider registered with
 * `client_ids`, then a bucket created and deleted with the session's
 * credentials */
async fn assume_role_with_web_identity(name: &str, client_ids: &[&str]) {
    let role_policy = serde_json::json!({
        "Version": "2012-10-17",
        "Statement": {"Effect": "Allow", "Action": "s3:*", "Resource": "arn:aws:s3:::*"}
    }).to_string();
    let role = WebIdRole::create(&get_iam_root_client(), &get_config().iam_root_user_id,
                                 name, client_ids, &role_policy).await;
    let s3 = role.session().await;

    let bucket = get_new_bucket_name();
    s3.create_bucket().bucket(&bucket).send().await.unwrap();
    s3.delete_bucket().bucket(&bucket).send().await.unwrap();

    role.delete().await;
}

/* A provider registered with no client IDs, as test_sts.py's:  RGW then
 * checks no audience */
#[tokio::test]
#[cfg_attr(not(feature = "has_keycloak"), ignore = "requires Keycloak (--features has_keycloak)")]
async fn test_assume_role_with_web_identity() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    assume_role_with_web_identity("WebIdRole", &[]).await;
}

/* Rust-only, an addition to test_sts.py:  a provider registered with
 * the token's audience, so RGW checks aud/azp against it */
#[tokio::test]
#[cfg_attr(not(feature = "has_keycloak"), ignore = "requires Keycloak (--features has_keycloak)")]
async fn test_assume_role_with_web_identity_client_id() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let cfg = get_config();
    let aud = cfg.webidentity_aud.as_deref().unwrap_or("account").to_string();
    assume_role_with_web_identity("WebIdClientIdRole", &[&aud]).await;
}

/* Rust-only, X2c:  a web identity session in [iam root]'s account reads
 * and writes a bucket of [iam alt root]'s account under that bucket's
 * policy, and is refused AccessDenied on one with no grant.  The role's
 * own policy allows S3 everywhere, so the bucket policy is the only
 * grant that differs.  Where the gateway impersonates, the session runs
 * as the role's identity and the other account's object is not its
 * own:  `Access` expects what the access model does with that, the
 * read and write served under `s3-native` and refused under
 * `posix-strict`. */
#[tokio::test]
#[cfg_attr(not(feature = "has_keycloak"), ignore = "requires Keycloak (--features has_keycloak)")]
async fn test_cross_account_web_identity_bucket_policy() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let role_policy = serde_json::json!({
        "Version": "2012-10-17",
        "Statement": {"Effect": "Allow", "Action": "s3:*", "Resource": "arn:aws:s3:::*"}
    }).to_string();
    let role = WebIdRole::create(&get_iam_root_client(), &get_config().iam_root_user_id,
                                 "WebIdXAcctRole", &[], &role_policy).await;
    /* the role principal, as `authenticated_user(s)` renders it for an
     * account with no tenant */
    let principal = format!("$role${}", role.role_id);

    let alt_s3 = get_iam_alt_root_s3client();
    let granted = get_new_bucket_name();
    let ungranted = get_new_bucket_name();
    for b in [&granted, &ungranted] {
        alt_s3.create_bucket().bucket(b).send().await.unwrap();
        alt_s3.put_object().bucket(b).key("alt-obj")
            .body(ByteStream::from_static(b"alt")).send().await.unwrap();
    }
    let policy = serde_json::json!({
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Principal": {"AWS": [&role.role_arn]},
            "Action": ["s3:GetObject", "s3:PutObject"],
            "Resource": [format!("arn:aws:s3:::{granted}/*")]
        }]
    }).to_string();
    alt_s3.put_bucket_policy().bucket(&granted).policy(&policy).send().await.unwrap();

    let s3 = role.session().await;

    /* the session resolves to an identity, as its request does:  the
     * role's account's row, the role having none of its own */
    assert_ne!(fs_access(&granted, "alt-obj", &principal, "r").await, FsAccess::NoIdentity,
               "the access hint resolved no identity for {principal}");

    /* granted:  the other account's object read, and an object written */
    let read = Access::ask(Class::Data, &granted, "alt-obj", &principal, "r").await;
    if let Some(got) = read.expect(s3.get_object().bucket(&granted).key("alt-obj").send().await) {
        let body = got.body.collect().await.unwrap().into_bytes();
        assert_eq!(&body[..], b"alt");
    }
    let write = Access::ask(Class::Data, &granted, "webid-obj", &principal, "c").await;
    write.expect(s3.put_object().bucket(&granted).key("webid-obj")
                 .body(ByteStream::from_static(b"webid")).send().await);

    /* no grant:  refused by S3 before the filesystem is asked */
    s3_tests_rs::assert_s3_err!(
        s3.get_object().bucket(&ungranted).key("alt-obj").send().await, 403, "AccessDenied");
    s3_tests_rs::assert_s3_err!(
        s3.put_object().bucket(&ungranted).key("webid-obj")
            .body(ByteStream::from_static(b"webid")).send().await, 403, "AccessDenied");

    /* clean up */
    for b in [&granted, &ungranted] {
        for k in ["alt-obj", "webid-obj"] {
            let _ = alt_s3.delete_object().bucket(b).key(k).send().await;
        }
        alt_s3.delete_bucket().bucket(b).send().await.unwrap();
    }
    role.delete().await;
}
