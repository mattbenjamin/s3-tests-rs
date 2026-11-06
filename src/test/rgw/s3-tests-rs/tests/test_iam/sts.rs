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
use s3_tests_rs::webidentity::{webidentity_aud, WebIdRole};

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
async fn assume_role_with_web_identity(name: &str, client_ids: &[&str],
                                       claim: &str, value: &str) {
    let role_policy = serde_json::json!({
        "Version": "2012-10-17",
        "Statement": {"Effect": "Allow", "Action": "s3:*", "Resource": "arn:aws:s3:::*"}
    }).to_string();
    let role = WebIdRole::create(&get_iam_root_client(), &get_config().iam_root_user_id,
                                 name, client_ids, claim, value, Some(&role_policy)).await;
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
    let aud = webidentity_aud(&get_config());
    assume_role_with_web_identity("WebIdRole", &[], "app_id", &aud).await;
}

/* Rust-only, an addition to test_sts.py:  a provider registered with
 * the token's audience, so RGW checks aud/azp against it */
#[tokio::test]
#[cfg_attr(not(feature = "has_keycloak"), ignore = "requires Keycloak (--features has_keycloak)")]
async fn test_assume_role_with_web_identity_client_id() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let aud = webidentity_aud(&get_config());
    assume_role_with_web_identity("WebIdClientIdRole", &[&aud], "app_id", &aud).await;
}

/* test_sts.py's:  the trust policy admits the token by its `sub` */
#[tokio::test]
#[cfg_attr(not(feature = "has_keycloak"), ignore = "requires Keycloak (--features has_keycloak)")]
async fn test_assume_role_with_web_identity_with_sub() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let cfg = get_config();
    let sub = cfg.webidentity_sub.clone().expect("webidentity sub not configured");
    assume_role_with_web_identity("WebIdSubRole", &[], "sub", &sub).await;
}

/* test_sts.py's:  the trust policy admits the token by its `azp` */
#[tokio::test]
#[cfg_attr(not(feature = "has_keycloak"), ignore = "requires Keycloak (--features has_keycloak)")]
async fn test_assume_role_with_web_identity_with_azp() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let cfg = get_config();
    let azp = cfg.webidentity_azp.clone().expect("webidentity azp not configured");
    assume_role_with_web_identity("WebIdAzpRole", &[], "azp", &azp).await;
}

/* test_sts.py's test_assume_role_with_web_identity_invalid_webtoken,
 * which is disabled there (inside a string), ported as an addition:  a
 * token that is not a JWT is refused.  RGW returns 403 AccessDenied, which
 * the original asserts;  AWS returns 400 InvalidIdentityToken, which the
 * original catches the exception for but does not assert. */
#[tokio::test]
#[cfg_attr(not(feature = "has_keycloak"), ignore = "requires Keycloak (--features has_keycloak)")]
async fn test_assume_role_with_web_identity_invalid_webtoken() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let aud = webidentity_aud(&get_config());
    let role_policy = serde_json::json!({
        "Version": "2012-10-17",
        "Statement": {"Effect": "Allow", "Action": "s3:*", "Resource": "arn:aws:s3:::*"}
    }).to_string();
    let role = WebIdRole::create(&get_iam_root_client(), &get_config().iam_root_user_id,
                                 "WebIdBadTokenRole", &[], "app_id", &aud, Some(&role_policy)).await;
    let r = get_sts_client().assume_role_with_web_identity()
        .role_arn(&role.role_arn)
        .role_session_name("webid-test")
        .web_identity_token("abcdef")
        .send()
        .await;
    role.delete().await;
    #[cfg(feature = "fails_on_rgw")]
    s3_tests_rs::assert_s3_err!(r, 403, "AccessDenied");
    #[cfg(not(feature = "fails_on_rgw"))]
    s3_tests_rs::assert_s3_err!(r, 400, "InvalidIdentityToken");
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
    let aud = webidentity_aud(&get_config());
    let role = WebIdRole::create(&get_iam_root_client(), &get_config().iam_root_user_id,
                                 "WebIdXAcctRole", &[], "app_id", &aud, Some(&role_policy)).await;
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

/*
 * test_sts.py's session policy tests.  A session policy limits a web
 * identity session to what both it and the role's policy allow, and a
 * bucket policy naming the session's ARN grants past it.  test_sts.py's
 * IAM user, which makes the role and the buckets, is [iam root] here, so
 * the role and its buckets share an account;  its fixed bucket names are
 * fresh ones.  The ARNs are RGW's in that account:  the role's, and the
 * session's as STS forms it (test_sts.py writes `arn:aws:iam:::` for
 * both, which RGW matches regardless of the service).
 */

fn s3_policy(effect: &str, actions: &[&str], resources: &[String]) -> String {
    serde_json::json!({
        "Version": "2012-10-17",
        "Statement": {"Effect": effect, "Action": actions, "Resource": resources}
    }).to_string()
}

fn bucket_resources(bucket: &str) -> Vec<String> {
    vec![format!("arn:aws:s3:::{bucket}"), format!("arn:aws:s3:::{bucket}/*")]
}

fn bucket_policy(effect: &str, principal: &str, bucket: &str) -> String {
    serde_json::json!({
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": effect,
            "Principal": {"AWS": principal},
            "Action": ["s3:GetObject", "s3:PutObject"],
            "Resource": bucket_resources(bucket)
        }]
    }).to_string()
}

/* the role the session policy tests make:  trusting the token by its
 * audience, with `role_policy` */
async fn session_policy_role(name: &str, role_policy: Option<&str>) -> WebIdRole {
    let aud = webidentity_aud(&get_config());
    WebIdRole::create(&get_iam_root_client(), &get_config().iam_root_user_id,
                      name, &[], "app_id", &aud, role_policy).await
}

async fn put_test_object(s3: &aws_sdk_s3::Client, bucket: &str, key: &str)
    -> Result<aws_sdk_s3::operation::put_object::PutObjectOutput,
              aws_sdk_s3::error::SdkError<aws_sdk_s3::operation::put_object::PutObjectError>>
{
    s3.put_object().bucket(bucket).key(key)
        .body(ByteStream::from_static(b"this is a test file")).send().await
}

async fn delete_test_bucket(bucket: &str) {
    let owner = get_iam_root_s3client();
    for key in ["test-1.txt", "test-2.txt"] {
        let _ = owner.delete_object().bucket(bucket).key(key).send().await;
    }
    let _ = owner.delete_bucket().bucket(bucket).send().await;
}

#[tokio::test]
#[cfg_attr(not(feature = "has_keycloak"), ignore = "requires Keycloak (--features has_keycloak)")]
async fn test_session_policy_check_on_different_buckets() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let (b1, b2) = (get_new_bucket_name(), get_new_bucket_name());
    let role = session_policy_role("WebIdSpDiffRole",
        Some(&s3_policy("Allow", &["s3:*"], &bucket_resources(&b2)))).await;
    let s3 = role.session_with_policy(Some(&s3_policy(
        "Allow", &["s3:GetObject", "s3:PutObject"], &bucket_resources(&b1)))).await;

    let c1 = s3.create_bucket().bucket(&b1).send().await;
    let c2 = s3.create_bucket().bucket(&b2).send().await;
    let put = put_test_object(&s3, &b1, "test-1.txt").await;
    delete_test_bucket(&b1).await;
    delete_test_bucket(&b2).await;
    role.delete().await;
    s3_tests_rs::assert_s3_err!(c1, 403, "AccessDenied");
    s3_tests_rs::assert_s3_err!(c2, 403, "AccessDenied");
    s3_tests_rs::assert_s3_err!(put, 404, "NoSuchBucket");
}

/* test_sts.py's five session policy tests that differ only in the two
 * policies and whether the session's PutObject is served:  the role's
 * policy, the session's, both over `bucket`, made by [iam root] */
async fn session_policy_put(name: &str, role_policy: impl Fn(&str) -> String,
                            session_policy: impl Fn(&str) -> String) -> bool {
    let bucket = get_new_bucket(Some(&get_iam_root_s3client())).await;
    let role = session_policy_role(name, Some(&role_policy(&bucket))).await;
    let s3 = role.session_with_policy(Some(&session_policy(&bucket))).await;
    let put = put_test_object(&s3, &bucket, "test-1.txt").await;
    delete_test_bucket(&bucket).await;
    role.delete().await;
    match put {
        Ok(_) => true,
        Err(e) => {
            let err = s3_tests_rs::error::extract_s3_error(&e);
            assert_eq!((err.status, err.error_code.as_str()), (403, "AccessDenied"),
                       "the session's PutObject failed otherwise than AccessDenied");
            false
        }
    }
}

#[tokio::test]
#[cfg_attr(not(feature = "has_keycloak"), ignore = "requires Keycloak (--features has_keycloak)")]
async fn test_session_policy_check_on_same_bucket() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    assert!(session_policy_put("WebIdSpSameRole",
        |_| s3_policy("Allow", &["s3:*"], &["*".to_string()]),
        |b| s3_policy("Allow", &["s3:GetObject", "s3:PutObject"], &bucket_resources(b))).await,
        "both policies allow the PutObject, and it was refused");
}

#[tokio::test]
#[cfg_attr(not(feature = "has_keycloak"), ignore = "requires Keycloak (--features has_keycloak)")]
async fn test_session_policy_check_put_obj_denial() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    assert!(!session_policy_put("WebIdSpPutDenyRole",
        |_| s3_policy("Allow", &["s3:*"], &["*".to_string()]),
        |b| s3_policy("Allow", &["s3:GetObject"], &bucket_resources(b))).await,
        "the session policy does not allow the PutObject, and it was served");
}

#[tokio::test]
#[cfg_attr(not(feature = "has_keycloak"), ignore = "requires Keycloak (--features has_keycloak)")]
async fn test_swapping_role_policy_and_session_policy() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    assert!(session_policy_put("WebIdSpSwapRole",
        |b| s3_policy("Allow", &["s3:GetObject", "s3:PutObject"], &bucket_resources(b)),
        |_| s3_policy("Allow", &["s3:*"], &["*".to_string()])).await,
        "both policies allow the PutObject, and it was refused");
}

#[tokio::test]
#[cfg_attr(not(feature = "has_keycloak"), ignore = "requires Keycloak (--features has_keycloak)")]
async fn test_session_policy_check_different_op_permissions() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    assert!(!session_policy_put("WebIdSpDiffOpRole",
        |b| s3_policy("Allow", &["s3:PutObject"], &bucket_resources(b)),
        |b| s3_policy("Allow", &["s3:GetObject"], &bucket_resources(b))).await,
        "the session policy does not allow the PutObject, and it was served");
}

#[tokio::test]
#[cfg_attr(not(feature = "has_keycloak"), ignore = "requires Keycloak (--features has_keycloak)")]
async fn test_session_policy_check_with_deny_effect() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    assert!(!session_policy_put("WebIdSpDenyRole",
        |_| s3_policy("Deny", &["s3:*"], &["*".to_string()]),
        |b| s3_policy("Allow", &["s3:PutObject"], &bucket_resources(b))).await,
        "the role's policy denies the PutObject, and it was served");
}

#[tokio::test]
#[cfg_attr(not(feature = "has_keycloak"), ignore = "requires Keycloak (--features has_keycloak)")]
async fn test_session_policy_check_with_deny_on_same_op() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    assert!(!session_policy_put("WebIdSpDenySameRole",
        |b| s3_policy("Allow", &["s3:PutObject"], &bucket_resources(b)),
        |b| s3_policy("Deny", &["s3:PutObject"], &bucket_resources(b))).await,
        "the session policy denies the PutObject, and it was served");
}

#[tokio::test]
#[cfg_attr(not(feature = "has_keycloak"), ignore = "requires Keycloak (--features has_keycloak)")]
async fn test_session_policy_bucket_policy_role_arn() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let owner = get_iam_root_s3client();
    let bucket = get_new_bucket(Some(&owner)).await;
    let role = session_policy_role("WebIdSpRoleArnRole",
        Some(&s3_policy("Allow", &["s3:*"], &["*".to_string()]))).await;
    owner.put_bucket_policy().bucket(&bucket)
        .policy(bucket_policy("Allow", &role.role_arn, &bucket)).send().await.unwrap();
    let s3 = role.session_with_policy(Some(&s3_policy(
        "Allow", &["s3:PutObject"], &bucket_resources(&bucket)))).await;

    let put = put_test_object(&s3, &bucket, "test-1.txt").await;
    let get = s3.get_object().bucket(&bucket).key("test-1.txt").send().await;
    delete_test_bucket(&bucket).await;
    role.delete().await;
    put.expect("the session policy and the role's allow the PutObject");
    /* a bucket policy naming the role grants no more than the session policy */
    s3_tests_rs::assert_s3_err!(get, 403, "AccessDenied");
}

#[tokio::test]
#[cfg_attr(not(feature = "has_keycloak"), ignore = "requires Keycloak (--features has_keycloak)")]
async fn test_session_policy_bucket_policy_session_arn() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let owner = get_iam_root_s3client();
    let bucket = get_new_bucket(Some(&owner)).await;
    let role = session_policy_role("WebIdSpSessArnRole",
        Some(&s3_policy("Allow", &["s3:*"], &["*".to_string()]))).await;
    owner.put_bucket_policy().bucket(&bucket)
        .policy(bucket_policy("Allow", &role.session_arn(), &bucket)).send().await.unwrap();
    let s3 = role.session_with_policy(Some(&s3_policy(
        "Allow", &["s3:PutObject"], &bucket_resources(&bucket)))).await;

    let put = put_test_object(&s3, &bucket, "test-1.txt").await;
    let get = s3.get_object().bucket(&bucket).key("test-1.txt").send().await;
    delete_test_bucket(&bucket).await;
    role.delete().await;
    put.expect("the session policy and the role's allow the PutObject");
    /* a bucket policy naming the session grants past the session policy */
    get.expect("the bucket policy names the session and allows the GetObject");
}

#[tokio::test]
#[cfg_attr(not(feature = "has_keycloak"), ignore = "requires Keycloak (--features has_keycloak)")]
async fn test_session_policy_copy_object() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let owner = get_iam_root_s3client();
    let bucket = get_new_bucket(Some(&owner)).await;
    let role = session_policy_role("WebIdSpCopyRole",
        Some(&s3_policy("Allow", &["s3:*"], &["*".to_string()]))).await;
    owner.put_bucket_policy().bucket(&bucket)
        .policy(bucket_policy("Allow", &role.session_arn(), &bucket)).send().await.unwrap();
    let s3 = role.session_with_policy(Some(&s3_policy(
        "Allow", &["s3:PutObject"], &bucket_resources(&bucket)))).await;

    let put = put_test_object(&s3, &bucket, "test-1.txt").await;
    let copy = s3.copy_object().bucket(&bucket).key("test-2.txt")
        .copy_source(format!("{bucket}/test-1.txt")).send().await;
    let get = s3.get_object().bucket(&bucket).key("test-2.txt").send().await;
    delete_test_bucket(&bucket).await;
    role.delete().await;
    put.expect("the session policy and the role's allow the PutObject");
    copy.expect("the bucket policy names the session and allows reading the source");
    get.expect("the bucket policy names the session and allows the GetObject");
}

#[tokio::test]
#[cfg_attr(not(feature = "has_keycloak"), ignore = "requires Keycloak (--features has_keycloak)")]
async fn test_session_policy_no_bucket_role_policy() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let bucket = get_new_bucket(Some(&get_iam_root_s3client())).await;
    let role = session_policy_role("WebIdSpNoPolicyRole", None).await;
    let s3 = role.session_with_policy(Some(&s3_policy(
        "Allow", &["s3:PutObject", "s3:GetObject"], &bucket_resources(&bucket)))).await;

    let put = put_test_object(&s3, &bucket, "test-1.txt").await;
    delete_test_bucket(&bucket).await;
    role.delete().await;
    /* a session policy grants nothing the role's policy does not */
    s3_tests_rs::assert_s3_err!(put, 403, "AccessDenied");
}

#[tokio::test]
#[cfg_attr(not(feature = "has_keycloak"), ignore = "requires Keycloak (--features has_keycloak)")]
async fn test_session_policy_bucket_policy_deny() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let owner = get_iam_root_s3client();
    let bucket = get_new_bucket(Some(&owner)).await;
    let role = session_policy_role("WebIdSpBpDenyRole",
        Some(&s3_policy("Allow", &["s3:*"], &["*".to_string()]))).await;
    owner.put_bucket_policy().bucket(&bucket)
        .policy(bucket_policy("Deny", &role.session_arn(), &bucket)).send().await.unwrap();
    let s3 = role.session_with_policy(Some(&s3_policy(
        "Allow", &["s3:PutObject"], &bucket_resources(&bucket)))).await;

    let put = put_test_object(&s3, &bucket, "test-1.txt").await;
    delete_test_bucket(&bucket).await;
    role.delete().await;
    s3_tests_rs::assert_s3_err!(put, 403, "AccessDenied");
}

/*
 * test_sts.py's ABAC tests:  sessions from the test user's token, which
 * carries its principal tags (Department = Engineering, Marketing), on a
 * role whose trust policy also allows sts:TagSession.  As above, the
 * buckets are [iam root]'s, the role's account's.
 */

const TAG_SESSION: &[&str] = &["sts:AssumeRoleWithWebIdentity", "sts:TagSession"];

fn request_tag_engineering(_issuer: &str) -> serde_json::Value {
    serde_json::json!({"StringEquals": {"aws:RequestTag/Department": "Engineering"}})
}

fn s3_all_when(condition: serde_json::Value) -> String {
    serde_json::json!({
        "Version": "2012-10-17",
        "Statement": {"Effect": "Allow", "Action": "s3:*", "Resource": "arn:aws:s3:::*",
                      "Condition": condition}
    }).to_string()
}

async fn abac_role(name: &str, trust: impl FnOnce(&str) -> serde_json::Value,
                   role_tags: &[(&str, &str)], role_policy: &str) -> WebIdRole {
    WebIdRole::create_with(&get_iam_root_client(), &get_config().iam_root_user_id,
                           name, &[], TAG_SESSION, trust, role_tags, Some(role_policy)).await
}

/* a bucket of [iam root]'s, tagged `tags` where there are any */
async fn tagged_bucket(tags: &[(&str, &str)]) -> String {
    let owner = get_iam_root_s3client();
    let bucket = get_new_bucket(Some(&owner)).await;
    if !tags.is_empty() {
        let mut set = aws_sdk_s3::types::Tagging::builder();
        for (k, v) in tags {
            set = set.tag_set(aws_sdk_s3::types::Tag::builder().key(*k).value(*v)
                              .build().unwrap());
        }
        owner.put_bucket_tagging().bucket(&bucket).tagging(set.build().unwrap())
            .send().await.unwrap();
    }
    bucket
}

/* the five ABAC tests whose session creates and deletes a bucket:
 * whether the CreateBucket is served */
async fn abac_create_bucket(name: &str, trust: impl FnOnce(&str) -> serde_json::Value,
                            role_policy: &str) -> bool {
    let role = abac_role(name, trust, &[], role_policy).await;
    let s3 = role.user_session().await;
    let bucket = get_new_bucket_name();
    let created = s3.create_bucket().bucket(&bucket).send().await;
    let deleted = match created {
        Ok(_) => Some(s3.delete_bucket().bucket(&bucket).send().await),
        Err(_) => None,
    };
    role.delete().await;
    match (created, deleted) {
        (Ok(_), Some(d)) => {
            d.expect("the session could not delete the bucket it created");
            true
        }
        (Err(e), _) => {
            let err = s3_tests_rs::error::extract_s3_error(&e);
            assert_eq!((err.status, err.error_code.as_str()), (403, "AccessDenied"),
                       "the session's CreateBucket failed otherwise than AccessDenied");
            false
        }
        (Ok(_), None) => unreachable!(),
    }
}

#[tokio::test]
#[cfg_attr(not(feature = "has_keycloak"), ignore = "requires Keycloak (--features has_keycloak)")]
async fn test_assume_role_with_web_identity_with_request_tag() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let all = serde_json::json!({
        "Version": "2012-10-17",
        "Statement": {"Effect": "Allow", "Action": "s3:*", "Resource": "arn:aws:s3:::*"}
    }).to_string();
    assert!(abac_create_bucket("WebIdReqTagRole", request_tag_engineering, &all).await,
            "the trust policy admits the token's tag, and the CreateBucket was refused");
}

#[tokio::test]
#[cfg_attr(not(feature = "has_keycloak"), ignore = "requires Keycloak (--features has_keycloak)")]
async fn test_assume_role_with_web_identity_with_principal_tag() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    assert!(abac_create_bucket("WebIdPrincTagRole", request_tag_engineering,
        &s3_all_when(serde_json::json!(
            {"StringEquals": {"aws:PrincipalTag/Department": "Engineering"}}))).await,
        "the session carries the principal tag, and the CreateBucket was refused");
}

#[tokio::test]
#[cfg_attr(not(feature = "has_keycloak"), ignore = "requires Keycloak (--features has_keycloak)")]
async fn test_assume_role_with_web_identity_for_all_values() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    assert!(abac_create_bucket("WebIdForAllRole", request_tag_engineering,
        &s3_all_when(serde_json::json!({"ForAllValues:StringEquals":
            {"aws:PrincipalTag/Department": ["Engineering", "Marketing"]}}))).await,
        "every principal tag value is in the policy's, and the CreateBucket was refused");
}

#[tokio::test]
#[cfg_attr(not(feature = "has_keycloak"), ignore = "requires Keycloak (--features has_keycloak)")]
async fn test_assume_role_with_web_identity_for_all_values_deny() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    /* ForAllValues:  true if every value in the request matches one in the
     * policy, and Marketing does not */
    assert!(!abac_create_bucket("WebIdForAllDenyRole", request_tag_engineering,
        &s3_all_when(serde_json::json!({"ForAllValues:StringEquals":
            {"aws:PrincipalTag/Department": ["Engineering"]}}))).await,
        "a principal tag value is not in the policy's, and the CreateBucket was served");
}

#[tokio::test]
#[cfg_attr(not(feature = "has_keycloak"), ignore = "requires Keycloak (--features has_keycloak)")]
async fn test_assume_role_with_web_identity_tag_keys_trust_policy() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    assert!(abac_create_bucket("WebIdTagKeysTrustRole",
        |_| serde_json::json!({"StringEquals": {"aws:TagKeys": "Department"}}),
        &s3_all_when(serde_json::json!({"ForAnyValue:StringEquals":
            {"aws:PrincipalTag/Department": ["Engineering"]}}))).await,
        "the trust policy admits the token's tag keys, and the CreateBucket was refused");
}

#[tokio::test]
#[cfg_attr(not(feature = "has_keycloak"), ignore = "requires Keycloak (--features has_keycloak)")]
async fn test_assume_role_with_web_identity_tag_keys_role_policy() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    assert!(abac_create_bucket("WebIdTagKeysRoleRole", request_tag_engineering,
        &s3_all_when(serde_json::json!(
            {"StringEquals": {"aws:TagKeys": ["Department"]}}))).await,
        "the role policy admits the session's tag keys, and the CreateBucket was refused");
}

fn resource_tag_engineering() -> String {
    s3_all_when(serde_json::json!(
        {"StringEquals": {"s3:ResourceTag/Department": ["Engineering"]}}))
}

fn resource_tag_is_principal_tag() -> String {
    s3_all_when(serde_json::json!(
        {"StringEquals": {"s3:ResourceTag/Department": ["${aws:PrincipalTag/Department}"]}}))
}

/* the three ABAC tests that differ only in the bucket's tags and whether
 * the session's PutObject is served */
async fn resource_tag_put(name: &str, bucket_tags: &[(&str, &str)]) -> bool {
    let bucket = tagged_bucket(bucket_tags).await;
    let role = abac_role(name, request_tag_engineering, &[], &resource_tag_engineering()).await;
    let s3 = role.user_session().await;
    let put = put_test_object(&s3, &bucket, "test-1.txt").await;
    delete_test_bucket(&bucket).await;
    role.delete().await;
    match put {
        Ok(_) => true,
        Err(e) => {
            let err = s3_tests_rs::error::extract_s3_error(&e);
            assert_eq!((err.status, err.error_code.as_str()), (403, "AccessDenied"),
                       "the session's PutObject failed otherwise than AccessDenied");
            false
        }
    }
}

#[tokio::test]
#[cfg_attr(not(feature = "has_keycloak"), ignore = "requires Keycloak (--features has_keycloak)")]
async fn test_assume_role_with_web_identity_resource_tag() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    assert!(resource_tag_put("WebIdResTagRole",
        &[("Department", "Engineering"), ("Department", "Marketing")]).await,
        "the bucket carries the tag the role policy requires, and the PutObject was refused");
}

#[tokio::test]
#[cfg_attr(not(feature = "has_keycloak"), ignore = "requires Keycloak (--features has_keycloak)")]
async fn test_assume_role_with_web_identity_resource_tag_deny() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    assert!(!resource_tag_put("WebIdResTagDenyRole", &[]).await,
        "the bucket has no tags, and the PutObject was served");
}

#[tokio::test]
#[cfg_attr(not(feature = "has_keycloak"), ignore = "requires Keycloak (--features has_keycloak)")]
async fn test_assume_role_with_web_identity_wrong_resource_tag_deny() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    assert!(!resource_tag_put("WebIdWrongResTagRole",
        &[("Department", "WrongResourcetag")]).await,
        "the bucket's tag is not the one the role policy requires, and the PutObject was served");
}

#[tokio::test]
#[cfg_attr(not(feature = "has_keycloak"), ignore = "requires Keycloak (--features has_keycloak)")]
async fn test_assume_role_with_web_identity_resource_tag_princ_tag() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let bucket = tagged_bucket(&[("Department", "Engineering")]).await;
    let role = abac_role("WebIdResPrincTagRole", request_tag_engineering, &[],
                         &resource_tag_is_principal_tag()).await;
    let s3 = role.user_session().await;
    let put = s3.put_object().bucket(&bucket).key("test-1.txt")
        .body(ByteStream::from_static(b"this is a test file"))
        .tagging("Department=Engineering&Department=Marketing").send().await;
    let get = s3.get_object().bucket(&bucket).key("test-1.txt").send().await;
    delete_test_bucket(&bucket).await;
    role.delete().await;
    put.expect("the bucket's tag is the session's principal tag, and the PutObject was refused");
    get.expect("the bucket's tag is the session's principal tag, and the GetObject was refused");
}

#[tokio::test]
#[cfg_attr(not(feature = "has_keycloak"), ignore = "requires Keycloak (--features has_keycloak)")]
async fn test_assume_role_with_web_identity_resource_tag_copy_obj() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let bucket = tagged_bucket(&[("Department", "Engineering")]).await;
    let copy_bucket = tagged_bucket(&[("Department", "Engineering")]).await;
    let role = abac_role("WebIdResCopyRole", request_tag_engineering, &[],
                         &resource_tag_is_principal_tag()).await;
    let s3 = role.user_session().await;
    let put = s3.put_object().bucket(&bucket).key("test-1.txt")
        .body(ByteStream::from_static(b"this is a test file"))
        .tagging("Department=Engineering").send().await;
    /* to the same bucket, and to another */
    let copy_same = s3.copy_object().bucket(&bucket).key("test-2.txt")
        .copy_source(format!("{bucket}/test-1.txt")).send().await;
    let get_same = s3.get_object().bucket(&bucket).key("test-2.txt").send().await;
    let copy_other = s3.copy_object().bucket(&copy_bucket).key("test-1.txt")
        .copy_source(format!("{bucket}/test-1.txt")).send().await;
    let get_other = s3.get_object().bucket(&copy_bucket).key("test-1.txt").send().await;
    delete_test_bucket(&bucket).await;
    delete_test_bucket(&copy_bucket).await;
    role.delete().await;
    put.expect("the PutObject was refused");
    copy_same.expect("the copy within the bucket was refused");
    get_same.expect("the copy within the bucket could not be read");
    copy_other.expect("the copy to the other bucket was refused");
    get_other.expect("the copy to the other bucket could not be read");
}

#[tokio::test]
#[cfg_attr(not(feature = "has_keycloak"), ignore = "requires Keycloak (--features has_keycloak)")]
async fn test_assume_role_with_web_identity_role_resource_tag() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let bucket = tagged_bucket(&[("Department", "Engineering"), ("Department", "Marketing")]).await;
    /* iam:ResourceTag is the role's own tag:  the role may be assumed only
     * while it carries the one the trust policy names */
    let role = abac_role("WebIdRoleResTagRole",
        |_| serde_json::json!({"StringEquals": {"iam:ResourceTag/Department": "Engineering"}}),
        &[("Department", "Engineering"), ("Department", "Marketing")],
        &resource_tag_engineering()).await;
    let s3 = role.user_session().await;
    let put = put_test_object(&s3, &bucket, "test-1.txt").await;
    delete_test_bucket(&bucket).await;
    role.delete().await;
    put.expect("the bucket carries the tag the role policy requires, and the PutObject was refused");
}
