//! test_iam.py's cross-account tests:  a principal of [iam alt root]'s
//! account lists a bucket of [iam root]'s, refused until both an identity
//! grant in its own account and a resource grant on the bucket -- a
//! bucket policy or ACL -- allow it, or, for the account roots, until the
//! resource grant alone does.  The last is a session on a role in
//! [iam root]'s account listing a bucket of [iam alt root]'s.
//!
//! test_iam.py retries each step until the IAM change takes effect;  one
//! gateway applies it at once, so each step is asked once.  Its
//! `get_iam_alt_root_account_id()` is the alt account's id from
//! `s3tests.conf`;  here it is read from the alt root's own ARN.

use aws_sdk_iam::Client as IamClient;
use s3_tests_rs::client::{
    build_s3_client, build_s3_client_with_session_token, build_sts_client,
    get_iam_alt_root_client, get_iam_alt_root_s3client, get_iam_root_client,
    get_iam_root_s3client,
};
use s3_tests_rs::config::get_config;
use s3_tests_rs::fixtures::{get_new_bucket, get_new_bucket_name};

/// A user of [iam alt root]'s account, with an access key.
struct AltUser {
    name: String,
    arn: String,
    user_id: String,
    access_key: String,
    secret_key: String,
    has_policy: bool,
}

impl AltUser {
    async fn create(tag: &str) -> AltUser {
        let iam = get_iam_alt_root_client();
        let cfg = get_config();
        let name = format!("{}AltUser{}", cfg.iam_name_prefix, tag);
        let _ = iam.delete_user_policy().user_name(&name).policy_name("AllowStar").send().await;
        if let Ok(keys) = iam.list_access_keys().user_name(&name).send().await {
            for k in keys.access_key_metadata() {
                if let Some(id) = k.access_key_id() {
                    let _ = iam.delete_access_key().user_name(&name).access_key_id(id)
                        .send().await;
                }
            }
        }
        let _ = iam.delete_user().user_name(&name).send().await;

        let user = iam.create_user().user_name(&name).path(&cfg.iam_path_prefix)
            .send().await.unwrap();
        let user = user.user().unwrap();
        let (arn, user_id) = (user.arn().to_string(), user.user_id().to_string());
        let key = iam.create_access_key().user_name(&name).send().await.unwrap();
        let key = key.access_key().unwrap();
        AltUser { name, arn, user_id,
                  access_key: key.access_key_id().to_string(),
                  secret_key: key.secret_access_key().to_string(),
                  has_policy: false }
    }

    /// The account's ARN, as its root's:  the user's with `:root` for the
    /// user's path and name.
    fn account_arn(&self) -> String {
        let cfg = get_config();
        self.arn.replace(&format!(":user{}{}", cfg.iam_path_prefix, self.name), ":root")
    }

    fn s3(&self) -> aws_sdk_s3::Client {
        build_s3_client(&self.access_key, &self.secret_key)
    }

    /// The user policy that allows S3 everywhere.
    async fn allow_star(&mut self) {
        let policy = serde_json::json!({
            "Version": "2012-10-17",
            "Statement": [{"Effect": "Allow", "Action": "s3:*", "Resource": "*"}]
        }).to_string();
        get_iam_alt_root_client().put_user_policy().user_name(&self.name)
            .policy_name("AllowStar").policy_document(policy).send().await.unwrap();
        self.has_policy = true;
    }

    async fn delete(self) {
        let iam = get_iam_alt_root_client();
        if self.has_policy {
            let _ = iam.delete_user_policy().user_name(&self.name).policy_name("AllowStar")
                .send().await;
        }
        let _ = iam.delete_access_key().user_name(&self.name).access_key_id(&self.access_key)
            .send().await;
        let _ = iam.delete_user().user_name(&self.name).send().await;
    }
}

/// An account's id, from its root's ARN, `arn:aws:iam::<account>:root`.
async fn account_id_of(root: &IamClient) -> String {
    let user = root.get_user().send().await.unwrap();
    user.user().unwrap().arn().split(':').nth(4).unwrap().to_string()
}

fn list_bucket_policy(principal: &str, bucket: &str) -> String {
    serde_json::json!({
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Principal": {"AWS": principal},
            "Action": "s3:ListBucket",
            "Resource": format!("arn:aws:s3:::{bucket}")
        }]
    }).to_string()
}

/// The outcome of a ListObjects:  served, or refused with AccessDenied.
async fn lists(s3: &aws_sdk_s3::Client, bucket: &str) -> bool {
    match s3.list_objects().bucket(bucket).send().await {
        Ok(_) => true,
        Err(e) => {
            let err = s3_tests_rs::error::extract_s3_error(&e);
            assert_eq!((err.status, err.error_code.as_str()), (403, "AccessDenied"),
                       "the ListObjects failed otherwise than AccessDenied");
            false
        }
    }
}

/* _test_cross_account_user_bucket_policy:  the user policy, then the
 * bucket policy for `principal` */
async fn user_bucket_policy(mut user: AltUser, principal: &str) {
    user.allow_star().await;
    let roots3 = get_iam_root_s3client();
    let bucket = get_new_bucket(Some(&roots3)).await;
    let before = lists(&user.s3(), &bucket).await;
    roots3.put_bucket_policy().bucket(&bucket).policy(list_bucket_policy(principal, &bucket))
        .send().await.unwrap();
    let after = lists(&user.s3(), &bucket).await;
    let _ = roots3.delete_bucket().bucket(&bucket).send().await;
    user.delete().await;
    assert!(!before, "the user listed the other account's bucket on its own policy alone");
    assert!(after, "the bucket policy and the user's allow the listing, and it was refused");
}

/* _test_cross_account_bucket_user_policy:  the bucket policy for
 * `principal`, then the user policy */
async fn bucket_user_policy(mut user: AltUser, principal: &str) {
    let roots3 = get_iam_root_s3client();
    let bucket = get_new_bucket(Some(&roots3)).await;
    roots3.put_bucket_policy().bucket(&bucket).policy(list_bucket_policy(principal, &bucket))
        .send().await.unwrap();
    let before = lists(&user.s3(), &bucket).await;
    user.allow_star().await;
    let after = lists(&user.s3(), &bucket).await;
    let _ = roots3.delete_bucket().bucket(&bucket).send().await;
    user.delete().await;
    assert!(!before, "the user listed the other account's bucket with no policy of its own");
    assert!(after, "the bucket policy and the user's allow the listing, and it was refused");
}

/* _test_cross_account_user_policy_bucket_acl:  the user policy, then a
 * bucket ACL granting READ to `grantee` */
async fn user_policy_bucket_acl(mut user: AltUser, grantee: &str) {
    user.allow_star().await;
    let roots3 = get_iam_root_s3client();
    let bucket = get_new_bucket(Some(&roots3)).await;
    let before = lists(&user.s3(), &bucket).await;
    roots3.put_bucket_acl().bucket(&bucket).grant_read(grantee).send().await.unwrap();
    let after = lists(&user.s3(), &bucket).await;
    let _ = roots3.delete_bucket().bucket(&bucket).send().await;
    user.delete().await;
    assert!(!before, "the user listed the other account's bucket on its own policy alone");
    assert!(after, "the bucket ACL and the user's policy allow the listing, and it was refused");
}

/* _test_cross_account_bucket_acl_user_policy:  a bucket ACL granting
 * READ to `grantee`, then the user policy */
async fn bucket_acl_user_policy(mut user: AltUser, grantee: &str) {
    let roots3 = get_iam_root_s3client();
    let bucket = get_new_bucket(Some(&roots3)).await;
    roots3.put_bucket_acl().bucket(&bucket).grant_read(grantee).send().await.unwrap();
    let before = lists(&user.s3(), &bucket).await;
    user.allow_star().await;
    let after = lists(&user.s3(), &bucket).await;
    let _ = roots3.delete_bucket().bucket(&bucket).send().await;
    user.delete().await;
    assert!(!before, "the user listed the other account's bucket with no policy of its own");
    assert!(after, "the bucket ACL and the user's policy allow the listing, and it was refused");
}

/* _test_cross_account_root_bucket_policy:  the alt account's root,
 * which needs no identity policy, and a bucket policy for `principal` */
async fn root_bucket_policy(principal: &str) {
    let (roots3, alts3) = (get_iam_root_s3client(), get_iam_alt_root_s3client());
    let bucket = get_new_bucket(Some(&roots3)).await;
    let before = lists(&alts3, &bucket).await;
    roots3.put_bucket_policy().bucket(&bucket).policy(list_bucket_policy(principal, &bucket))
        .send().await.unwrap();
    let after = lists(&alts3, &bucket).await;
    let _ = roots3.delete_bucket().bucket(&bucket).send().await;
    assert!(!before, "the other account's root listed the bucket with no grant");
    assert!(after, "the bucket policy allows the other account's root, and it was refused");
}

/* _test_cross_account_root_bucket_acl:  the same, with a bucket ACL
 * granting READ to `grantee` */
async fn root_bucket_acl(grantee: &str) {
    let (roots3, alts3) = (get_iam_root_s3client(), get_iam_alt_root_s3client());
    let bucket = get_new_bucket(Some(&roots3)).await;
    let before = lists(&alts3, &bucket).await;
    roots3.put_bucket_acl().bucket(&bucket).grant_read(grantee).send().await.unwrap();
    let after = lists(&alts3, &bucket).await;
    let _ = roots3.delete_bucket().bucket(&bucket).send().await;
    assert!(!before, "the other account's root listed the bucket with no grant");
    assert!(after, "the bucket ACL grants the other account's root, and it was refused");
}

#[tokio::test]
async fn test_cross_account_role_policy_allow() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let (iam_root, iam_alt_root) = (get_iam_root_client(), get_iam_alt_root_client());
    let cfg = get_config();
    let path = &cfg.iam_path_prefix;
    let role_name = format!("{}MyRole", cfg.iam_name_prefix);
    let bucket = get_new_bucket_name();

    let user = AltUser::create("RolePolicy").await;
    let s3_alt = get_iam_alt_root_s3client();
    s3_alt.create_bucket().bucket(&bucket).send().await.unwrap();

    let _ = iam_root.delete_role_policy().role_name(&role_name)
        .policy_name("AllowListBucket").send().await;
    let _ = iam_root.delete_role().role_name(&role_name).send().await;
    let trust_policy = serde_json::json!({
        "Version": "2012-10-17",
        "Statement": [{"Effect": "Allow", "Action": "sts:AssumeRole",
                       "Principal": {"AWS": &user.arn}}]
    }).to_string();
    let role = iam_root.create_role().role_name(&role_name).path(path)
        .assume_role_policy_document(&trust_policy).send().await.unwrap();
    let role_arn = role.role().unwrap().arn().to_string();

    /* add user policy to assume the role arn */
    let assume_policy = serde_json::json!({
        "Version": "2012-10-17",
        "Statement": [{"Effect": "Allow", "Action": "sts:AssumeRole", "Resource": &role_arn}]
    }).to_string();
    get_iam_alt_root_client().put_user_policy().user_name(&user.name)
        .policy_name("AllowAssumeRole").policy_document(assume_policy).send().await.unwrap();

    let sts = build_sts_client(&user.access_key, &user.secret_key);
    let resp = sts.assume_role().role_arn(&role_arn).role_session_name("MySession")
        .send().await.unwrap();
    let creds = resp.credentials().unwrap();
    let s3 = build_s3_client_with_session_token(
        creds.access_key_id(), creds.secret_access_key(), creds.session_token());

    /* no identity policy allows S3 */
    let no_policy = lists(&s3, &bucket).await;

    let policy = serde_json::json!({
        "Version": "2012-10-17",
        "Statement": [{"Effect": "Allow", "Action": "s3:ListBucket", "Resource": "*"}]
    }).to_string();
    iam_root.put_role_policy().role_name(&role_name).policy_name("AllowListBucket")
        .policy_document(policy).send().await.unwrap();
    /* no resource policy allows the main account */
    let role_policy_only = lists(&s3, &bucket).await;

    /* a bucket policy allowing s3:ListBucket for the main account's ARN */
    let main_arn = iam_root.get_user().send().await.unwrap().user().unwrap().arn().to_string();
    s3_alt.put_bucket_policy().bucket(&bucket).policy(list_bucket_policy(&main_arn, &bucket))
        .send().await.unwrap();
    let both = lists(&s3, &bucket).await;

    let _ = s3_alt.delete_bucket().bucket(&bucket).send().await;
    let _ = iam_root.delete_role_policy().role_name(&role_name)
        .policy_name("AllowListBucket").send().await;
    let _ = iam_root.delete_role().role_name(&role_name).send().await;
    let _ = get_iam_alt_root_client().delete_user_policy().user_name(&user.name)
        .policy_name("AllowAssumeRole").send().await;
    user.delete().await;
    assert!(!no_policy, "the session listed the bucket with no identity policy");
    assert!(!role_policy_only, "the session listed the other account's bucket on its role \
                                policy alone");
    assert!(both, "the bucket policy and the role's allow the listing, and it was refused");
}

#[tokio::test]
async fn test_cross_account_bucket_user_policy_allow_user_arn() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let user = AltUser::create("BUUserArn").await;
    let principal = user.arn.clone();
    bucket_user_policy(user, &principal).await;
}

#[tokio::test]
async fn test_cross_account_user_bucket_policy_allow_user_arn() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let user = AltUser::create("UBUserArn").await;
    let principal = user.arn.clone();
    user_bucket_policy(user, &principal).await;
}

#[tokio::test]
async fn test_cross_account_user_bucket_policy_allow_account_arn() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let user = AltUser::create("UBAcctArn").await;
    let principal = user.account_arn();
    user_bucket_policy(user, &principal).await;
}

#[tokio::test]
async fn test_cross_account_bucket_user_policy_allow_account_arn() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let user = AltUser::create("BUAcctArn").await;
    let principal = user.account_arn();
    bucket_user_policy(user, &principal).await;
}

#[tokio::test]
async fn test_cross_account_user_bucket_policy_allow_account_id() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let user = AltUser::create("UBAcctId").await;
    let account_id = account_id_of(&get_iam_alt_root_client()).await;
    user_bucket_policy(user, &account_id).await;
}

#[tokio::test]
async fn test_cross_account_bucket_user_policy_allow_account_id() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let user = AltUser::create("BUAcctId").await;
    let account_id = account_id_of(&get_iam_alt_root_client()).await;
    bucket_user_policy(user, &account_id).await;
}

#[tokio::test]
#[cfg_attr(feature = "fails_on_aws", ignore = "fails on aws")]
async fn test_cross_account_bucket_acl_user_policy_grant_user_id() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let user = AltUser::create("BAUserId").await;
    let grantee = format!("id={}", user.user_id);
    bucket_acl_user_policy(user, &grantee).await;
}

#[tokio::test]
#[cfg_attr(feature = "fails_on_aws", ignore = "fails on aws")]
async fn test_cross_account_user_policy_bucket_acl_grant_user_id() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let user = AltUser::create("UAUserId").await;
    let grantee = format!("id={}", user.user_id);
    user_policy_bucket_acl(user, &grantee).await;
}

#[tokio::test]
async fn test_cross_account_bucket_acl_user_policy_grant_canonical_id() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let user = AltUser::create("BACanonId").await;
    let grantee = format!("id={}", get_config().iam_alt_root_user_id);
    bucket_acl_user_policy(user, &grantee).await;
}

#[tokio::test]
async fn test_cross_account_user_policy_bucket_acl_grant_canonical_id() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let user = AltUser::create("UACanonId").await;
    let grantee = format!("id={}", get_config().iam_alt_root_user_id);
    user_policy_bucket_acl(user, &grantee).await;
}

#[tokio::test]
async fn test_cross_account_bucket_acl_user_policy_grant_account_email() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let user = AltUser::create("BAEmail").await;
    let grantee = format!("emailAddress={}", get_config().iam_alt_root_email);
    bucket_acl_user_policy(user, &grantee).await;
}

#[tokio::test]
async fn test_cross_account_user_policy_bucket_acl_grant_account_email() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let user = AltUser::create("UAEmail").await;
    let grantee = format!("emailAddress={}", get_config().iam_alt_root_email);
    user_policy_bucket_acl(user, &grantee).await;
}

#[tokio::test]
async fn test_cross_account_root_bucket_policy_allow_account_arn() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let alt_arn = get_iam_alt_root_client().get_user().send().await.unwrap()
        .user().unwrap().arn().to_string();
    root_bucket_policy(&alt_arn).await;
}

#[tokio::test]
async fn test_cross_account_root_bucket_policy_allow_account_id() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    let account_id = account_id_of(&get_iam_alt_root_client()).await;
    root_bucket_policy(&account_id).await;
}

#[tokio::test]
async fn test_cross_account_root_bucket_acl_grant_canonical_id() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    root_bucket_acl(&format!("id={}", get_config().iam_alt_root_user_id)).await;
}

#[tokio::test]
async fn test_cross_account_root_bucket_acl_grant_account_email() {
    let _guard = s3_tests_rs::fixtures::TestGuard::setup();
    root_bucket_acl(&format!("emailAddress={}", get_config().iam_alt_root_email)).await;
}
