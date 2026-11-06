//! The web identity half of STS against an OIDC identity provider:
//! `rgw-vstart.sh --with-keycloak` provisions one and writes the
//! `[webidentity]` section, and the tests using this are built with
//! `has_keycloak`.

use aws_sdk_iam::Client as IamClient;

use crate::client::{build_s3_client_with_session_token, get_sts_client};
use crate::config::{get_config, S3TestConfig};

/// The token's audience, `[webidentity] aud`.
pub fn webidentity_aud(cfg: &S3TestConfig) -> String {
    cfg.webidentity_aud.as_deref().unwrap_or("account").to_string()
}

/// The token and realm, failing where `[webidentity]` is missing:  a test
/// built with `has_keycloak` that has no IdP has not run.
pub fn require_webidentity(cfg: &S3TestConfig) -> (String, String) {
    let token = cfg.webidentity_token.as_ref()
        .expect("webidentity not configured — run keycloak-vstart.sh first");
    let realm = cfg.webidentity_realm.as_ref()
        .expect("webidentity realm not configured");
    (token.clone(), realm.clone())
}

/* The issuer the token names:  the provider RGW looks up, and the
 * prefix of the trust policy's condition keys.  Keycloak 24 issues
 * http://<host>/realms/<realm>, 11 (teuthology's) .../auth/realms/<realm>. */
pub fn webidentity_issuer(token: &str) -> String {
    use base64::Engine;
    let payload = token.split('.').nth(1).expect("token is not a JWT");
    let claims = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(payload.trim_end_matches('='))
        .expect("token payload is not base64url");
    let claims: serde_json::Value = serde_json::from_slice(&claims)
        .expect("token payload is not JSON");
    claims["iss"].as_str().expect("token has no iss").to_string()
}

/// An OIDC provider for the token's issuer in an account, and a role
/// there that trusts it, with a role policy.
pub struct WebIdRole {
    pub role_name: String,
    pub role_arn: String,
    pub role_id: String,
    oidc_arn: String,
    has_policy: bool,
    iam: IamClient,
}

/// The session name every session on a [`WebIdRole`] is given.
pub const SESSION_NAME: &str = "webid-test";

impl WebIdRole {
    /// Made by `iam`, the root of account `account_id`.  The trust policy
    /// admits a token whose `claim` (`app_id`, the audience, or `sub`,
    /// `azp`) is `value`;  `role_policy` is the role's own, or none.
    pub async fn create(iam: &IamClient, account_id: &str, name: &str,
                        client_ids: &[&str], claim: &str, value: &str,
                        role_policy: Option<&str>) -> WebIdRole {
        WebIdRole::create_with(iam, account_id, name, client_ids,
            &["sts:AssumeRoleWithWebIdentity"],
            |issuer| serde_json::json!({"StringEquals": {format!("{issuer}:{claim}"): value}}),
            &[], role_policy).await
    }

    /// The same, the trust policy allowing `actions` under the condition
    /// `condition` returns for the provider's issuer (`host/realms/...`,
    /// the prefix of its claims' condition keys), and the role tagged
    /// `role_tags`.
    pub async fn create_with(iam: &IamClient, account_id: &str, name: &str,
                             client_ids: &[&str], actions: &[&str],
                             condition: impl FnOnce(&str) -> serde_json::Value,
                             role_tags: &[(&str, &str)],
                             role_policy: Option<&str>) -> WebIdRole {
        let cfg = get_config();
        let (token, _) = require_webidentity(&cfg);
        let path = &cfg.iam_path_prefix;

        let role_name = format!("{}{}", cfg.iam_name_prefix, name);
        let thumbprint = cfg.webidentity_thumbprint.as_deref().unwrap_or("");
        let oidc_url = webidentity_issuer(&token);
        let issuer = oidc_url.trim_start_matches("https://").trim_start_matches("http://");

        /* clean up stale resources from prior interrupted runs */
        let _ = iam.delete_role_policy().role_name(&role_name)
            .policy_name("WebIdPolicy").send().await;
        let _ = iam.delete_role().role_name(&role_name).send().await;
        if let Ok(providers) = iam.list_open_id_connect_providers().send().await {
            for p in providers.open_id_connect_provider_list() {
                if let Some(arn) = p.arn() {
                    if arn.ends_with(issuer) {
                        let _ = iam.delete_open_id_connect_provider()
                            .open_id_connect_provider_arn(arn).send().await;
                    }
                }
            }
        }

        /* create OIDC provider in RGW */
        let mut create = iam.create_open_id_connect_provider()
            .url(&oidc_url)
            .thumbprint_list(thumbprint);
        for id in client_ids {
            create = create.client_id_list(*id);
        }
        let oidc_resp = create.send().await.unwrap();
        let oidc_arn = oidc_resp.open_id_connect_provider_arn().unwrap().to_string();

        /* create role trusting the OIDC provider */
        let trust_policy = serde_json::json!({
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Principal": {"Federated": [&oidc_arn]},
                "Action": actions,
                "Condition": condition(issuer)
            }]
        }).to_string();

        let mut create_role = iam.create_role().role_name(&role_name).path(path)
            .assume_role_policy_document(&trust_policy);
        for (k, v) in role_tags {
            create_role = create_role.tags(aws_sdk_iam::types::Tag::builder()
                .key(*k).value(*v).build().unwrap());
        }
        let role = create_role.send().await.unwrap();
        let role_arn = role.role().unwrap().arn().to_string();
        let role_id = role.role().unwrap().role_id().to_string();
        assert_eq!(role_arn,
                   format!("arn:aws:iam::{}:role{}{}", account_id, path, role_name));

        if let Some(policy) = role_policy {
            iam.put_role_policy().role_name(&role_name).policy_name("WebIdPolicy")
                .policy_document(policy).send().await.unwrap();
        }

        WebIdRole { role_name, role_arn, role_id, oidc_arn,
                    has_policy: role_policy.is_some(), iam: iam.clone() }
    }

    /// The ARN of a session on the role, as STS forms it from the role's:
    /// `arn:aws:sts::<account>:assumed-role<path><name>/<session name>`.
    pub fn session_arn(&self) -> String {
        format!("{}/{}", self.role_arn.replacen("arn:aws:iam::", "arn:aws:sts::", 1)
                         .replacen(":role/", ":assumed-role/", 1), SESSION_NAME)
    }

    /// An S3 client on a session of the role, from the web identity token.
    pub async fn session(&self) -> aws_sdk_s3::Client {
        self.session_with_policy(None).await
    }

    /// The same, the session limited by `policy`, a session policy.
    pub async fn session_with_policy(&self, policy: Option<&str>) -> aws_sdk_s3::Client {
        let (token, _) = require_webidentity(&get_config());
        self.session_as(&token, policy).await
    }

    /// A session from `user_token`, the test user's token, which carries
    /// its principal tags.
    pub async fn user_session(&self) -> aws_sdk_s3::Client {
        let token = get_config().webidentity_user_token.clone()
            .expect("webidentity user_token not configured");
        self.session_as(&token, None).await
    }

    async fn session_as(&self, token: &str, policy: Option<&str>) -> aws_sdk_s3::Client {
        let sts = get_sts_client();
        let resp = sts.assume_role_with_web_identity()
            .role_arn(&self.role_arn)
            .role_session_name(SESSION_NAME)
            .web_identity_token(token)
            .set_policy(policy.map(str::to_string))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.assumed_role_user().map(|u| u.arn()), Some(self.session_arn().as_str()),
                   "the session's ARN is not the one its role's names");
        let creds = resp.credentials().unwrap();
        build_s3_client_with_session_token(
            creds.access_key_id(), creds.secret_access_key(), creds.session_token())
    }

    pub async fn delete(self) {
        let iam = &self.iam;
        if self.has_policy {
            iam.delete_role_policy().role_name(&self.role_name).policy_name("WebIdPolicy")
                .send().await.unwrap();
        }
        iam.delete_role().role_name(&self.role_name).send().await.unwrap();
        iam.delete_open_id_connect_provider()
            .open_id_connect_provider_arn(&self.oidc_arn).send().await.unwrap();
    }
}
