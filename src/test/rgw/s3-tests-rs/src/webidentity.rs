//! The web identity half of STS against an OIDC identity provider:
//! `rgw-vstart.sh --with-keycloak` provisions one and writes the
//! `[webidentity]` section, and the tests using this are built with
//! `has_keycloak`.

use aws_sdk_iam::Client as IamClient;

use crate::client::{build_s3_client_with_session_token, get_sts_client};
use crate::config::{get_config, S3TestConfig};

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
    iam: IamClient,
}

impl WebIdRole {
    /// Made by `iam`, the root of account `account_id`.
    pub async fn create(iam: &IamClient, account_id: &str, name: &str,
                        client_ids: &[&str], role_policy: &str) -> WebIdRole {
        let cfg = get_config();
        let (token, _) = require_webidentity(&cfg);
        let path = &cfg.iam_path_prefix;
        let aud = cfg.webidentity_aud.as_deref().unwrap_or("account");

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
        let oidc_app_id_key = format!("{}:app_id", issuer);
        let trust_policy = serde_json::json!({
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Principal": {"Federated": [&oidc_arn]},
                "Action": "sts:AssumeRoleWithWebIdentity",
                "Condition": {"StringEquals": {
                    &oidc_app_id_key: aud
                }}
            }]
        }).to_string();

        let role = iam.create_role().role_name(&role_name).path(path)
            .assume_role_policy_document(&trust_policy).send().await.unwrap();
        let role_arn = role.role().unwrap().arn().to_string();
        let role_id = role.role().unwrap().role_id().to_string();
        assert_eq!(role_arn,
                   format!("arn:aws:iam::{}:role{}{}", account_id, path, role_name));

        iam.put_role_policy().role_name(&role_name).policy_name("WebIdPolicy")
            .policy_document(role_policy).send().await.unwrap();

        WebIdRole { role_name, role_arn, role_id, oidc_arn, iam: iam.clone() }
    }

    /// An S3 client on a session of the role, from the web identity token.
    pub async fn session(&self) -> aws_sdk_s3::Client {
        let cfg = get_config();
        let (token, _) = require_webidentity(&cfg);
        let sts = get_sts_client();
        let resp = sts.assume_role_with_web_identity()
            .role_arn(&self.role_arn)
            .role_session_name("webid-test")
            .web_identity_token(&token)
            .send()
            .await
            .unwrap();
        let creds = resp.credentials().unwrap();
        build_s3_client_with_session_token(
            creds.access_key_id(), creds.secret_access_key(), creds.session_token())
    }

    pub async fn delete(self) {
        let iam = &self.iam;
        iam.delete_role_policy().role_name(&self.role_name).policy_name("WebIdPolicy")
            .send().await.unwrap();
        iam.delete_role().role_name(&self.role_name).send().await.unwrap();
        iam.delete_open_id_connect_provider()
            .open_id_connect_provider_arn(&self.oidc_arn).send().await.unwrap();
    }
}
