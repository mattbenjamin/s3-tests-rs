//! Identity:  IAM, STS and accounts.
//!
//! Translated from the vendored Python s3-tests suite in the Ceph tree:
//!
//!   s3tests/functional/test_iam.py  -> most of this module;  its
//!                                      cross-account tests in cross_account.rs
//!   s3tests/functional/test_sts.py  -> sts.rs:  the web identity tests,
//!                                      with `has_keycloak`
//!
//! Python names are preserved for the translated tests.  Much of this
//! module has no Python counterpart:  `test_iam.py` was only partly
//! ported and the account tests are new.

mod cross_account;
mod group;
mod oidc;
mod role;
mod sts;
mod user;
mod user_policy;
