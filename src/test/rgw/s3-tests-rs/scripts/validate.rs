#!/usr/bin/env -S cargo +nightly -Zscript
---
[package]
edition = "2021"
---
//! Run one validation of the nsfs driver:  build, the unit suite, and
//! the s3-tests-rs profiles under each gateway configuration, with one
//! sudo password for the whole run.
//!
//! This is a meta driver above the suite, not part of it.  It is a
//! `-Zscript`, outside the crate's build graph, and the suite never
//! knows it exists:  every step is a command you could run by hand,
//! and `--dry-run` prints them.  Root is never taken by the suite.
//! The privileged steps stay in `rgw-vstart.sh`, which is the single
//! authority on the fixture;  the one this program issues itself is
//! the unit binary's `setcap`, with the capability string read from
//! that script.
//!
//! Privilege is one `sudo -v` at the start, kept alive while the run
//! lasts, so the sudo calls `rgw-vstart.sh` makes go through on the
//! same tty's ticket.  Run it from your own terminal, as yourself.
//!
//!   scripts/validate.rs [--dry-run] [--no-build] [--no-unit] [--no-librgw]
//!                       [--lifecycle] [--cells a,b] [--test EXPR]
//!                       [--config FILE] [--build DIR] [--conf FILE]
//!                       [--adoption-cache DIR] [--noobaa-src DIR]
//!                       [--work-root DIR] [--parallel-log FILE]
//!
//! The paths are this machine's, so none has a default here.  Each comes
//! from its option, or else from a config file, `key = value` per line.
//! The file is `--config FILE`, else `$VALIDATE_CONF` -- as the suite
//! takes `$S3TEST_CONF` -- else the per-user
//! `$XDG_CONFIG_HOME/s3-tests-rs/validate.conf` (by default
//! `~/.config/s3-tests-rs/validate.conf`):
//!
//!   build          = /path/to/ceph/build        required
//!   conf           = /path/to/s3tests.conf      required
//!   adoption_cache = /path/to/adoption-cache    for the conversion cells
//!   noobaa_src     = /path/to/noobaa-core       for the conversion cells
//!   work_root      = /path/to/work              for the conversion cells;
//!                                                every fixture uid must be
//!                                                able to search it
//!   parallel_log   = /path/to/log               every suite's output,
//!                                                appended;  optional
//!
//! `--test` runs only the tests a nextest filter expression selects, in
//! each cell, in place of its profiles;  `--cells` picks the cells.  The
//! conversion cells -- the adoption corpus, `standup.rs conversion` --
//! run only when named:  each takes a fresh work directory, with the
//! NooBaa worktree already built in `--adoption-cache` linked into it.

use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, IsTerminal, Write};
use std::net::TcpStream;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

const TARGETS: &[&str] = &["radosgw", "radosgw-admin", "ceph-conf",
    "unittest_rgw_nsfs_driver", "ceph_test_librgw_file_write2",
    "ceph_test_librgw_file_nfsns"];
const FEATURES: &str = "fails_on_nsfs,rgw_admin";
/* the librdkafka keys test_sns's kafka-config tests set;  the cells
 * passing it take the tests' accepting case, the others the refusing */
const KAFKA_CONFIG_KEYS: &str =
    "rgw_kafka_config_allowed_keys=client.id,linger.ms,message.max.bytes,security.protocol,sasl.password";
/* vstart's fixed test user, which the librgw suites mount as */
const AWS_KEY: &str = "0555b35654ad1656d804";
const AWS_SECRET: &str = "h7GhxuBLTrlhVUyxSPUKUV8r/2EI4ngqJxD7iBdBYLhwluN30JaT3Q==";

enum Suite {
    Profile(&'static str),
    /* profile `dynamic-groups`, then its membership test again with
     * `rgwalice` taken out of `rgwfix`, putting her back after */
    DynamicGroups,
    Librgw,
    /* the notification tests with `has_kafka`, against the broker the
     * cell's `--with-kafka` started, which is stopped after them */
    Kafka,
    /* `standup.rs conversion`, which starts its own gateway;  `adopt_by`
     * is its setting of that name:  command, automatic or promotion;
     * `kafka` starts kafka-vstart.sh's broker for the `notify` steps'
     * Kafka half, and stops it after */
    Conversion { impersonate: bool, access_model: &'static str, adopt_by: &'static str,
                 kafka: bool },
}

struct Cell {
    name: &'static str,
    /* rgw_nsfs_user_identity_override:  passed to the gateway,
     * shown in every label, and checked against what the gateway says
     * it runs */
    user_identity_override: bool,
    /* empty:  the suite stands its own gateway up */
    vstart: &'static [&'static str],
    suites: &'static [Suite],
    /* run when no --cells names any */
    by_default: bool,
}

/* Adding a configuration is an edit here. */
const CELLS: &[Cell] = &[
    Cell { name: "posix-strict", user_identity_override: false,
           vstart: &["--store", "nsfs", "--with-impersonation", "--clean"],
           suites: &[Suite::Profile("baseline"),
                     Suite::Profile("access-model-switch"),
                     Suite::DynamicGroups,
                     Suite::Profile("role-identity")],
           by_default: true },
    Cell { name: "s3-native", user_identity_override: false,
           vstart: &["--store", "nsfs", "--with-impersonation",
                     "--access-model", "s3-native", "--clean"],
           suites: &[Suite::Profile("baseline"),
                     Suite::Profile("access-model-switch"),
                     Suite::DynamicGroups,
                     Suite::Profile("role-identity")],
           by_default: true },
    Cell { name: "plain", user_identity_override: false,
           vstart: &["--store", "nsfs", "--bucket-profile", "strong", "--clean",
                     "-o", KAFKA_CONFIG_KEYS],
           suites: &[Suite::Profile("baseline"), Suite::Librgw],
           by_default: true },
    /* the adoption corpus:  NooBaa writes a tree, RGW captures it before
     * and after conversion, and the captures are compared -- with RGW
     * as the gateway's own identity, and impersonating under each
     * access model */
    Cell { name: "conversion", user_identity_override: false, vstart: &[],
           suites: &[Suite::Conversion { impersonate: false, access_model: "posix-strict",
                                       adopt_by: "command", kafka: false }],
           by_default: false },
    Cell { name: "conversion-posix-strict", user_identity_override: false, vstart: &[],
           suites: &[Suite::Conversion { impersonate: true, access_model: "posix-strict",
                                       adopt_by: "command", kafka: false }],
           by_default: false },
    Cell { name: "conversion-s3-native", user_identity_override: false, vstart: &[],
           suites: &[Suite::Conversion { impersonate: true, access_model: "s3-native",
                                       adopt_by: "command", kafka: false }],
           by_default: false },
    /* the same tree adopted by the queue, not the commands:  RGW
     * restarted in automatic mode, and the import's gateway switched to
     * it by the `adoption` hint with the promotion checks;  both compare
     * as `conversion` does */
    /* the notification tests against a Kafka broker:  the one cell
     * that builds `has_kafka`.  Off by default, since the sidecar needs
     * podman */
    Cell { name: "kafka", user_identity_override: false,
           vstart: &["--store", "nsfs", "--clean", "--with-kafka", "-o", KAFKA_CONFIG_KEYS],
           suites: &[Suite::Kafka],
           by_default: false },
    /* `conversion` with a Kafka half in the `notify` steps:  a NooBaa
     * Kafka connection, carried across by the import as a topic with
     * its librdkafka settings, compared as `noobaa:rgw-notify-kafka`.
     * Off by default, since the broker needs podman */
    Cell { name: "conversion-kafka", user_identity_override: false, vstart: &[],
           suites: &[Suite::Conversion { impersonate: false, access_model: "posix-strict",
                                       adopt_by: "command", kafka: true }],
           by_default: false },
    Cell { name: "conversion-automatic", user_identity_override: false, vstart: &[],
           suites: &[Suite::Conversion { impersonate: false, access_model: "posix-strict",
                                       adopt_by: "automatic", kafka: false }],
           by_default: false },
    Cell { name: "conversion-promotion", user_identity_override: false, vstart: &[],
           suites: &[Suite::Conversion { impersonate: false, access_model: "posix-strict",
                                       adopt_by: "promotion", kafka: false }],
           by_default: false },
    /* the same six with rgw_nsfs_user_identity_override on:  an
     * account user's own row overrides its account's, as NooBaa serves
     * an IAM user from its own record */
    Cell { name: "posix-strict-override", user_identity_override: true,
           vstart: &["--store", "nsfs", "--with-impersonation", "--clean"],
           suites: &[Suite::Profile("baseline"),
                     Suite::Profile("access-model-switch"),
                     Suite::DynamicGroups,
                     Suite::Profile("role-identity")],
           by_default: true },
    Cell { name: "s3-native-override", user_identity_override: true,
           vstart: &["--store", "nsfs", "--with-impersonation",
                     "--access-model", "s3-native", "--clean"],
           suites: &[Suite::Profile("baseline"),
                     Suite::Profile("access-model-switch"),
                     Suite::DynamicGroups,
                     Suite::Profile("role-identity")],
           by_default: true },
    Cell { name: "plain-override", user_identity_override: true,
           vstart: &["--store", "nsfs", "--bucket-profile", "strong", "--clean",
                     "-o", KAFKA_CONFIG_KEYS],
           suites: &[Suite::Profile("baseline"), Suite::Librgw],
           by_default: true },
    Cell { name: "conversion-override", user_identity_override: true, vstart: &[],
           suites: &[Suite::Conversion { impersonate: false, access_model: "posix-strict",
                                       adopt_by: "command", kafka: false }],
           by_default: false },
    Cell { name: "conversion-posix-strict-override", user_identity_override: true, vstart: &[],
           suites: &[Suite::Conversion { impersonate: true, access_model: "posix-strict",
                                       adopt_by: "command", kafka: false }],
           by_default: false },
    Cell { name: "conversion-s3-native-override", user_identity_override: true, vstart: &[],
           suites: &[Suite::Conversion { impersonate: true, access_model: "s3-native",
                                       adopt_by: "command", kafka: false }],
           by_default: false },
];

struct Opts {
    dry_run: bool,
    build: bool,
    unit: bool,
    librgw: bool,
    lifecycle: bool,
    cells: Option<Vec<String>>,
    test: Option<String>,
    build_dir: PathBuf,
    /* the ceph source tree, from the build's CMakeCache.txt */
    source_dir: PathBuf,
    conf: PathBuf,
    adoption_cache: PathBuf,
    noobaa_src: PathBuf,
    /* where the conversion cells' work directories go;  under the run
     * directory when unset */
    work_root: Option<PathBuf>,
    parallel_log: Option<PathBuf>,
}

/// One command, as it is run and as it is printed.
struct Step {
    cell: String,
    name: String,
    cwd: PathBuf,
    argv: Vec<String>,
    env: Vec<(String, String)>,
    /* the suite's own log convention:  also appended there */
    parallel_log: bool,
    /* the deployment's rgw_nsfs_user_identity_override;  `None` for the
     * steps outside any deployment, the build and the unit suite */
    user_identity_override: Option<bool>,
}

/// How a step is labelled:  its deployment, with every setting that
/// distinguishes one deployment from another stated rather than implied.
fn label(cell: &str, over: Option<bool>) -> String {
    match over {
        Some(on) => format!("{cell} · user-identity-override={}", if on { "on" } else { "off" }),
        None => cell.to_string(),
    }
}

impl Step {
    fn show(&self) -> String {
        let env: String = self.env.iter()
            .map(|(k, v)| format!("{k}={} ", if k.contains("SECRET") { "…" } else { v }))
            .collect();
        let argv: Vec<String> = self.argv.iter()
            .map(|a| if a.contains(|c: char| " ()|&;<>$*?".contains(c)) {
                format!("'{a}'")
            } else {
                a.clone()
            })
            .collect();
        format!("(cd {} && {env}{})", self.cwd.display(), argv.join(" "))
    }
}

fn main() {
    let opts = parse_args();
    let suite_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent().expect("the script lives in scripts/").to_path_buf();
    let stamp = Command::new("date").arg("+%Y%m%d-%H%M%S").output()
        .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string())
        .unwrap_or_else(|_| "run".into());
    let run_dir = suite_dir.join("target/validate").join(&stamp);

    let work_dir = opts.work_root.as_ref().map(|w| w.join(&stamp))
        .unwrap_or_else(|| run_dir.clone());
    let steps = plan(&opts, &suite_dir, &run_dir, &work_dir);
    println!("validate:  {} steps, logs under {}", steps.len(), run_dir.display());
    for s in &steps {
        println!("  [{}] {}:  {}", label(&s.cell, s.user_identity_override), s.name, s.show());
    }
    if opts.dry_run {
        return;
    }

    preflight(&opts, &steps).unwrap_or_else(|e| die(&e));
    take_sudo().unwrap_or_else(|e| die(&e));
    fs::create_dir_all(&run_dir).unwrap_or_else(|e| die(&format!("{}: {e}", run_dir.display())));

    let mut results: Vec<(String, String, Res, String)> = Vec::new();
    let mut skip_cell: Option<String> = None;
    for s in &steps {
        if skip_cell.as_deref() == Some(&s.cell) {
            results.push((s.cell.clone(), s.name.clone(), Res::Fail, "not run:  the cell did not stand up".into()));
            continue;
        }
        println!("\n==> [{}] {}", label(&s.cell, s.user_identity_override), s.name);
        let (res, summary) = run(s, &run_dir, opts.parallel_log.as_deref());
        let ok = res != Res::Fail;
        println!("    {} {summary}", res.label());
        if !ok && s.name == "vstart" {
            skip_cell = Some(s.cell.clone());
        }
        if !ok && s.name == "build" {
            results.push((s.cell.clone(), s.name.clone(), res, summary));
            break;
        }
        if s.name == "vstart" && ok {
            if let Err(e) = wait_for_gateway() {
                results.push((s.cell.clone(), "gateway".into(), Res::Fail, e));
                skip_cell = Some(s.cell.clone());
                continue;
            }
            /* the label is a checked fact:  the gateway says which it runs */
            if let Some(want) = s.user_identity_override {
                if let Err(e) = check_user_identity_override(&opts.build_dir, want) {
                    println!("    FAIL  {e}");
                    results.push((s.cell.clone(), "user-identity-override".into(), Res::Fail, e));
                    skip_cell = Some(s.cell.clone());
                    continue;
                }
            }
        }
        results.push((s.cell.clone(), s.name.clone(), res, summary));
    }

    let setting = |cell: &str| steps.iter().find(|s| s.cell == cell)
        .and_then(|s| s.user_identity_override)
        .map(|on| if on { "on" } else { "off" }).unwrap_or("-");
    let mut table = String::from(
        "\ncell                              user-identity-override  step                    result\n");
    for (cell, name, res, summary) in &results {
        table += &format!("{cell:<33} {:<23} {name:<23} {} {summary}\n",
                          setting(cell), res.label());
    }
    print!("{table}");
    let _ = fs::write(run_dir.join("summary.txt"), &table);
    if results.iter().any(|r| r.2 == Res::Fail) {
        std::process::exit(1);
    }
    /* as corpus-compare does:  nothing failed, and known defects remain */
    if results.iter().any(|r| r.2 == Res::Known) {
        std::process::exit(3);
    }
}

fn plan(o: &Opts, suite_dir: &Path, run_dir: &Path, work_dir: &Path) -> Vec<Step> {
    let b = &o.build_dir;
    let step = |cell: &str, name: &str, cwd: &Path, argv: Vec<String>| Step {
        cell: cell.into(), name: name.into(), cwd: cwd.to_path_buf(), argv,
        env: Vec::new(), parallel_log: false, user_identity_override: None,
    };
    let s = |v: &[&str]| v.iter().map(|x| x.to_string()).collect::<Vec<_>>();
    let mut steps = Vec::new();

    if o.build {
        let mut argv = s(&["ninja", &format!("-j{}", nproc())]);
        argv.extend(s(TARGETS));
        steps.push(step("build", "build", b, argv));
    }
    let caps = vstart_caps(&o.source_dir);
    if o.unit {
        let unit = b.join("bin/unittest_rgw_nsfs_driver");
        let caps = caps.as_ref().unwrap_or_else(|e| die(e));
        steps.push(step("unit", "setcap", b,
            s(&["sudo", "-n", "setcap", &format!("{caps}+p"), &unit.to_string_lossy()])));
        steps.push(step("unit", "unittest", b, vec![unit.to_string_lossy().into()]));
    }

    let vstart = o.source_dir.join("src/script/rgw/rgw-vstart.sh");
    for c in CELLS {
        match &o.cells {
            Some(want) if !want.iter().any(|w| w == c.name) => continue,
            None if !c.by_default => continue,
            _ => {}
        }
        /* refused before anything starts, rather than failing inside the
         * sidecar's start */
        if c.suites.iter().any(|s| matches!(s, Suite::Kafka))
            && !Command::new("podman").arg("--version").stdout(Stdio::null())
                .stderr(Stdio::null()).status().is_ok_and(|st| st.success()) {
            die(&format!("the {} cell runs a Kafka broker in a container, and \
                          podman is not installed", c.name));
        }
        /* a conversion's gateway is started with --with-lifecycle already */
        let variants = if o.lifecycle && !c.vstart.is_empty() { &[false, true][..] }
                       else { &[false][..] };
        for lc in variants {
            let cell = if *lc { format!("{}+lifecycle", c.name) } else { c.name.into() };
            let mut argv = vec![vstart.to_string_lossy().into_owned()];
            argv.extend(s(c.vstart));
            if *lc {
                argv.push("--with-lifecycle".into());
            }
            if c.user_identity_override {
                argv.push("--user-identity-override".into());
            }
            let first = steps.len();
            if !c.vstart.is_empty() {
                steps.push(step(&cell, "vstart", b, argv));
            }
            if let Some(expr) = &o.test {
                let mut st = step(&cell, "test", suite_dir, s(&["cargo", "nextest", "run",
                    "--no-fail-fast", "--features", FEATURES, "-E", expr]));
                st.env.push(("S3TEST_CONF".into(), o.conf.to_string_lossy().into()));
                st.parallel_log = true;
                steps.push(st);
            }
            for suite in c.suites {
                match suite {
                    Suite::Profile(_) | Suite::Conversion { .. } | Suite::DynamicGroups
                    | Suite::Kafka
                        if o.test.is_some() => {}
                    Suite::Librgw if !o.librgw => {}
                    Suite::Conversion { impersonate, access_model, adopt_by, kafka } => {
                        /* conversion raises CAP_DAC_OVERRIDE and CAP_CHOWN
                         * whether or not it impersonates, and an unimpersonated
                         * vstart sets no caps */
                        let caps = caps.as_ref().unwrap_or_else(|e| die(e));
                        steps.push(step(&cell, "setcap", b, s(&["sudo", "-n", "setcap",
                            &format!("{caps}+p"), &b.join("bin/radosgw").to_string_lossy()])));
                        /* and the unit binary, which `store-tests` runs against
                         * NooBaa's store:  root's, under impersonation */
                        steps.push(step(&cell, "setcap-unit", b, s(&["sudo", "-n", "setcap",
                            &format!("{caps}+p"),
                            &b.join("bin/unittest_rgw_nsfs_driver").to_string_lossy()])));
                        let work = work_dir.join(&cell).join("work");
                        steps.push(step(&cell, "work", b, s(&["sh", "-c",
                            "mkdir -p \"$1\" && for w in \"$2\"/noobaa-*; do ln -s \"$w\" \"$1\"/; done",
                            "sh", &work.to_string_lossy(), &o.adoption_cache.to_string_lossy()])));
                        /* from its own directory, which is where it finds its
                         * conf, patches, locks and expected differences */
                        let adoption = suite_dir.join("scripts/adoption");
                        let mut st = step(&cell, "conversion", &adoption,
                            s(&["./standup.rs", "conversion"]));
                        st.env.push(("ADOPTION_WORK".into(), work.to_string_lossy().into()));
                        st.env.push(("ADOPTION_CEPH_BUILD".into(), b.to_string_lossy().into()));
                        st.env.push(("ADOPTION_NOOBAA_SRC".into(), o.noobaa_src.to_string_lossy().into()));
                        st.env.push(("ADOPTION_IMPERSONATE".into(), impersonate.to_string()));
                        st.env.push(("ADOPTION_ACCESS_MODEL".into(), access_model.to_string()));
                        st.env.push(("ADOPTION_ADOPT_BY".into(), adopt_by.to_string()));
                        st.env.push(("ADOPTION_USER_IDENTITY_OVERRIDE".into(),
                                     c.user_identity_override.to_string()));
                        let broker = o.source_dir.join("src/script/rgw/kafka-vstart.sh")
                            .to_string_lossy().into_owned();
                        if *kafka {
                            st.env.push(("ADOPTION_KAFKA_BROKER".into(), "127.0.0.1:9092".into()));
                            steps.push(step(&cell, "kafka-start", b, vec![broker.clone()]));
                        }
                        steps.push(st);
                        if *kafka {
                            /* run whatever the stand-up returned */
                            steps.push(step(&cell, "kafka-stop", b, vec![broker, "--stop".into()]));
                        }
                    }
                    Suite::Profile(p) => {
                        let mut st = step(&cell, p, suite_dir, s(&["cargo", "nextest", "run",
                            "-P", p, "--no-fail-fast", "--features", FEATURES]));
                        st.env.push(("S3TEST_CONF".into(), o.conf.to_string_lossy().into()));
                        st.parallel_log = true;
                        steps.push(st);
                    }
                    Suite::DynamicGroups => {
                        for (name, argv) in [
                            ("dynamic-groups", s(&["cargo", "nextest", "run", "-P",
                                "dynamic-groups", "--no-fail-fast", "--features", FEATURES])),
                            ("gpasswd-del", s(&["sudo", "-n", "gpasswd", "-d", "rgwalice", "rgwfix"])),
                            ("dynamic-groups-del", s(&["cargo", "nextest", "run", "-P",
                                "dynamic-groups-membership", "--no-fail-fast", "--features",
                                FEATURES])),
                            /* run whatever the step before returned:  a failing
                             * step does not stop the cell, and rgw-vstart.sh
                             * puts the membership back too */
                            ("gpasswd-add", s(&["sudo", "-n", "gpasswd", "-a", "rgwalice", "rgwfix"])),
                        ] {
                            let mut st = step(&cell, name, suite_dir, argv);
                            if name.starts_with("dynamic-groups") {
                                st.env.push(("S3TEST_CONF".into(), o.conf.to_string_lossy().into()));
                                st.parallel_log = true;
                            }
                            steps.push(st);
                        }
                    }
                    Suite::Kafka => {
                        let mut st = step(&cell, "kafka", suite_dir, s(&["cargo", "nextest", "run",
                            "--no-fail-fast", "--features", &format!("{FEATURES},has_kafka"),
                            "-E", "binary(test_sns)"]));
                        st.env.push(("S3TEST_CONF".into(), o.conf.to_string_lossy().into()));
                        st.parallel_log = true;
                        steps.push(st);
                        /* run whatever the tests returned:  the broker is a
                         * container, and the next cell's --clean only stops
                         * one rgw-vstart.sh started */
                        steps.push(step(&cell, "kafka-stop", b, vec![
                            o.source_dir.join("src/script/rgw/kafka-vstart.sh")
                                .to_string_lossy().into_owned(), "--stop".into()]));
                    }
                    Suite::Librgw => {
                        for (bin, extra) in [("ceph_test_librgw_file_write2", &["--create"][..]),
                                             ("ceph_test_librgw_file_nfsns", &["--create", "--hier1"][..])] {
                            let log = run_dir.join(&cell).join(format!("{bin}.rgw.log"));
                            let mut argv = vec![b.join("bin").join(bin).to_string_lossy().into_owned()];
                            argv.extend(s(extra));
                            argv.extend(s(&["--userid", "testid"]));
                            argv.push(format!("-c {} --log-file={} --debug-rgw=10",
                                              b.join("ceph.conf").display(), log.display()));
                            let mut st = step(&cell, bin.trim_start_matches("ceph_test_librgw_file_"), b, argv);
                            st.env.push(("AWS_ACCESS_KEY_ID".into(), AWS_KEY.into()));
                            st.env.push(("AWS_SECRET_ACCESS_KEY".into(), AWS_SECRET.into()));
                            steps.push(st);
                        }
                    }
                }
            }
            for st in &mut steps[first..] {
                st.user_identity_override = Some(c.user_identity_override);
            }
        }
    }
    steps
}

/// A step's result.  `Known` is the conversion step's exit 3:  every
/// difference is accounted for, and known defects remain.
#[derive(Clone, Copy, PartialEq)]
enum Res {
    Ok,
    Known,
    Fail,
}

impl Res {
    fn label(self) -> &'static str {
        match self {
            Res::Ok => "ok   ",
            Res::Known => "KNOWN",
            Res::Fail => "FAIL ",
        }
    }
}

/// Run a step with its output in its own log, and the summary line the
/// suite prints:  nextest's `Summary`, or gtest's PASSED/FAILED counts.
fn run(s: &Step, run_dir: &Path, parallel_log: Option<&Path>) -> (Res, String) {
    let dir = run_dir.join(&s.cell);
    let _ = fs::create_dir_all(&dir);
    let mut log = match File::create(dir.join(format!("{}.log", s.name))) {
        Ok(f) => f,
        Err(e) => return (Res::Fail, format!("cannot create its log: {e}")),
    };
    let mut parallel = parallel_log.filter(|_| s.parallel_log).and_then(|p| {
        OpenOptions::new().create(true).append(true).open(p).ok()
    });
    let _ = writeln!(log, "# {}", s.show());

    /* stderr into the same pipe, so the log holds both in order */
    let mut cmd = Command::new("sh");
    cmd.arg("-c").arg("exec \"$@\" 2>&1").arg("sh").args(&s.argv)
        .current_dir(&s.cwd).stdin(Stdio::null()).stdout(Stdio::piped());
    for (k, v) in &s.env {
        cmd.env(k, v);
    }
    let mut child = match cmd.spawn() {
        Ok(c) => c,
        Err(e) => return (Res::Fail, format!("cannot start {}: {e}", s.argv[0])),
    };
    let mut summary = Vec::new();
    for line in BufReader::new(child.stdout.take().unwrap()).lines().map_while(Result::ok) {
        let _ = writeln!(log, "{line}");
        if let Some(p) = parallel.as_mut() {
            let _ = writeln!(p, "{line}");
        }
        let t = line.trim_start();
        let counted = t.contains(" tests") || t.contains(" test.");
        if t.starts_with("Summary [") || t.starts_with("[  PASSED  ] ")
            || ((t.starts_with("[  FAILED  ] ") || t.starts_with("[  SKIPPED ] "))
                && counted && !t.contains(" (")) {
            summary.push(t.to_string());
        }
        /* a conversion's result in counts:  corpus-compare's two pair
         * lines, and how the profile ended */
        if let Some(pair) = t.strip_prefix("== ").filter(|p| p.starts_with("adoption (")
                                                          || p.starts_with("conversion (")) {
            summary.push(pair.to_string());
        }
        if t.starts_with("==> conversion:  every step passed")
            || t.contains("the conversion profile stopped at")
            || t.contains("the corpus failed against RGW") {
            summary.push(t.trim_start_matches("==> conversion:  ").to_string());
        }
        if t.starts_with("FAIL [") || t.starts_with("error:") || t.starts_with("ERROR:") {
            println!("    {t}");
        }
    }
    let res = match child.wait().ok().and_then(|st| st.code()) {
        Some(0) => Res::Ok,
        /* standup.rs exits 3 when every difference is accounted for and
         * known defects remain */
        Some(3) if s.name == "conversion" => Res::Known,
        _ => Res::Fail,
    };
    (res, summary.join(";  "))
}

fn preflight(o: &Opts, steps: &[Step]) -> Result<(), String> {
    let status = fs::read_to_string("/proc/self/status").map_err(|e| e.to_string())?;
    let euid = status.lines().find(|l| l.starts_with("Uid:"))
        .and_then(|l| l.split_whitespace().nth(2)).unwrap_or("0");
    if euid == "0" {
        return Err("run as yourself, not root:  rgw-vstart.sh --with-impersonation refuses root, and sudo is taken where it is needed".into());
    }
    if Command::new("pgrep").args(["-x", "ninja"]).stdout(Stdio::null()).status()
        .map(|s| s.success()).unwrap_or(false) {
        return Err("a ninja is already running;  one build at a time".into());
    }
    if !o.build_dir.join("bin").is_dir() {
        return Err(format!("{} has no bin/;  is it the ceph build directory?", o.build_dir.display()));
    }
    if !o.conf.is_file() {
        return Err(format!("{} does not exist", o.conf.display()));
    }
    /* setcap on a nosuid mount succeeds, and the kernel then ignores the
     * capabilities:  every impersonated test would fail as a denial */
    let bin = o.build_dir.join("bin");
    if let Ok(out) = Command::new("findmnt").args(["-no", "OPTIONS", "-T"]).arg(&bin).output() {
        if String::from_utf8_lossy(&out.stdout).split(',').any(|f| f.trim() == "nosuid") {
            return Err(format!("{} is on a nosuid mount, where file capabilities are \
                                ignored;  build on a mount without nosuid", bin.display()));
        }
    }
    /* an impersonated conversion writes its tree as the fixture uids */
    for s in steps.iter().filter(|s| s.name == "conversion"
                                 && s.env.iter().any(|(k, v)| k == "ADOPTION_IMPERSONATE" && v == "true")) {
        let work = s.env.iter().find(|(k, _)| k == "ADOPTION_WORK").map(|(_, v)| PathBuf::from(v));
        if let Some(blocked) = work.as_deref().and_then(unsearchable_ancestor) {
            return Err(format!("the fixture uids cannot search {}, so cell {} cannot \
                                stand NooBaa up in {};  set work_root (or --work-root) to \
                                a directory every user can reach", blocked.display(),
                               s.cell, work.unwrap().display()));
        }
    }
    if !std::io::stdin().is_terminal() {
        return Err("needs a terminal:  the sudo ticket is per tty, and the password is asked once, here".into());
    }
    Ok(())
}

/// The first existing ancestor of `p` that a fixture uid cannot search.
/// `rgw-vstart.sh --with-impersonation` serves identities as uids
/// 70101-70110, which own none of these directories and are in none of
/// their groups, so what lets them through is the other-execute bit.
/// `None` when every ancestor has it.
fn unsearchable_ancestor(p: &Path) -> Option<PathBuf> {
    use std::os::unix::fs::PermissionsExt;
    p.ancestors()
        .filter_map(|a| fs::metadata(a).ok().map(|m| (a, m)))
        .filter(|(_, m)| m.is_dir())
        .find(|(_, m)| m.permissions().mode() & 0o001 == 0)
        .map(|(a, _)| a.to_path_buf())
}

/// One password, then the ticket refreshed every minute for the run.
fn take_sudo() -> Result<(), String> {
    println!("\nsudo is needed for setcap and for rgw-vstart.sh's fixture;  once:");
    if !Command::new("sudo").arg("-v").status().map(|s| s.success()).unwrap_or(false) {
        return Err("sudo -v failed".into());
    }
    std::thread::spawn(|| loop {
        std::thread::sleep(Duration::from_secs(60));
        let _ = Command::new("sudo").args(["-n", "-v"]).status();
    });
    Ok(())
}

/// The capability string, from rgw-vstart.sh's own `sudo setcap` line.
fn vstart_caps(source: &Path) -> Result<String, String> {
    let p = source.join("src/script/rgw/rgw-vstart.sh");
    let text = fs::read_to_string(&p).map_err(|e| format!("{}: {e}", p.display()))?;
    text.lines()
        .find_map(|l| l.trim().strip_prefix("sudo setcap ")
                       .and_then(|r| r.split("+p").next()).map(str::to_string))
        .ok_or_else(|| format!("{} has no `sudo setcap <caps>+p` line", p.display()))
}

/// The gateway answers once vstart has started it, but not at once.
fn wait_for_gateway() -> Result<(), String> {
    let start = Instant::now();
    while start.elapsed() < Duration::from_secs(60) {
        if let Ok(mut s) = TcpStream::connect("127.0.0.1:8000") {
            if s.write_all(b"GET / HTTP/1.0\r\n\r\n").is_ok() {
                let mut first = String::new();
                if BufReader::new(s).read_line(&mut first).is_ok() && first.starts_with("HTTP/") {
                    return Ok(());
                }
            }
        }
        std::thread::sleep(Duration::from_millis(500));
    }
    Err("the gateway did not answer on :8000 within 60 s".into())
}

/// What the gateway says it runs, against what this deployment asked
/// for:  its startup line `nsfs: user identity override on|off, ...`,
/// the last one in vstart's gateway log.
fn check_user_identity_override(build: &Path, want: bool) -> Result<(), String> {
    let log = build.join("out/radosgw.8000.log");
    let text = fs::read(&log).map_err(|e| format!("{}: {e}", log.display()))?;
    let text = String::from_utf8_lossy(&text);
    let said = text.lines().rev()
        .find_map(|l| l.split("nsfs: user identity override ").nth(1))
        .map(|rest| rest.starts_with("on"));
    match said {
        Some(on) if on == want => Ok(()),
        Some(on) => Err(format!("asked for user-identity-override={}, and the gateway \
                                 runs it {}", if want { "on" } else { "off" },
                                if on { "on" } else { "off" })),
        None => Err(format!("{} does not say which user identity override the \
                             gateway runs", log.display())),
    }
}

fn nproc() -> String {
    Command::new("nproc").output()
        .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string())
        .unwrap_or_else(|_| "1".into())
}

fn parse_args() -> Opts {
    let mut flags = Flags::default();
    let mut config: Option<PathBuf> = None;
    let mut args = std::env::args().skip(1);
    while let Some(a) = args.next() {
        let mut val = |flag: &str| args.next().unwrap_or_else(|| die(&format!("{flag} takes a value")));
        match a.as_str() {
            "--dry-run" => flags.dry_run = true,
            "--no-build" => flags.no_build = true,
            "--no-unit" => flags.no_unit = true,
            "--no-librgw" => flags.no_librgw = true,
            "--test" => flags.test = Some(val("--test")),
            "--lifecycle" => flags.lifecycle = true,
            "--cells" => flags.cells = Some(val("--cells").split(',').map(str::to_string).collect()),
            "--config" => config = Some(PathBuf::from(val("--config"))),
            "--build" => flags.paths.push(("build", val("--build"))),
            "--conf" => flags.paths.push(("conf", val("--conf"))),
            "--adoption-cache" => flags.paths.push(("adoption_cache", val("--adoption-cache"))),
            "--noobaa-src" => flags.paths.push(("noobaa_src", val("--noobaa-src"))),
            "--work-root" => flags.paths.push(("work_root", val("--work-root"))),
            "--parallel-log" => flags.paths.push(("parallel_log", val("--parallel-log"))),
            _ => die(&format!("unknown argument {a};  see the header of validate.rs")),
        }
    }
    if let Some(want) = &flags.cells {
        for w in want {
            if !CELLS.iter().any(|c| c.name == w) {
                die(&format!("no cell {w};  the cells are {}",
                             CELLS.iter().map(|c| c.name).collect::<Vec<_>>().join(", ")));
            }
        }
    }

    /* an option wins over the file;  the per-user file is read only if
     * it exists, and a file named by --config or VALIDATE_CONF must */
    let named = config.or_else(|| std::env::var_os("VALIDATE_CONF")
        .filter(|v| !v.is_empty()).map(PathBuf::from));
    let file = named.clone().unwrap_or_else(default_config);
    let mut settings = read_config(&file, named.is_some());
    for (k, v) in flags.paths {
        settings.insert(k.to_string(), v);
    }
    let path = |key: &str| settings.get(key).map(|v| PathBuf::from(expand_home(v)));
    let need = |key: &str, flag: &str| path(key).unwrap_or_else(|| die(&format!(
        "no {key}:  pass {flag}, or set `{key} = ...` in {}", file.display())));

    let conversion = CELLS.iter().any(|c| {
        let selected = match &flags.cells {
            Some(want) => want.iter().any(|w| w == c.name),
            None => c.by_default,
        };
        selected && c.suites.iter().any(|s| matches!(s, Suite::Conversion { .. }))
    });
    let build_dir = need("build", "--build");
    let source_dir = source_tree(&build_dir).unwrap_or_else(|e| die(&e));
    Opts {
        dry_run: flags.dry_run, build: !flags.no_build, unit: !flags.no_unit,
        librgw: !flags.no_librgw, lifecycle: flags.lifecycle,
        cells: flags.cells, test: flags.test,
        conf: need("conf", "--conf"),
        adoption_cache: if conversion { need("adoption_cache", "--adoption-cache") }
                        else { path("adoption_cache").unwrap_or_default() },
        noobaa_src: if conversion { need("noobaa_src", "--noobaa-src") }
                    else { path("noobaa_src").unwrap_or_default() },
        work_root: path("work_root"),
        parallel_log: path("parallel_log"),
        build_dir, source_dir,
    }
}

/// The command line, before the config file fills in what it leaves out.
#[derive(Default)]
struct Flags {
    dry_run: bool,
    no_build: bool,
    no_unit: bool,
    no_librgw: bool,
    lifecycle: bool,
    cells: Option<Vec<String>>,
    test: Option<String>,
    paths: Vec<(&'static str, String)>,
}

/// `$XDG_CONFIG_HOME/s3-tests-rs/validate.conf`, or under `~/.config`.
fn default_config() -> PathBuf {
    let base = std::env::var_os("XDG_CONFIG_HOME").filter(|v| !v.is_empty())
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(expand_home("~/.config")));
    base.join("s3-tests-rs/validate.conf")
}

/// `key = value` lines;  `#` starts a comment.  A key this program does
/// not know is refused, so a misspelled one is not silently ignored.
fn read_config(file: &Path, named: bool) -> std::collections::HashMap<String, String> {
    const KEYS: &[&str] = &["build", "conf", "adoption_cache", "noobaa_src", "work_root",
                            "parallel_log"];
    let mut m = std::collections::HashMap::new();
    let text = match fs::read_to_string(file) {
        Ok(t) => t,
        Err(e) if !named && e.kind() == std::io::ErrorKind::NotFound => return m,
        Err(e) => die(&format!("{}: {e}", file.display())),
    };
    for (n, line) in text.lines().enumerate() {
        let line = line.split('#').next().unwrap_or("").trim();
        if line.is_empty() {
            continue;
        }
        let Some((k, v)) = line.split_once('=') else {
            die(&format!("{}:{}:  not `key = value`", file.display(), n + 1));
        };
        let k = k.trim();
        if !KEYS.contains(&k) {
            die(&format!("{}:{}:  unknown key {k};  the keys are {}",
                         file.display(), n + 1, KEYS.join(", ")));
        }
        m.insert(k.to_string(), v.trim().to_string());
    }
    m
}

fn expand_home(v: &str) -> String {
    match v.strip_prefix("~/") {
        Some(rest) => format!("{}/{rest}", std::env::var("HOME").unwrap_or_default()),
        None => v.to_string(),
    }
}

/// The ceph source tree a build was configured from, as `rgw-vstart.sh`
/// finds it:  `CMAKE_HOME_DIRECTORY` in the build's `CMakeCache.txt`.
/// A build directory need not be inside the tree it builds.
fn source_tree(build: &Path) -> Result<PathBuf, String> {
    let cache = build.join("CMakeCache.txt");
    let text = fs::read_to_string(&cache)
        .map_err(|e| format!("{}: {e};  is {} the ceph build directory?",
                             cache.display(), build.display()))?;
    text.lines()
        .find_map(|l| l.strip_prefix("CMAKE_HOME_DIRECTORY:INTERNAL="))
        .map(|v| PathBuf::from(v.trim()))
        .ok_or_else(|| format!("{} has no CMAKE_HOME_DIRECTORY", cache.display()))
}

fn die(msg: &str) -> ! {
    eprintln!("validate:  {msg}");
    std::process::exit(2);
}
