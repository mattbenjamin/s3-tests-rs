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
//! Each cell's `rgw-vstart.sh --clean` stops the gateway before it, and
//! the run stops the last:  every `radosgw` that started after it did.
//! A gateway already up when the run began is left alone.
//!
//!   scripts/validate.rs [--dry-run] [--no-build] [--no-unit] [--no-librgw]
//!                       [--lifecycle] [--cells a,b | --preset NAME]
//!                       [--overrides LIST]
//!                       [--test EXPR]
//!                       [--config FILE] [--build DIR] [--conf FILE]
//!                       [--adoption-cache DIR] [--noobaa-src DIR]
//!                       [--work-root DIR] [--parallel-log FILE]
//!                       [--gpfs-root DIR]
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
//!   gpfs_root      = /path/on/gpfs              run on GPFS:  every cell's
//!                                                gateway as `rgw-vstart.sh
//!                                                --gpfs --gpfs-root`;  work_root
//!                                                must be on GPFS too;  optional
//!
//! `--test` runs only the tests a nextest filter expression selects, in
//! each cell, in place of its profiles;  `--cells` picks the cells.  The
//! conversion cells -- the adoption corpus, `standup.rs conversion` --
//! run only when named:  each takes a fresh work directory, with the
//! NooBaa worktree already built in `--adoption-cache` linked into it.
//!
//! `--lifecycle` runs each cell a second time, as `<cell>+lifecycle`,
//! with the gateway running lifecycle at the suite's interval
//! (`rgw-vstart.sh --with-lifecycle`):  its profiles, and then the
//! `lifecycle` profile, which no other step runs.  With `--test` the
//! expression runs in both, and the `lifecycle` profile does not.
//!
//! `--overrides` picks which settings of the two identity overrides the
//! cells that cross them run:  `off`, `user`, `role`, `user-role`, comma
//! separated;  all four by default.  `--overrides off,user` leaves the
//! role override out.  It narrows `--cells` the same way, and a cell
//! that does not cross the overrides ignores it.  A run that leaves a
//! setting out says so, and validates only what it ran.
//!
//! `--preset` runs a named set of cells in place of `--cells`:  `quick`,
//! `cover` or `full` (`PRESETS`).  Only `full` is the whole matrix;  a
//! run of another says so.  `--overrides` narrows a preset too, leaving
//! out the cells it excludes.

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
    /* the web identity tests with `has_keycloak` -- profiles `sts` and
     * `role-identity` -- against the IdP the cell's `--with-keycloak`
     * started, which is stopped after them.  Its cell reads and writes
     * a copy of the s3tests.conf, which the IdP's `[webidentity]` is
     * written into */
    Keycloak,
    /* `standup.rs conversion`, which starts its own gateway;  `adopt_by`
     * is its setting of that name:  command, automatic or promotion;
     * `kafka` starts kafka-vstart.sh's broker for the `notify` steps'
     * Kafka half, and stops it after */
    Conversion { impersonate: bool, access_model: &'static str, adopt_by: &'static str,
                 kafka: bool },
}

struct Cell {
    name: &'static str,
    /* run under each of the four settings of the two identity
     * overrides, as `<name>`, `<name>-user`, `<name>-role` and
     * `<name>-user-role`;  only `<name>`, with both off, otherwise */
    overrides: bool,
    /* empty:  the suite stands its own gateway up */
    vstart: &'static [&'static str],
    suites: &'static [Suite],
    /* run when no --cells names any */
    by_default: bool,
}

/* Adding a configuration is an edit here.  A cell with `overrides`
 * is four deployments:  rgw_nsfs_user_identity_override, an account
 * user's own row overriding its account's, as NooBaa serves an IAM user
 * from its own record;  and rgw_nsfs_role_identity_override, a role
 * given an identity running as it, NooBaa's documented future. */
const CELLS: &[Cell] = &[
    Cell { name: "posix-strict", overrides: true,
           vstart: &["--store", "nsfs", "--with-impersonation", "--clean"],
           suites: &[Suite::Profile("baseline"),
                     Suite::Profile("access-model-switch"),
                     Suite::DynamicGroups,
                     Suite::Profile("role-identity")],
           by_default: true },
    Cell { name: "s3-native", overrides: true,
           vstart: &["--store", "nsfs", "--with-impersonation",
                     "--access-model", "s3-native", "--clean"],
           suites: &[Suite::Profile("baseline"),
                     Suite::Profile("access-model-switch"),
                     Suite::DynamicGroups,
                     Suite::Profile("role-identity")],
           by_default: true },
    /* the web identity tests against a Keycloak IdP, under each access
     * model.  Off by default, since the IdP needs podman.  Before `plain`,
     * as every impersonating cell is:  its --clean removes the fixture
     * they leave in the data root, which a conversion's stand-up refuses */
    Cell { name: "keycloak-s3-native", overrides: false,
           vstart: &["--store", "nsfs", "--with-impersonation",
                     "--access-model", "s3-native", "--with-keycloak", "--clean"],
           suites: &[Suite::Keycloak],
           by_default: false },
    Cell { name: "keycloak-posix-strict", overrides: false,
           vstart: &["--store", "nsfs", "--with-impersonation", "--with-keycloak", "--clean"],
           suites: &[Suite::Keycloak],
           by_default: false },
    Cell { name: "plain", overrides: true,
           vstart: &["--store", "nsfs", "--bucket-profile", "strong", "--clean",
                     "-o", KAFKA_CONFIG_KEYS],
           suites: &[Suite::Profile("baseline"), Suite::Librgw],
           by_default: true },
    /* the adoption corpus:  NooBaa writes a tree, RGW captures it before
     * and after conversion, and the captures are compared -- with RGW
     * as the gateway's own identity, and impersonating under each
     * access model */
    Cell { name: "conversion", overrides: true, vstart: &[],
           suites: &[Suite::Conversion { impersonate: false, access_model: "posix-strict",
                                       adopt_by: "command", kafka: false }],
           by_default: false },
    Cell { name: "conversion-posix-strict", overrides: true, vstart: &[],
           suites: &[Suite::Conversion { impersonate: true, access_model: "posix-strict",
                                       adopt_by: "command", kafka: false }],
           by_default: false },
    Cell { name: "conversion-s3-native", overrides: true, vstart: &[],
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
    Cell { name: "kafka", overrides: false,
           vstart: &["--store", "nsfs", "--clean", "--with-kafka", "-o", KAFKA_CONFIG_KEYS],
           suites: &[Suite::Kafka],
           by_default: false },
    /* `conversion` with a Kafka half in the `notify` steps:  a NooBaa
     * Kafka connection, carried across by the import as a topic with
     * its librdkafka settings, compared as `noobaa:rgw-notify-kafka`.
     * Off by default, since the broker needs podman */
    Cell { name: "conversion-kafka", overrides: false, vstart: &[],
           suites: &[Suite::Conversion { impersonate: false, access_model: "posix-strict",
                                       adopt_by: "command", kafka: true }],
           by_default: false },
    Cell { name: "conversion-automatic", overrides: false, vstart: &[],
           suites: &[Suite::Conversion { impersonate: false, access_model: "posix-strict",
                                       adopt_by: "automatic", kafka: false }],
           by_default: false },
    Cell { name: "conversion-promotion", overrides: false, vstart: &[],
           suites: &[Suite::Conversion { impersonate: false, access_model: "posix-strict",
                                       adopt_by: "promotion", kafka: false }],
           by_default: false },
];

/* Named sets of cells, for `--preset`.  `cells: None` is every cell. */
struct Preset {
    name: &'static str,
    cells: Option<&'static [&'static str]>,
    what: &'static str,
}

const PRESETS: &[Preset] = &[
    Preset { name: "quick", cells: Some(&["posix-strict", "plain"]),
             what: "impersonation on and off, the switches and librgw;  both overrides off, no adoption" },
    /* every value of every dimension once, fresh and adopted, and the web
     * identity tests under each access model;  not every combination */
    Preset { name: "cover",
             cells: Some(&["posix-strict", "s3-native-user", "s3-native-role", "posix-strict-user-role",
                           "plain",
                           "conversion", "conversion-posix-strict-role", "conversion-s3-native-user",
                           "conversion-user-role", "conversion-promotion",
                           "keycloak-s3-native", "keycloak-posix-strict"]),
             what: "every value of every dimension once, fresh and adopted, and the web identity \
                    tests under each access model;  not every combination" },
    Preset { name: "full", cells: None, what: "every cell" },
];

struct Opts {
    dry_run: bool,
    build: bool,
    unit: bool,
    librgw: bool,
    lifecycle: bool,
    cells: Option<Vec<String>>,
    /* the override settings the crossing cells run */
    overrides: Vec<Overrides>,
    preset: Option<&'static Preset>,
    test: Option<String>,
    build_dir: PathBuf,
    /* the ceph source tree, from the build's CMakeCache.txt */
    source_dir: PathBuf,
    conf: PathBuf,
    /* the endpoint `conf` names, `[DEFAULT] host` and `port`:  the
     * gateway each cell starts serves there */
    host: String,
    port: u16,
    adoption_cache: PathBuf,
    noobaa_src: PathBuf,
    /* where the conversion cells' work directories go;  under the run
     * directory when unset */
    work_root: Option<PathBuf>,
    parallel_log: Option<PathBuf>,
    /* the GPFS directory every cell's gateway is started on, its data
     * root, LMDB and userdb under it;  a local filesystem when unset */
    gpfs_root: Option<PathBuf>,
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
    /* the deployment's identity overrides;  `None` for the steps
     * outside any deployment, the build and the unit suite */
    overrides: Option<Overrides>,
}

/// rgw_nsfs_user_identity_override and rgw_nsfs_role_identity_override:
/// passed to the gateway, shown in every label, and checked against what
/// the gateway says it runs.
#[derive(Clone, Copy, PartialEq)]
struct Overrides {
    user: bool,
    role: bool,
}

impl Overrides {
    const ALL: [Overrides; 4] = [Overrides { user: false, role: false },
                                 Overrides { user: true, role: false },
                                 Overrides { user: false, role: true },
                                 Overrides { user: true, role: true }];

    /// The cell's name for this setting:  a suffix per override on.
    fn suffix(self) -> &'static str {
        match (self.user, self.role) {
            (false, false) => "",
            (true, false) => "-user",
            (false, true) => "-role",
            (true, true) => "-user-role",
        }
    }

    /// The setting's name in `--overrides`.
    fn name(self) -> &'static str {
        match self.suffix() {
            "" => "off",
            s => &s[1..],
        }
    }
}

/// What a run leaves out, said in the plan and in `summary.txt`:  a
/// preset short of `full`, and override settings;  and the filesystem,
/// where it is GPFS.  Empty for a run of all four settings and no
/// preset, or `full`, on a local filesystem.
fn partial_run(o: &Opts) -> Vec<String> {
    let mut notes = Vec::new();
    if let Some(g) = &o.gpfs_root {
        notes.push(format!("on GPFS:  every gateway under {}, every conversion tree under the \
                            work root;  the unimpersonated conversion cells are left out, since \
                            NooBaa cannot register with GPFS unless it is root", g.display()));
    }
    if let Some(p) = o.preset.filter(|p| p.cells.is_some()) {
        notes.push(format!("preset {}:  {};  this run validates only what it ran",
                           p.name, p.what));
    }
    let left: Vec<&str> = Overrides::ALL.iter().filter(|ov| !o.overrides.contains(ov))
        .map(|ov| ov.name()).collect();
    if !left.is_empty() {
        notes.push(format!(
            "overrides:  {} only;  {} left out, so this run validates only what it ran",
            o.overrides.iter().map(|ov| ov.name()).collect::<Vec<_>>().join(", "),
            left.join(", ")));
    }
    notes
}

/// Whether the cell's stand-up runs NooBaa as this user rather than as
/// root:  a conversion that does not impersonate.
fn noobaa_unprivileged(c: &Cell) -> bool {
    c.suites.iter().any(|s| matches!(s, Suite::Conversion { impersonate: false, .. }))
}

fn on_off(on: bool) -> &'static str {
    if on { "on" } else { "off" }
}

/// A cell's deployments, each with its name:  four for a cell with
/// `overrides`, one otherwise.
fn deployments(c: &Cell) -> impl Iterator<Item = (Overrides, String)> + '_ {
    let n = if c.overrides { 4 } else { 1 };
    Overrides::ALL[..n].iter().map(move |o| (*o, format!("{}{}", c.name, o.suffix())))
}

/// How a step is labelled:  its deployment, with every setting that
/// distinguishes one deployment from another stated rather than implied.
fn label(cell: &str, over: Option<Overrides>) -> String {
    match over {
        Some(o) => format!("{cell} · user-identity-override={} · role-identity-override={}",
                           on_off(o.user), on_off(o.role)),
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
    let notes = partial_run(&opts);
    for n in &notes {
        println!("validate:  {n}");
    }
    for s in &steps {
        println!("  [{}] {}:  {}", label(&s.cell, s.overrides), s.name, s.show());
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
        println!("\n==> [{}] {}", label(&s.cell, s.overrides), s.name);
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
            if let Err(e) = wait_for_gateway(&opts.host, opts.port) {
                results.push((s.cell.clone(), "gateway".into(), Res::Fail, e));
                skip_cell = Some(s.cell.clone());
                continue;
            }
            /* the label is a checked fact:  the gateway says which it runs */
            if let Some(want) = s.overrides {
                if let Err(e) = check_identity_overrides(&opts.build_dir, opts.port, want) {
                    println!("    FAIL  {e}");
                    results.push((s.cell.clone(), "identity-overrides".into(), Res::Fail, e));
                    skip_cell = Some(s.cell.clone());
                    continue;
                }
            }
        }
        results.push((s.cell.clone(), s.name.clone(), res, summary));
    }
    /* the next cell's --clean stops each cell's gateway but the last */
    if let Err(e) = stop_our_gateways(&opts.build_dir) {
        results.push(("run".into(), "stop-gateway".into(), Res::Fail, e));
    }

    let setting =|cell: &str| steps.iter().find(|s| s.cell == cell)
        .and_then(|s| s.overrides)
        .map(|o| (on_off(o.user), on_off(o.role))).unwrap_or(("-", "-"));
    let mut table: String = notes.iter().map(|n| format!("\n{n}")).collect();
    table += "\ncell                              user-identity-override  role-identity-override  step                    result\n";
    for (cell, name, res, summary) in &results {
        let (user, role) = setting(cell);
        table += &format!("{cell:<33} {user:<23} {role:<23} {name:<23} {} {summary}\n",
                          res.label());
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
        env: Vec::new(), parallel_log: false, overrides: None,
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
    for (c, (ov, name)) in CELLS.iter().flat_map(|c| deployments(c).map(move |d| (c, d))) {
        match &o.cells {
            Some(want) if !want.iter().any(|w| *w == name) => continue,
            None if !c.by_default => continue,
            _ => {}
        }
        if c.overrides && !o.overrides.contains(&ov) {
            continue;
        }
        /* NooBaa unprivileged cannot register with GPFS (gpfs_ganesha
         * refuses a process that is not root), so on GPFS a conversion
         * runs only as an installation does, NooBaa as root:  the
         * impersonated cells */
        if o.gpfs_root.is_some() && noobaa_unprivileged(c) {
            if o.preset.is_none() && o.cells.as_ref().is_some_and(|w| w.contains(&name)) {
                die(&format!("--cells names {name}, which runs NooBaa unprivileged, and \
                              NooBaa cannot register with GPFS unless it is root"));
            }
            continue;
        }
        /* refused before anything starts, rather than failing inside the
         * sidecar's start */
        if c.suites.iter().any(|s| matches!(s, Suite::Kafka | Suite::Keycloak))
            && !Command::new("podman").arg("--version").stdout(Stdio::null())
                .stderr(Stdio::null()).status().is_ok_and(|st| st.success()) {
            die(&format!("the {} cell runs a sidecar in a container, and \
                          podman is not installed", c.name));
        }
        let keycloak = c.suites.iter().any(|s| matches!(s, Suite::Keycloak));
        /* a conversion's gateway is started with --with-lifecycle already */
        let variants = if o.lifecycle && !c.vstart.is_empty() { &[false, true][..] }
                       else { &[false][..] };
        for lc in variants {
            let cell = if *lc { format!("{name}+lifecycle") } else { name.clone() };
            let mut argv = vec![vstart.to_string_lossy().into_owned()];
            argv.extend(s(c.vstart));
            argv.extend(["--port".into(), o.port.to_string()]);
            if let Some(g) = &o.gpfs_root {
                argv.extend(["--gpfs".into(), "--gpfs-root".into(),
                             g.to_string_lossy().into_owned()]);
            }
            if *lc {
                argv.push("--with-lifecycle".into());
            }
            if ov.user {
                argv.push("--user-identity-override".into());
            }
            if ov.role {
                argv.push("--role-identity-override".into());
            }
            let first = steps.len();
            /* the IdP's cells read their own copy, never the one named */
            let conf = if keycloak { run_dir.join(&cell).join("s3tests.conf") }
                       else { o.conf.clone() };
            if keycloak {
                steps.push(step(&cell, "conf", b, s(&["sh", "-c",
                    "mkdir -p \"$(dirname \"$2\")\" && cp \"$1\" \"$2\"",
                    "sh", &o.conf.to_string_lossy(), &conf.to_string_lossy()])));
            }
            if !c.vstart.is_empty() {
                let mut st = step(&cell, "vstart", b, argv);
                if keycloak {
                    st.env.push(("S3TEST_CONF".into(), conf.to_string_lossy().into()));
                }
                steps.push(st);
            }
            if let Some(expr) = &o.test {
                let mut st = step(&cell, "test", suite_dir, s(&["cargo", "nextest", "run",
                    "--no-fail-fast", "--features", FEATURES, "-E", expr]));
                st.env.push(("S3TEST_CONF".into(), conf.to_string_lossy().into()));
                st.parallel_log = true;
                steps.push(st);
            }
            for suite in c.suites {
                match suite {
                    Suite::Profile(_) | Suite::Conversion { .. } | Suite::DynamicGroups
                    | Suite::Kafka | Suite::Keycloak
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
                                     ov.user.to_string()));
                        st.env.push(("ADOPTION_ROLE_IDENTITY_OVERRIDE".into(),
                                     ov.role.to_string()));
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
                        st.env.push(("S3TEST_CONF".into(), conf.to_string_lossy().into()));
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
                                st.env.push(("S3TEST_CONF".into(), conf.to_string_lossy().into()));
                                st.parallel_log = true;
                            }
                            steps.push(st);
                        }
                    }
                    Suite::Kafka => {
                        let mut st = step(&cell, "kafka", suite_dir, s(&["cargo", "nextest", "run",
                            "--no-fail-fast", "--features", &format!("{FEATURES},has_kafka"),
                            "-E", "binary(test_sns)"]));
                        st.env.push(("S3TEST_CONF".into(), conf.to_string_lossy().into()));
                        st.parallel_log = true;
                        steps.push(st);
                        /* run whatever the tests returned:  the broker is a
                         * container, and the next cell's --clean only stops
                         * one rgw-vstart.sh started */
                        steps.push(step(&cell, "kafka-stop", b, vec![
                            o.source_dir.join("src/script/rgw/kafka-vstart.sh")
                                .to_string_lossy().into_owned(), "--stop".into()]));
                    }
                    Suite::Keycloak => {
                        for p in ["sts", "role-identity"] {
                            let mut st = step(&cell, p, suite_dir, s(&["cargo", "nextest", "run",
                                "-P", p, "--no-fail-fast", "--features",
                                &format!("{FEATURES},has_keycloak")]));
                            st.env.push(("S3TEST_CONF".into(), conf.to_string_lossy().into()));
                            st.parallel_log = true;
                            steps.push(st);
                        }
                        /* run whatever the tests returned, as for Kafka */
                        steps.push(step(&cell, "keycloak-stop", b, vec![
                            o.source_dir.join("src/script/rgw/keycloak-vstart.sh")
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
            /* the lifecycle tests, which every profile above filters out,
             * where the gateway runs lifecycle at the suite's interval */
            if *lc && o.test.is_none() {
                let mut st = step(&cell, "lifecycle", suite_dir, s(&["cargo", "nextest", "run",
                    "-P", "lifecycle", "--no-fail-fast", "--features", FEATURES]));
                st.env.push(("S3TEST_CONF".into(), conf.to_string_lossy().into()));
                st.parallel_log = true;
                steps.push(st);
            }
            for st in &mut steps[first..] {
                st.overrides = Some(ov);
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
    /* a GPFS run that quietly ran on another filesystem validates that
     * filesystem again:  the gateways' root, and every conversion tree */
    if let Some(g) = &o.gpfs_root {
        let mut on = vec![g.clone()];
        if steps.iter().any(|s| s.name == "conversion") {
            on.push(o.work_root.clone().ok_or("gpfs_root is set, and the conversion cells' \
                trees would be under the run directory;  set work_root to a directory on GPFS")?);
        }
        for p in on {
            let fs = Command::new("stat").args(["-f", "-c", "%T"]).arg(&p).output()
                .map(|out| String::from_utf8_lossy(&out.stdout).trim().to_string())
                .unwrap_or_default();
            if fs != "gpfs" {
                return Err(format!("{} is on {}, not GPFS;  gpfs_root and work_root must \
                                    both be on GPFS", p.display(),
                                   if fs.is_empty() { "no filesystem stat could name" } else { &fs }));
            }
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
/// `rgw-vstart.sh` has already refused a gateway that is not the one it
/// started.
fn wait_for_gateway(host: &str, port: u16) -> Result<(), String> {
    let start = Instant::now();
    while start.elapsed() < Duration::from_secs(60) {
        if let Ok(mut s) = TcpStream::connect((host, port)) {
            if s.write_all(b"GET / HTTP/1.0\r\n\r\n").is_ok() {
                let mut first = String::new();
                if BufReader::new(s).read_line(&mut first).is_ok() && first.starts_with("HTTP/") {
                    return Ok(());
                }
            }
        }
        std::thread::sleep(Duration::from_millis(500));
    }
    Err(format!("the gateway did not answer on {host}:{port} within 60 s"))
}

/// Each gateway this run started, stopped:  a `radosgw` named by a pid
/// file in the build's `out/` that started after this process did.  A
/// gateway that was up before the run is not ours, and is left alone.
fn stop_our_gateways(build: &Path) -> Result<(), String> {
    /* field 22, starttime, in clock ticks since boot;  the fields after
     * the parenthesized comm start at field 3 */
    let started = |pid: &str| fs::read_to_string(format!("/proc/{pid}/stat")).ok()
        .and_then(|st| st.rsplit_once(')')
            .and_then(|(_, rest)| rest.split_whitespace().nth(19)?.parse::<u64>().ok()));
    let Some(ours_since) = started("self") else {
        return Err("cannot read this process's start time from /proc/self/stat".into());
    };
    let Ok(dir) = fs::read_dir(build.join("out")) else {
        return Ok(());
    };
    let mut stuck = Vec::new();
    for e in dir.flatten() {
        let name = e.file_name().to_string_lossy().into_owned();
        if !(name.starts_with("radosgw.") && name.ends_with(".pid")) {
            continue;
        }
        let Some(pid) = fs::read_to_string(e.path()).ok()
            .and_then(|p| p.trim().parse::<u32>().ok()).map(|p| p.to_string()) else {
            continue;
        };
        let is_radosgw = fs::read_to_string(format!("/proc/{pid}/comm"))
            .is_ok_and(|c| c.trim() == "radosgw");
        if !is_radosgw || !started(&pid).is_some_and(|t| t >= ours_since) {
            continue;
        }
        let _ = Command::new("kill").arg(&pid).status();
        let start = Instant::now();
        while Path::new(&format!("/proc/{pid}")).exists()
            && start.elapsed() < Duration::from_secs(30) {
            std::thread::sleep(Duration::from_millis(500));
        }
        if Path::new(&format!("/proc/{pid}")).exists() {
            stuck.push(format!("radosgw pid {pid} ({name})"));
        } else {
            println!("validate:  stopped radosgw pid {pid} ({name})");
        }
    }
    if stuck.is_empty() {
        Ok(())
    } else {
        Err(format!("{} did not stop within 30 s", stuck.join(", ")))
    }
}

/// What the gateway says it runs, against what this deployment asked
/// for:  its startup line `nsfs: user identity override on|off, role
/// identity override on|off`, the last one in vstart's gateway log.  The
/// `identity-override` hint logs each override alone, so a line naming
/// one is not the startup line.
fn check_identity_overrides(build: &Path, port: u16, want: Overrides) -> Result<(), String> {
    let log = build.join(format!("out/radosgw.{port}.log"));
    let text = fs::read(&log).map_err(|e| format!("{}: {e}", log.display()))?;
    let text = String::from_utf8_lossy(&text);
    let said = text.lines().rev().find_map(|l| {
        let rest = l.split("nsfs: user identity override ").nth(1)?;
        let role = rest.split(", role identity override ").nth(1)?;
        Some(Overrides { user: rest.starts_with("on"), role: role.starts_with("on") })
    });
    match said {
        Some(o) if o == want => Ok(()),
        Some(o) => Err(format!("asked for user-identity-override={} and \
                                role-identity-override={}, and the gateway runs \
                                them {} and {}", on_off(want.user), on_off(want.role),
                               on_off(o.user), on_off(o.role))),
        None => Err(format!("{} does not say which identity overrides the gateway \
                             runs", log.display())),
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
            "--preset" => flags.preset = Some(val("--preset")),
            "--overrides" => flags.overrides = Some(val("--overrides").split(',').map(str::to_string).collect()),
            "--config" => config = Some(PathBuf::from(val("--config"))),
            "--build" => flags.paths.push(("build", val("--build"))),
            "--conf" => flags.paths.push(("conf", val("--conf"))),
            "--adoption-cache" => flags.paths.push(("adoption_cache", val("--adoption-cache"))),
            "--noobaa-src" => flags.paths.push(("noobaa_src", val("--noobaa-src"))),
            "--work-root" => flags.paths.push(("work_root", val("--work-root"))),
            "--parallel-log" => flags.paths.push(("parallel_log", val("--parallel-log"))),
            "--gpfs-root" => flags.paths.push(("gpfs_root", val("--gpfs-root"))),
            _ => die(&format!("unknown argument {a};  see the header of validate.rs")),
        }
    }
    let names: Vec<String> = CELLS.iter().flat_map(|c| deployments(c).map(|d| d.1)).collect();
    let preset = flags.preset.as_deref().map(|want| {
        if flags.cells.is_some() {
            die("--preset and --cells name the cells two ways;  give one");
        }
        PRESETS.iter().find(|p| p.name == want).unwrap_or_else(|| die(&format!(
            "no preset {want};  the presets are {}",
            PRESETS.iter().map(|p| p.name).collect::<Vec<_>>().join(", "))))
    });
    if let Some(p) = preset {
        flags.cells = Some(match p.cells {
            Some(cells) => cells.iter().map(|c| c.to_string()).collect(),
            None => names.clone(),
        });
    }
    if let Some(want) = &flags.cells {
        for w in want {
            if !names.contains(w) {
                die(&format!("no cell {w};  the cells are {}", names.join(", ")));
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

    let overrides: Vec<Overrides> = match &flags.overrides {
        None => Overrides::ALL.to_vec(),
        Some(want) => {
            for w in want {
                if !Overrides::ALL.iter().any(|o| o.name() == w) {
                    die(&format!("no override setting {w};  the settings are {}",
                                 Overrides::ALL.map(Overrides::name).join(", ")));
                }
            }
            Overrides::ALL.into_iter().filter(|o| want.iter().any(|w| w == o.name())).collect()
        }
    };
    /* a cell named outright is not dropped by the setting filter;  one a
     * preset names is */
    for (c, (ov, name)) in CELLS.iter().flat_map(|c| deployments(c).map(move |d| (c, d))) {
        if preset.is_none() && c.overrides && !overrides.contains(&ov)
            && flags.cells.as_ref().is_some_and(|w| w.contains(&name)) {
            die(&format!("--cells names {name}, and --overrides leaves out {}", ov.name()));
        }
    }
    let conversion = CELLS.iter().any(|c| {
        let selected = match &flags.cells {
            Some(want) => deployments(c).any(|(_, name)| want.contains(&name)),
            None => c.by_default,
        };
        selected && c.suites.iter().any(|s| matches!(s, Suite::Conversion { .. }))
    });
    let build_dir = need("build", "--build");
    let source_dir = source_tree(&build_dir).unwrap_or_else(|e| die(&e));
    let conf = need("conf", "--conf");
    let (host, port) = endpoint(&conf).unwrap_or_else(|e| die(&e));
    Opts {
        dry_run: flags.dry_run, build: !flags.no_build, unit: !flags.no_unit,
        librgw: !flags.no_librgw, lifecycle: flags.lifecycle,
        cells: flags.cells, overrides, preset, test: flags.test,
        conf, host, port,
        adoption_cache: if conversion { need("adoption_cache", "--adoption-cache") }
                        else { path("adoption_cache").unwrap_or_default() },
        noobaa_src: if conversion { need("noobaa_src", "--noobaa-src") }
                    else { path("noobaa_src").unwrap_or_default() },
        work_root: path("work_root"),
        parallel_log: path("parallel_log"),
        gpfs_root: path("gpfs_root"),
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
    overrides: Option<Vec<String>>,
    preset: Option<String>,
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
                            "parallel_log", "gpfs_root"];
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

/// The endpoint an s3tests.conf names, read as the suite reads it
/// (`src/config.rs`):  `host` in `[DEFAULT]` or `[default]`, required,
/// and `port` there, 8000 when absent.
fn endpoint(conf: &Path) -> Result<(String, u16), String> {
    let text = fs::read_to_string(conf).map_err(|e| format!("{}: {e}", conf.display()))?;
    let (mut host, mut port) = (None, None);
    let mut in_default = false;
    for line in text.lines() {
        let line = line.split('#').next().unwrap_or("").trim();
        if line.starts_with('[') {
            in_default = line == "[DEFAULT]" || line == "[default]";
            continue;
        }
        let Some((k, v)) = line.split_once('=').filter(|_| in_default) else {
            continue;
        };
        match k.trim() {
            "host" => host = Some(v.trim().to_string()),
            "port" => port = Some(v.trim().parse::<u16>()
                .map_err(|e| format!("{}:  port {}: {e}", conf.display(), v.trim()))?),
            _ => {}
        }
    }
    match host {
        Some(h) => Ok((h, port.unwrap_or(8000))),
        None => Err(format!("{} names no `host` in [DEFAULT]", conf.display())),
    }
}

fn die(msg: &str) -> ! {
    eprintln!("validate:  {msg}");
    std::process::exit(2);
}
