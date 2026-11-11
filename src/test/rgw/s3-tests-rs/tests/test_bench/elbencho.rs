use std::process::Command;
use s3_tests_rs::config::{get_config, BenchConfig};

fn env_or(var: &str, default: &str) -> String {
    std::env::var(var).unwrap_or_else(|_| default.to_string())
}

struct ElbenchoOpts {
    label: &'static str,
    threads: u32,
    num_dirs: u32,
    num_files: u32,
    file_size: String,
    block_size: String,
    bucket: &'static str,
    phases: &'static [&'static str],
    /* List the bucket before the workload.  A filesystem driver's
     * listing cache holds a bucket only once it has been listed, and
     * only then does every write update it;  a bucket never listed
     * measures the write path without the cache.  elbencho lists after
     * it writes, whatever the order of the options, so the listing is a
     * step of its own. */
    listed: bool,
}

impl ElbenchoOpts {
    fn smoke_small(bench: &BenchConfig) -> Self {
        Self {
            label: "smoke_small",
            threads: env_or("BENCH_THREADS", &bench.default_threads.to_string())
                .parse().unwrap_or(bench.default_threads),
            num_dirs: env_or("BENCH_NUM_DIRS", &bench.default_num_dirs.to_string())
                .parse().unwrap_or(bench.default_num_dirs),
            num_files: env_or("BENCH_NUM_FILES", "50")
                .parse().unwrap_or(50),
            file_size: env_or("BENCH_FILE_SIZE", "4k"),
            block_size: env_or("BENCH_BLOCK_SIZE", "4k"),
            bucket: "benchsmall1",
            phases: &["-w", "-r", "-F", "-D"],
            listed: false,
        }
    }

    fn smoke_large(bench: &BenchConfig) -> Self {
        Self {
            label: "smoke_large",
            threads: env_or("BENCH_THREADS", "4")
                .parse().unwrap_or(4),
            num_dirs: env_or("BENCH_NUM_DIRS", "1")
                .parse().unwrap_or(1),
            num_files: env_or("BENCH_NUM_FILES", "4")
                .parse().unwrap_or(4),
            file_size: env_or("BENCH_FILE_SIZE", &bench.default_file_size),
            block_size: env_or("BENCH_BLOCK_SIZE", &bench.default_block_size),
            bucket: "benchlarge1",
            phases: &["-w", "-r", "-F", "-D"],
            listed: false,
        }
    }

    fn soak_small_objects(_bench: &BenchConfig) -> Self {
        Self {
            label: "soak_small_objects",
            threads: env_or("BENCH_THREADS", "300")
                .parse().unwrap_or(300),
            num_dirs: env_or("BENCH_NUM_DIRS", "1")
                .parse().unwrap_or(1),
            num_files: env_or("BENCH_NUM_FILES", "100")
                .parse().unwrap_or(100),
            file_size: env_or("BENCH_FILE_SIZE", "512k"),
            block_size: env_or("BENCH_BLOCK_SIZE", "512k"),
            bucket: "benchsmallsoak1",
            phases: &["-w"],
            listed: false,
        }
    }

    fn soak_saturation(bench: &BenchConfig) -> Self {
        Self {
            label: "soak_saturation",
            threads: env_or("BENCH_THREADS", "300")
                .parse().unwrap_or(300),
            num_dirs: env_or("BENCH_NUM_DIRS", "2")
                .parse().unwrap_or(2),
            num_files: env_or("BENCH_NUM_FILES", "1")
                .parse().unwrap_or(1),
            file_size: env_or("BENCH_FILE_SIZE", &bench.default_file_size),
            block_size: env_or("BENCH_BLOCK_SIZE", &bench.default_block_size),
            bucket: "benchsoak1",
            phases: &["-w"],
            listed: false,
        }
    }

    fn soak_mixed(bench: &BenchConfig) -> Self {
        Self {
            label: "soak_mixed",
            threads: env_or("BENCH_THREADS", "32")
                .parse().unwrap_or(32),
            num_dirs: env_or("BENCH_NUM_DIRS", "10")
                .parse().unwrap_or(10),
            num_files: env_or("BENCH_NUM_FILES", "100")
                .parse().unwrap_or(100),
            file_size: env_or("BENCH_FILE_SIZE", "1m"),
            block_size: env_or("BENCH_BLOCK_SIZE", "1m"),
            bucket: "benchmixed1",
            phases: &["-w", "-r", "-F", "-D"],
            listed: false,
        }
    }
}

impl ElbenchoOpts {
    /// The same workload against a bucket listed first, under its own
    /// label and bucket.
    fn listed(mut self, label: &'static str, bucket: &'static str) -> Self {
        self.label = label;
        self.bucket = bucket;
        self.listed = true;
        self
    }
}

fn require_elbencho(bin: &str) {
    assert!(
        std::path::Path::new(bin).exists(),
        "elbencho not found at {bin} — install elbencho to run bench tests"
    );
}

fn run_elbencho(opts: &ElbenchoOpts) {
    let cfg = get_config();
    let bench = &cfg.bench;
    require_elbencho(&bench.elbencho_bin);

    let proto = if cfg.default_is_secure { "https" } else { "http" };
    let endpoint = format!("{proto}://{}:{}", cfg.default_host, cfg.default_port);
    let json_path = format!("/tmp/elbencho-{}.json", opts.label);

    // remove stale results
    let _ = std::fs::remove_file(&json_path);

    if opts.listed {
        /* create the bucket and list it, one thread, no objects */
        let out = Command::new(&bench.elbencho_bin)
            .arg("--s3endpoints").arg(&endpoint)
            .arg("--s3key").arg(&cfg.main_access_key)
            .arg("--s3secret").arg(&cfg.main_secret_key)
            .arg("-t").arg("1")
            .arg("-d")
            .arg("--s3listobj").arg("1")
            .arg(opts.bucket)
            .output().expect("failed to execute elbencho");
        assert!(out.status.success(), "bench::{}: listing the bucket first failed:\n{}",
                opts.label, String::from_utf8_lossy(&out.stderr));
    }

    let mut cmd = Command::new(&bench.elbencho_bin);
    cmd.arg("--s3endpoints").arg(&endpoint)
       .arg("--s3key").arg(&cfg.main_access_key)
       .arg("--s3secret").arg(&cfg.main_secret_key)
       .arg("-t").arg(opts.threads.to_string())
       /* elbencho:  -n directories per thread, -N files per directory */
       .arg("-n").arg(opts.num_dirs.to_string())
       .arg("-N").arg(opts.num_files.to_string())
       .arg("-s").arg(&opts.file_size)
       .arg("-b").arg(&opts.block_size)
       .arg("--mkdirs")
       .arg(opts.bucket)
       .arg("--jsonfile").arg(&json_path)
       .arg("--livecsv").arg(format!("/tmp/elbencho-live-{}.csv", opts.label));

    for arg in opts.phases {
        cmd.arg(arg);
    }

    eprintln!("bench::{}: threads={} dirs={} files={} size={} phases={:?} listed={}",
        opts.label, opts.threads, opts.num_dirs,
        opts.num_files, opts.file_size, opts.phases, opts.listed);

    let output = cmd.output().expect("failed to execute elbencho");
    let code = output.status.code().unwrap_or(-1);
    let stdout = String::from_utf8_lossy(&output.stdout).to_string();
    let stderr = String::from_utf8_lossy(&output.stderr).to_string();

    eprint!("{stdout}");

    if let Ok(json_str) = std::fs::read_to_string(&json_path) {
        report_json_results(opts.label, &json_str);
    }

    assert_eq!(code, 0,
        "bench::{} failed (exit {}):\n{}", opts.label, code, stderr);
}

/// elbencho's `--jsonfile` holds one JSON object per phase, a line each:
/// `phase_type`, and the results when the last worker finished under
/// `last_done`, every number a string.
fn report_json_results(label: &str, json_str: &str) {
    let num = |v: &serde_json::Value, k: &str| -> Option<f64> {
        v.get(k).and_then(|x| x.as_str()).and_then(|x| x.parse().ok())
    };
    for line in json_str.lines().filter(|l| !l.trim().is_empty()) {
        let Ok(phase) = serde_json::from_str::<serde_json::Value>(line) else {
            continue;
        };
        let op = phase.get("phase_type").and_then(|v| v.as_str()).unwrap_or("?");
        let Some(done) = phase.get("last_done") else { continue };

        let mut parts = vec![format!("bench::{label} [{op}]:")];
        if let Some(v) = num(done, "bytes/s") { parts.push(format!("{:.1} MiB/s", v / 1048576.0)); }
        if let Some(v) = num(done, "iops") { parts.push(format!("{v:.0} IOPS")); }
        if let Some(v) = num(done, "entries") { parts.push(format!("{v:.0} entries")); }
        if let Some(v) = num(done, "elapsed_time_ms") { parts.push(format!("{v:.0} ms")); }

        eprintln!("{}", parts.join("  "));
    }
}

// --- Smoke workloads (quick, suitable for CI/baseline) ---

#[test]
#[ignore = "bench: requires elbencho"]
fn bench_smoke_small_objects() {
    let cfg = get_config();
    run_elbencho(&ElbenchoOpts::smoke_small(&cfg.bench));
}

/// Small objects written to a bucket listed first, so every write
/// updates the listing cache.
#[test]
#[ignore = "bench: requires elbencho"]
fn bench_smoke_small_objects_listed() {
    let cfg = get_config();
    run_elbencho(&ElbenchoOpts::smoke_small(&cfg.bench)
                 .listed("smoke_small_listed", "benchsmalllisted1"));
}

#[test]
#[ignore = "bench: requires elbencho"]
fn bench_smoke_large_objects() {
    let cfg = get_config();
    run_elbencho(&ElbenchoOpts::smoke_large(&cfg.bench));
}

#[test]
#[ignore = "bench: requires elbencho"]
fn bench_smoke_mixed_sizes() {
    require_elbencho(&get_config().bench.elbencho_bin);
    let tiers: &[(&str, &str, u32, u32, &str)] = &[
        // (label_suffix, file_size, threads, num_files, block_size)
        ("4k",  "4k",  8, 100, "4k"),
        ("1m",  "1m",  8,  20, "1m"),
        ("64m", "64m", 4,   4, "16m"),
    ];
    for (suffix, fsize, threads, nfiles, bsize) in tiers {
        let label_str = format!("smoke_mixed_{suffix}");
        // leak to get 'static — these are test-only, bounded count
        let label: &'static str = Box::leak(label_str.into_boxed_str());
        run_elbencho(&ElbenchoOpts {
            label,
            threads: *threads,
            num_dirs: 2,
            num_files: *nfiles,
            file_size: fsize.to_string(),
            block_size: bsize.to_string(),
            bucket: "benchmixed1",
            phases: &["-w", "-r", "-F", "-D"],
            listed: false,
        });
    }
}

// --- Soak workloads (longer, stress the system under sustained load) ---

#[test]
#[ignore = "bench: requires elbencho, long-running"]
fn bench_soak_saturation() {
    let cfg = get_config();
    run_elbencho(&ElbenchoOpts::soak_saturation(&cfg.bench));
}

#[test]
#[ignore = "bench: requires elbencho, long-running"]
fn bench_soak_small_objects() {
    let cfg = get_config();
    run_elbencho(&ElbenchoOpts::soak_small_objects(&cfg.bench));
}

/// The small-object soak against a bucket listed first.
#[test]
#[ignore = "bench: requires elbencho, long-running"]
fn bench_soak_small_objects_listed() {
    let cfg = get_config();
    run_elbencho(&ElbenchoOpts::soak_small_objects(&cfg.bench)
                 .listed("soak_small_objects_listed", "benchsmallsoaklisted1"));
}

#[test]
#[ignore = "bench: requires elbencho, long-running"]
fn bench_soak_mixed_sizes() {
    let cfg = get_config();
    run_elbencho(&ElbenchoOpts::soak_mixed(&cfg.bench));
}
