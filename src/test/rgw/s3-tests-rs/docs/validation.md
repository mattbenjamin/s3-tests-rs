# Validating the nsfs driver

**Who this is for.**  A developer validating a change to RGW's nsfs
driver, or running the adoption tests:  which deployments the suite is
run against, how `scripts/validate.rs` runs them, and how to read the
result.  It assumes a Ceph build with the nsfs driver and its
`src/script/rgw/rgw-vstart.sh`.

## Before the first run:  what the machine needs

Nothing here assumes a particular machine, but several things have to
be true of yours.  Each script checks the ones it can and names what is
wrong;  the rest are listed so a failure is not a surprise.

| what | why | checked by |
|---|---|---|
| a Ceph build of the branch with the nsfs driver | the gateway and the unit suite | `validate.rs`:  the build directory has `bin/` and a `CMakeCache.txt`, which also gives it the source tree, wherever that is |
| the build on a mount without `nosuid` | `setcap` there succeeds and the kernel ignores the capabilities, so every impersonated request would fail as a denial | `validate.rs`, and `rgw-vstart.sh --with-impersonation` |
| every directory above the data roots searchable by every user (`o+x`) | impersonated identities are system users that own none of those directories;  a `0700` home directory is the usual one that blocks them | `rgw-vstart.sh --with-impersonation` for the gateway's data root;  `validate.rs` and the stand-up's `config` for a conversion's work directory |
| `sudo` from a terminal, run as yourself and not as root | `rgw-vstart.sh --with-impersonation` creates the fixture's system users and group, applies `setcap`, and owns files as them | `validate.rs` asks for the password once;  both refuse root |
| uids 70101-70110 and gid 70010 free, or already the fixture's | the fixture creates `rgwalice`, `rgwbob` and the rest at those ids, and the group `rgwfix` | `rgw-vstart.sh`, which reuses accounts that exist by name |
| port 8000 free for the gateway | `rgw-vstart.sh` stops whatever holds it (`fuser -k 8000/tcp`) | -- |
| a copy of `s3tests.conf.SAMPLE`, named by `S3TEST_CONF` or `validate.rs`'s `conf` | the suite's users and keys;  the sample already matches what `vstart.sh` creates | `validate.rs`:  the file exists |
| `cargo-nextest`, and a nightly toolchain (`rustup toolchain install nightly`) | the suite runs under nextest;  `validate.rs` and `standup.rs` are `-Zscript` files | -- |
| `python3` | `rgw-vstart.sh --with-impersonation` seeds its fixture with `nsfs-impersonate-fixture.py` | -- |
| for the conversion deployments:  `node`, a native toolchain, and a noobaa-core clone containing the pin | the stand-up builds NooBaa from source | `standup.rs pin` |
| for the bench profiles:  `elbencho` | the workloads;  its path is `elbencho_bin` in `[bench]` of `s3tests.conf` | the bench tests |

The suite itself takes `S3TEST_CONF` and nothing else from the
environment.  Against nsfs, run it with `--features
fails_on_nsfs,rgw_admin`:  the first skips what nsfs is known not to
do, the second enables the tests that use Admin Ops and driver hints.

## What a deployment can vary, and how each is tested

A setting a site chooses when it deploys the gateway is tested at one
of three levels:  as a deployment of its own, as a switch inside one,
or not by any deployment yet.

### 1.  The deployment semantics matrix

The deployment semantics matrix is the set of deployments a full
validation runs.  Each is a gateway started its own way and checked
for the semantics it gives.  Its dimensions:

| dimension | values | set by |
|---|---|---|
| impersonation | off, on | `rgw-vstart.sh --with-impersonation` |
| access model | `posix-strict`, `s3-native` | `--access-model`;  meaningful only under impersonation, so the unimpersonated deployment has one |
| the tree | fresh, or adopted from NooBaa | `rgw-vstart.sh`, or the adoption stand-up |
| user identity override | off, on | `--user-identity-override` |

Three fresh setups and three adopted ones, each with the override off
and on, make twelve deployments;  four more follow the table:

| deployment | started as | runs |
|---|---|---|
| `posix-strict` | impersonated, `posix-strict` | `baseline`, then the switches below |
| `s3-native` | impersonated, `s3-native` | the same |
| `plain` | not impersonated, `--bucket-profile strong` | `baseline` and the librgw suites, which need `strong` and no impersonation |
| `conversion` | the stand-up, not impersonated | the adoption corpus |
| `conversion-posix-strict` | the stand-up, impersonated, `posix-strict` | the adoption corpus, and the stand-up's `groups`, `nsswitch` and `copy` steps |
| `conversion-s3-native` | the stand-up, impersonated, `s3-native` | the same |
| each with `-override` | the same, with `--user-identity-override` | the same;  tests that resolve an account user expect its own row |

| deployment | started as | runs |
|---|---|---|
| `conversion-automatic` | the stand-up, restarted in `automatic` after `import` | the adoption corpus, adopted by `automatic`'s queue |
| `conversion-promotion` | the stand-up, switched to `automatic` by the `adoption` hint | the adoption corpus, and the promotion checks |
| `kafka` | not impersonated, `--with-kafka` | `test_sns` built with `has_kafka`, against the broker it starts;  needs podman |
| `conversion-kafka` | the stand-up, with `kafka-vstart.sh`'s broker | the adoption corpus, and the `notify` steps' Kafka half;  needs podman |

The four run only when `--cells` names them.  The deployments other
than `plain` run with the default bucket profile, `shared`.

### 2.  Switches tested inside a deployment

Three settings are daemon-wide and are switched by a driver hint for
the length of one profile.  Each profile runs after `baseline` in each
impersonated fresh deployment, against the same gateway:

| setting | profile | what it checks |
|---|---|---|
| the access model | `access-model-switch` | that the bytes follow the filesystem under the model, then switches to the other and checks the same read is served |
| `rgw_nsfs_dynamic_supplemental_groups` | `dynamic-groups`, then `dynamic-groups-membership` after `gpasswd -d` | that an identity with no group list takes its Unix groups, and follows a change of membership |
| `rgw_nsfs_role_identity_override` | `role-identity` | that a role given a row runs as it, and its new buckets follow it, and otherwise as its account |

So each is tested under both access models and both settings of the
user override, without a deployment of its own.  Each test puts the
gateway's own setting back when it finishes.  They are kept out of
`baseline` because each changes what every test beside it sees;
within `-P all` each runs alone, holding every test thread.

`access-model-switch` checks nothing without impersonation:  it prints
that impersonation is off and passes, so a green run there is not
evidence.

### 3.  Settings the suite reads but no deployment varies

The suite asks the gateway for these (`GET /admin/features`), so it
runs correctly under either value, but no deployment in `validate.rs`
sets the non-default one.  To test one, start a gateway with it and
run the profiles by hand, or add a deployment:  `validate.rs`'s
`CELLS` table is the whole definition, and adding one is an edit
there.

| setting | to set it | the suite reads |
|---|---|---|
| non-md5 etag | `--with-non-md5-etag` | `non_md5_etag` |
| rename disabled | `-o rgw_nsfs_enable_rename=false` | `can_rename`, `rename_enabled` |
| the io_uring engine | `-o rgw_nsfs_io_engine=io_uring`;  the default is `sync` | -- |
| GPFS | `--gpfs`, with `--clone` and `--lwe` for clone and locking, on a GPFS filesystem | `strategy`, `shares_extents` |
| fast lifecycle | `--with-lifecycle`, or `validate.rs --lifecycle`;  the lifecycle tests run in their own profile | `lc_threads`, `lc_debug_interval` |
| inotify | `--with-inotify` | -- |
| automatic adoption | `--adoption-mode automatic`;  the stand-up runs `command` | -- |
| the posix backend | `--store posix`, with `--features fails_on_posix` | -- |

A setting the suite does not read (`--`) is run against the same
expectations as the default, so a test that depends on it fails
rather than adapting.

## Running the matrix:  `scripts/validate.rs`

`validate.rs` builds, runs the nsfs unit suite, then starts a gateway
per deployment and runs that deployment's profiles against it.  It
asks for your `sudo` password once and keeps the ticket alive, and it
applies every `setcap` itself, so a relink between runs needs nothing
from you.  Run it from your own terminal, as yourself, from the
suite's root.  It is a nightly `-Zscript`:  `rustup toolchain install
nightly` on a fresh machine.

**Tell it your paths once, in a config file.**  It has no defaults,
because every one of them is a property of your machine.  As the suite
takes its own config from `S3TEST_CONF`, `validate.rs` takes this one
from `VALIDATE_CONF`;  `--config FILE` overrides that, and with
neither it reads the per-user `~/.config/s3-tests-rs/validate.conf`
(under `$XDG_CONFIG_HOME` if that is set).  A file named by
`VALIDATE_CONF` or `--config` must exist;  the per-user one is read if
it does.  One `key = value` per line, `#` for comments, `~/` for your
home directory:

```
build          = /path/to/ceph/build
conf           = /path/to/s3tests.conf
# the conversion deployments
adoption_cache = /path/to/adoption-cache
noobaa_src     = /path/to/noobaa-core
work_root      = /path/to/work
# optional:  every suite's output appended to one file
parallel_log   = /tmp/s3test-parallel.log
```

| key | option | what it is | needed |
|---|---|---|---|
| `build` | `--build DIR` | the ceph build directory | always |
| `conf` | `--conf FILE` | your `s3tests.conf` | always |
| `adoption_cache` | `--adoption-cache DIR` | where the built NooBaa worktree is, below | for a conversion deployment |
| `noobaa_src` | `--noobaa-src DIR` | your noobaa-core clone, read only | for a conversion deployment |
| `work_root` | `--work-root DIR` | where the conversion deployments' work directories go;  every user must be able to search it | for an impersonated conversion deployment whose run directory is not reachable |
| `parallel_log` | `--parallel-log FILE` | every suite's output, appended | never |

An option overrides the file.  A key that is needed and missing is
refused, naming the key and the file;  an unknown key is refused too,
so a misspelling is not silently ignored.  Without `work_root` the
work directories go under the run directory, `target/validate/` in the
suite, which is fine when the checkout is somewhere every user can
search and refused before anything starts when it is not.

**The adoption cache is built once,** by the stand-up's first four
steps with `ADOPTION_WORK` set to the cache directory.  It then holds
`noobaa-<pin>/`, which every conversion deployment links into its own
fresh work directory instead of rebuilding NooBaa:

```bash
cd scripts/adoption
for step in pin worktree patch build; do
  ADOPTION_NOOBAA_SRC=/path/to/noobaa-core \
  ADOPTION_WORK=/path/to/adoption-cache ./standup.rs $step || break
done
```

The build step is the slow one:  NooBaa's dependency tree and its
native addon.

**The full matrix** is every deployment, named:

```bash
./scripts/validate.rs \
  --cells posix-strict,s3-native,plain,posix-strict-override,s3-native-override,plain-override,conversion,conversion-posix-strict,conversion-s3-native,conversion-override,conversion-posix-strict-override,conversion-s3-native-override,conversion-automatic,conversion-promotion,kafka,conversion-kafka
```

That is the run that says a change is done.  **With no `--cells` you
get only the six fresh deployments and the unit suite**:  the
conversion deployments run only when `--cells` names them, so a clean
default run says nothing about adoption.

**A partial run** is for iterating, and validates only what it ran:

| to | run |
|---|---|
| check a driver change quickly | no `--cells`:  the unit suite and the six fresh deployments |
| run the adoption matrix alone | `--no-unit --cells conversion,conversion-posix-strict,conversion-s3-native,conversion-override,conversion-posix-strict-override,conversion-s3-native-override,conversion-automatic,conversion-promotion,conversion-kafka` |
| repeat one deployment | `--cells posix-strict` |
| re-run without rebuilding | add `--no-build`;  the binaries in `--build` are used as they are |
| run one test in every deployment that has it | `--test 'test(=module::test_name)'`, which replaces each deployment's profiles with that filter |
| see what would run | `--dry-run`, which prints every command and runs none |

`--no-librgw` skips the librgw suites in `plain`, and `--lifecycle`
starts the fresh deployments with `--with-lifecycle`.

**Reading a run.**  Each run writes `target/validate/<timestamp>/`:  a
directory per deployment with one log per step, and `summary.txt`, one
line per step with `ok`, `KNOWN` or `FAIL`.  `KNOWN` means every step
passed and a defect already on the expected-difference list remains
(below).  The exit status is 0 for all `ok`, 3 for `KNOWN`, 1 for any
`FAIL`.  Every label names the user identity override, and the
gateway's startup line is checked against it, so a deployment cannot
pass while running the wrong setting.  Start at `summary.txt`, then
open the step's log.

**One run at a time, and no build during one.**  Every fresh
deployment starts on the same port, and the run uses the binaries in
the build tree as it finds them at each step.

## The adoption matrix, and reading its result

Adoption is RGW serving a tree NooBaa wrote.  The adoption matrix is
the nine conversion deployments.  Each runs
`scripts/adoption/standup.rs conversion` from a fresh work directory:
NooBaa writes a tree, the conversion corpus captures it three times,
and the captures are compared.  `scripts/adoption/README.md`
describes the stand-up, and its steps in sequence.

The nine differ in how RGW is started over the tree, or in what adopts
it, and so in what they show:

| deployment | RGW is started | what it adds |
|---|---|---|
| `conversion` | as the gateway's own identity | the format:  adoption and conversion of every corpus shape, with ownership out of the picture |
| `conversion-posix-strict` | impersonated, `posix-strict` | that the adopted tree is served as its owners, as NooBaa serves it;  the `groups`, `nsswitch` and `copy` steps run |
| `conversion-s3-native` | impersonated, `s3-native` | the same under the other access model |
| each with `-override` | as above, `--user-identity-override` | an IAM user NooBaa serves from its own copy is served the same;  `copy rgw` expects the user's own row instead of the account's |
| `conversion-automatic` | as `conversion`;  `adopt` restarts it in `automatic` | that the queue adopts the whole store from startup, fails the damaged record, and observes completion after its abandonment |
| `conversion-promotion` | as `conversion`;  `adopt` switches it to `automatic` with one worker | the request path:  a held bucket's read served as found, its write 503, the promoted bucket taken next |
| `conversion-kafka` | as `conversion`, with a Kafka broker | NooBaa's Kafka connection carried across as a topic, its events compared with RGW's |

**Four captures, and a comparison for each pair.**  The corpus runs
against NooBaa (`capture/noobaa`), against RGW after `import` with
every bucket still in NooBaa's format (`rgw-unadopted`), and after
`adopt` has converted them (`rgw-converted`);  the `notify` steps
capture the events each gateway sends for the same writes
(`notify-noobaa`, `notify-rgw`, and `notify-kafka-*` in
`conversion-kafka`).  `compare` holds:

- `noobaa:unadopted` -- what RGW serves from NooBaa's tree before
  touching it.  A difference nothing accounts for is an **adoption
  defect**.
- `unadopted:converted` -- what conversion changed.  A difference
  nothing accounts for is a **conversion defect**.
- `noobaa:rgw-notify`, and `noobaa:rgw-notify-kafka` -- NooBaa's event
  records against those RGW sends through what the import carried.  A
  difference nothing accounts for is a defect in the record or in the
  notification translation.

Each pair prints one line:

```
== adoption (noobaa:unadopted):  162 differences, 157 expected, 0 breaking their row, 0 unexpected, 5 known defects, 0 unobserved
```

The counts partition the differences:

| count | means | verdict |
|---|---|---|
| expected | a row in `expected-differences.tsv` accounts for it, and its class holds | passes |
| breaking their row | a row names it, and the difference violates the row's class -- say `equal`, and the values differ | fails |
| unexpected | no row names it | fails |
| known defects | a `known` row names it:  a defect recorded and not yet fixed | `KNOWN`, never a pass |
| unobserved | one side recorded the value as `<unobserved>`, a place it could not look -- under impersonation, bucket configuration the corpus's filesystem half cannot read | neither |

**`scripts/adoption/expected-differences.tsv`** is the list of
differences that are accounted for.  Each row names a pair, a modality
(`md5-on`, `md5-off`, or both), an artifact, a subject, a key, a
class, and a required reason.  The classes are statements about the
pair:  `equal`, `recognize:<normalizer>`, `differs`, `vanishes`,
`appears`, `absent`, `filtered`, and `known`.  The file's header is the
reference.  A row is a claim someone has to defend, and is keyed on a
field, never on a literal value from NooBaa's side.

**After each run `compare` lists the rows that matched nothing.**  A
guard row (`equal`, `filtered`) is meant to match nothing.  Any other
row that turns up there is stale.

**The verdict.**  `corpus-compare` exits 0 when every difference is
expected, 3 when the only failures are known defects, 1 for anything
unexpected or breaking;  `validate.rs` reports these as `ok`, `KNOWN`
and `FAIL`.  At the time of writing every conversion deployment is
`KNOWN`:  five facts of one key longer than RGW's LMDB listing cache
can hold.

**When a deployment fails:**

1. Read `target/validate/<stamp>/<deployment>/conversion.log` for the
   `==` lines and the difference lines under them;  each names the
   artifact, subject, key and both values.
2. The pair says where the defect is:  adoption or conversion.  The
   step that failed, if one did, is named in `the conversion profile
   stopped at`.
3. The captures are under the deployment's `work/capture/`, one
   directory per capture with a log beside each;  `diff` the two
   artifacts the line names.
4. A difference is a measurement, not a verdict on the test.  Settle
   what the behaviour should be, from NooBaa's code at the pin, before
   changing the code or adding a row.  A row added to make a run pass,
   without a reason that would convince someone else, defeats the
   comparison.
