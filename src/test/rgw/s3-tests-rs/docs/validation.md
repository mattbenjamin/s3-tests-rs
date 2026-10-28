# Validating the nsfs driver

**Who this is for.**  A developer validating a change to RGW's nsfs
driver, or running the adoption tests:  which deployments the suite is
run against, how `scripts/validate.rs` runs them, and how to read the
result.  It assumes a Ceph build with the nsfs driver and its
`src/script/rgw/rgw-vstart.sh`.

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
and on, make twelve deployments:

| deployment | started as | runs |
|---|---|---|
| `posix-strict` | impersonated, `posix-strict` | `baseline`, then the switches below |
| `s3-native` | impersonated, `s3-native` | the same |
| `plain` | not impersonated, `--bucket-profile strong` | `baseline` and the librgw suites, which need `strong` and no impersonation |
| `conversion` | the stand-up, not impersonated | the adoption corpus |
| `conversion-posix-strict` | the stand-up, impersonated, `posix-strict` | the adoption corpus, and the stand-up's `groups`, `nsswitch` and `copy` steps |
| `conversion-s3-native` | the stand-up, impersonated, `s3-native` | the same |
| each with `-override` | the same, with `--user-identity-override` | the same;  tests that resolve an account user expect its own row |

The deployments other than `plain` run with the default bucket
profile, `shared`.

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

**Point it at your paths first.**  Its defaults are one developer's
machine:

| option | default | what it is |
|---|---|---|
| `--build DIR` | `/lv2tb/ceph-cp/build` | the ceph build directory |
| `--conf FILE` | `~/dev/rgw/s3tests.conf` | your `s3tests.conf` |
| `--adoption-cache DIR` | `/lv2tb/nsfs-interchange` | where the built NooBaa worktree is, for the conversion deployments |

**The adoption cache is built once,** by the stand-up's first four
steps with `ADOPTION_WORK` set to the cache directory.  It then holds
`noobaa-<pin>/`, which every conversion deployment links into its own
fresh work directory instead of rebuilding NooBaa:

```bash
cd scripts/adoption
for step in pin worktree patch build; do
  ADOPTION_NOOBAA_SRC=$HOME/dev/noobaa-core \
  ADOPTION_WORK=$HOME/nsfs-adoption-cache ./standup.rs $step || break
done
```

The build step is the slow one:  NooBaa's dependency tree and its
native addon.

**The full matrix** is every deployment, named:

```bash
./scripts/validate.rs --build $BUILD --conf $CONF --adoption-cache $CACHE \
  --cells posix-strict,s3-native,plain,posix-strict-override,s3-native-override,plain-override,conversion,conversion-posix-strict,conversion-s3-native,conversion-override,conversion-posix-strict-override,conversion-s3-native-override
```

That is the run that says a change is done.  **With no `--cells` you
get only the six fresh deployments and the unit suite**:  the
conversion deployments run only when `--cells` names them, so a clean
default run says nothing about adoption.

**A partial run** is for iterating, and validates only what it ran:

| to | run |
|---|---|
| check a driver change quickly | no `--cells`:  the unit suite and the six fresh deployments |
| run the adoption matrix alone | `--no-unit --cells conversion,conversion-posix-strict,conversion-s3-native,conversion-override,conversion-posix-strict-override,conversion-s3-native-override` |
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
the six conversion deployments.  Each runs
`scripts/adoption/standup.rs conversion` from a fresh work directory:
NooBaa writes a tree, the conversion corpus captures it three times,
and the captures are compared.  `scripts/adoption/WORKFLOW.md` is the
sequence of steps, and `scripts/adoption/README.md` the stand-up
itself.

The six differ in how RGW is started over the tree, and so in what
they show:

| deployment | RGW is started | what it adds |
|---|---|---|
| `conversion` | as the gateway's own identity | the format:  adoption and conversion of every corpus shape, with ownership out of the picture |
| `conversion-posix-strict` | impersonated, `posix-strict` | that the adopted tree is served as its owners, as NooBaa serves it;  the `groups`, `nsswitch` and `copy` steps run |
| `conversion-s3-native` | impersonated, `s3-native` | the same under the other access model |
| each with `-override` | as above, `--user-identity-override` | an IAM user NooBaa serves from its own copy is served the same;  `copy rgw` expects the user's own row instead of the account's |

**Three captures, two comparisons.**  The corpus runs against NooBaa
(`capture/noobaa`), against RGW after `import` with every bucket still
in NooBaa's format (`rgw-unadopted`), and after `adopt` has converted
them (`rgw-converted`).  `compare` holds two pairs:

- `noobaa:unadopted` -- what RGW serves from NooBaa's tree before
  touching it.  A difference nothing accounts for is an **adoption
  defect**.
- `unadopted:converted` -- what conversion changed.  A difference
  nothing accounts for is a **conversion defect**.

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
