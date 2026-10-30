# nsfs adoption:  standing NooBaa up

`standup.rs` brings a pinned NooBaa NSFS gateway up over a data root,
so the conversion corpus can be run against *their* implementation and
the result compared with RGW.

It is the workflow driver and tests nothing itself.  The corpus is in
`../../tests/test_corpus/`.

## Why this exists

Without a tree NooBaa itself wrote, every control in the adoption
work would be composed from *our reading* of NooBaa's schema, so a
passing stage would be evidence about our reading rather than about
NooBaa's output.  The corpus's axes validate RGW against S3;  this
driver is what validates RGW against NooBaa, by having NooBaa write
the tree RGW then adopts.  `scripts/validate.rs` runs it in its
conversion cells, the adoption matrix (`../../docs/validation.md`).

## Running it

```sh
cargo +nightly -Zscript standup.rs list      # the steps, and the settings
cargo +nightly -Zscript standup.rs pin       # one step
cargo +nightly -Zscript standup.rs up        # every built step
```

**`-Zscript` is nightly-only** and there is no stable equivalent.  On a
fresh machine:  `rustup toolchain install nightly`.  Stable cargo
cannot run this file.

Every setting is an environment variable, or a line in a settings
file;  the environment wins.  The file is `--config FILE`, given before
the step (`standup.rs --config FILE pin`), else the one `ADOPTION_CONF`
names, else `adoption.conf` beside this file;  copy
`adoption.conf.sample` to start one.  The paths have no defaults,
because they are your machine's;  `list` prints every setting and which
file it read.

| | |
|---|---|
| `ADOPTION_NOOBAA_SRC` | **required for `pin`, and so for `up` and `conversion`.**  your noobaa-core clone, containing the pin.  **Read only** |
| `ADOPTION_PIN` | the commit to stand up |
| `ADOPTION_WORK` | **required.**  the worktree, the NooBaa config, the data root, logs, captures.  Under `ADOPTION_IMPERSONATE`, every directory above it must be searchable by every user, which `config` checks |
| `ADOPTION_DATA` | the tree both gateways serve;  defaults to `$ADOPTION_WORK/data` |
| `ADOPTION_PORT` | the NooBaa endpoint |
| `ADOPTION_CEPH_BUILD` | **required for `adopt`.**  the ceph build directory;  `rgw-vstart.sh` is found in the source tree its `CMakeCache.txt` names |
| `ADOPTION_SUITE` | the s3-tests-rs checkout;  defaults to two levels up from this driver |
| `ADOPTION_TIER` | which corpus tier `capture` runs |
| `ADOPTION_NOTIFY_PORT` | where `notify` listens for both gateways' notifications;  default 9101 |
| `ADOPTION_KAFKA_BROKER` | gives `notify` a Kafka half through `kafka-vstart.sh`'s broker, e.g. `127.0.0.1:9092`;  default none |
| `ADOPTION_ADOPT_BY` | what adopts the tree at `adopt`:  `command` (the default), the commands one bucket at a time;  `automatic`, the import's gateway restarted in `automatic` mode;  `promotion`, the import's gateway switched to `automatic` by the `adoption` hint, with the promotion checks.  See "Hand off to RGW" below |

## The conversion profile, step by step

Four steps, all built.  **`standup.rs conversion` runs every one of
them, in order, stopping at the first failure** -- the conversion
profile.  It runs only on a fresh work directory:  it chains the steps,
and chaining past a failure is how a tree gets destroyed, so it refuses
one that already holds a tree.

```
  1. stand NooBaa up        standup.rs up && standup.rs serve && standup.rs iam-user
                            && standup.rs store-tests
  2. generate and ingest    standup.rs capture noobaa
  3. hand off to RGW        standup.rs stop && standup.rs damage, then import and
                            adopt, capturing after each, through to completion (§3)
  4. verify                 standup.rs compare
```

Everything is driven from `scripts/adoption/standup.rs` in the suite.
`scripts/validate.rs` runs `conversion` in each of its conversion
cells, from a fresh work directory each time;
`../../docs/validation.md` describes them and how to read the
result.
`standup.rs list` prints the steps and the settings;  `ADOPTION_WORK`
is required and has no default, because it holds the worktree, their
config store, the tree their gateway writes, and the captures.

### 1.  Stand NooBaa up

```sh
cd scripts/adoption
./standup.rs up          # pin, worktree, patch, build, config
./standup.rs serve       # their endpoint, HTTP, on 6001;  IAM, HTTPS, on 7005
./standup.rs iam-user    # an IAM user in the account, through their IAM API
./standup.rs store-tests # the nsfs unit tests that read a real NooBaa store, against this one
```

`up` is the slow one — their dependency tree and their native addon,
tens of minutes on a first run.  Everything after it is seconds.

**What this produces.**  A pinned NooBaa NSFS gateway serving a tree,
with an account, an anonymous account and a bucket their own CLI
created, and an IAM user in that account their IAM API created — records *they* wrote,
rather than records composed from our reading of their schema.  Their
CLI cannot address an IAM user, which is why `iam-user` goes through
the API.  NooBaa shows the IAM user's secret once;  `iam-user` keeps it
in `$ADOPTION_WORK/iam-user-credentials`.

`store-tests` runs the four nsfs unit tests that read a real config
store (`*AStoreOnDisk*`), with `NSFS_NOOBAA_STORE` pointed at this one.
Nothing else sets that variable, so without this step they skip.  It
needs `ceph_build`, and it fails if any of the four skip rather than
run.  Like any run of the nsfs unit binary, it must not run beside
another.

"Decisions" and "NooBaa's lock does not `npm ci`" below have the
reasons:  source and not a container, a `--detach`ed worktree, the
dependency tree pinned on our side, and the one patch their native
addon needs.

### 2.  Generate and ingest

```sh
./standup.rs capture
```

Runs the conversion corpus against their endpoint and keeps what it
produced in `$ADOPTION_WORK/capture/noobaa/` — one manifest and one
inventory per axis, plus `noobaa.log` for the whole run.

**What this produces.**  Artifacts in *their* on-disk format, written
by their gateway, and a record of what S3 said about each one.  The
artifacts are the ingest;  the captures are the baseline the fourth
step compares against.

Three things about reading the output:

* **A failing axis is the finding, not a broken step**, and **no
  artifacts at all is a real failure** ("What `capture` produces"
  below);  for the second, read `noobaa.log` from the top.
* `capture` sets `--features fails_on_noobaa`, because three things
  their implementation does not do would otherwise be indistinguishable
  from real disagreements.  The corpus's mode check needs nothing:  it
  expects NooBaa's modes, `0660` for objects and `0770` for directories,
  which both gateways create.

The corpus itself is described by `../../tests/test_corpus/README.md`,
including its two capture formats.

### 3.  Hand off to RGW

```sh
./standup.rs stop                      # leaves the tree
./standup.rs damage                    # one bucket record adoption cannot carry
./standup.rs import                    # RGW over the tree;  identities imported, buckets left as NooBaa's
./standup.rs capture rgw-unadopted     # the corpus in observe mode
./standup.rs adopt                     # the same gateway;  every bucket adopted, then completion
./standup.rs capture rgw-converted     # the corpus in observe mode again
```

**Sequential, never concurrent.**  NooBaa writes, stops, then RGW opens
the same directory.  That is the production story — drain, then upgrade
— and `adopt` refuses while their endpoint is still running rather than
warning about it.

`adopt` runs `rgw-vstart.sh` with three things that matter:

| | |
|---|---|
| `--data-root` | the tree NooBaa wrote, as RGW's base path.  NooBaa's buckets are served from the paths their records give;  the base path is where a bucket created afterwards goes |
| `--noobaa-config-root` | their config store, so RGW can read what they wrote |
| `--adoption-mode command` | the driver's default, said explicitly:  nothing is adopted until asked |

Then `adopt` asks, through Admin Ops as an operator would, since the
gateway runs in command mode:  the `adoption` driver hint with
`identities=true` imports the store's accounts, users and keys;  then
each bucket NooBaa holds a record of is adopted into the shared
profile, by the hint with `bucket=<name>`, except
`corpus-coverage-copy`, which goes through `PUT /admin/nsfs/profile`.
Adopting a bucket imports its NooBaa record, writing its placement row
from the record's `path`, and rewrites its tree.  Last, the full pass,
`run=true`, has to report adoption complete, which writes the
completion record.

**What this produces.**  An RGW gateway serving the tree NooBaa wrote,
with that deployment's accounts, users, identity rows and bucket state
imported, every bucket placed by a row, and adoption complete:  NooBaa's
store is never read again.

**`damage`** sets one bucket's `owner_account`, `corpus-damaged`, to an
id no identity has, and records it in `$ADOPTION_WORK/damaged.json`.
`import` and `adopt` skip that bucket, which leaves the escape hatch
something real to do.  After adopting every other bucket, `adopt`
exercises it end to end:

1. the full pass, the `adoption` hint with `run=true`, must fail to
   complete, and the hint must report `complete:  false` — the control;
2. `adopt` grants itself the `nsfs-adoption` cap, abandons the damaged
   record through `PUT /admin/nsfs/abandoned`, with a reason, and
   checks the stage is "not imported" and the record is listed;
3. the full pass must now complete;
4. afterwards the damaged bucket is not served, its directory is still
   there, and taking the abandonment back is refused, because adoption
   is complete.

`import` on a tree whose adoption is already complete has nothing left
to do, and says so.

`adopt` also grants the imported IAM user the `nsfs=read` cap, through
Admin Ops as vstart's `[iam]` user unless `rgw_admin_key` says
otherwise:  the corpus needs it to ask what the IAM user's requests run
as.

`import` and `adopt` produce the two RGW runs that `compare` holds
against NooBaa's and each other.  `import` imports the identities
and leaves every bucket in NooBaa's format, served with its
configuration from NooBaa's configuration store
(`buckets/<name>.json`).  `adopt` adopts every bucket, which writes its
state, marks it and converts its tree;  `corpus-coverage-copy` goes
through the profile endpoint.  When
the `adoption` hint reports that a gateway already serves this tree,
`adopt` uses it rather than restarting:  a restart empties the userdb
and would re-import the identities under new ids, so the two captures
would see different owners.

**`notify`** captures what each gateway sends a notification endpoint,
for the same writes, so a consumer of NooBaa's notifications can be
held to RGW's.  It runs twice:  `notify noobaa` before `stop`, and
`notify rgw` after `capture rgw-converted`.  Each listens on
`notify_port` (9101), configures a notification on the `notify` bucket
for creates, removes and tags, and makes the same writes:  a PUT, a PUT
with user metadata, a copy, a one-part multipart upload, tags, a
DELETE, and a multi-object delete of two keys.

- **NooBaa** sends through a connection, `noobaa-cli connection add`,
  HTTP to the listener;  configuring the notification sends it a
  TestEvent, which is not kept.  `config` names `NOTIFICATION_LOG_DIR`
  in `config.json`, where the endpoint stages each event, and `notify`
  delivers them after the writes.  Not with `noobaa-cli notification`,
  which at the pin cannot be pointed at this store:  the `notification`
  type has no action, so `validate_no_extra_options()` reads
  `notification_options[undefined]` and throws on any option,
  `--config_root` included (`manage_nsfs_validations.js:182`).
  `notify` runs what that command runs (`notification_management()`)
  against this store instead.
- **RGW** sends through the topic and notification the import carried
  across from NooBaa's connection and the bucket's record (the nsfs work
  plan, I10), as each write completes;  `notify rgw` configures nothing,
  so the comparison is the translation's acceptance test.

The events are written as `capture/notify-<side>/notify.manifest`, one
subject per event, `event <key> <kind>`, a fact per field of the
record;  times, sequencers and request ids are written as present or
empty.  `compare` holds them as the pair `noobaa:rgw-notify`.

**With `ADOPTION_KAFKA_BROKER`**, `notify` has a Kafka half, through
`kafka-vstart.sh`'s broker.  `notify noobaa` also adds a Kafka
connection, `notify-kafka`, whose librdkafka settings include
`client.id`, and a second notification on the bucket naming it.  Each
side's events are read from the Kafka topic `notify-kafka` with the
broker container's console consumer, under one consumer group, into
`capture/notify-kafka-<side>`, compared as `noobaa:rgw-notify-kafka`.
RGW is then started with `--with-kafka` -- `rgw-vstart.sh` stops every
sidecar when it starts a gateway, so the broker is started again, with
an empty topic -- and with `rgw_kafka_config_allowed_keys=client.id`,
so the imported topic delivers.  `validate.rs`'s `conversion-kafka`
cell runs this.

**`ADOPTION_ADOPT_BY`** says what adopts the tree at `adopt`.
`command`, the default, is the sequence above.  The other two let
`automatic`'s queue adopt it instead (`NSFS_ADOPTION.md` §9), and both
then wait for the queue to empty and require every bucket adopted but
`corpus-damaged`, which must fail.  The escape hatch runs as above, and
completion must then come from the queue alone, within 60 s, with no
`run=true`.

- **`automatic`** restarts the import's gateway with
  `rgw-vstart.sh --restart --adoption-mode automatic`.  `--restart` runs
  no `vstart.sh` and cleans nothing, so the userdb the import wrote, and
  the account ids the unadopted capture saw, stay.  The queue starts
  with the gateway, as a deployment's does.
- **`promotion`** keeps the import's gateway in `command` mode, holds
  every bucket (`inject-hold-adoption`), and switches it to `automatic`
  with one worker through the `adoption` hint.  It then checks, on a
  bucket the worker did not take:  a read past its 1 s quantum is served
  as found, a write past it gets 503 ServiceUnavailable, and both waited;
  released, the worker takes that bucket next, ahead of the backfill;
  and a write waiting while it is released is served adopted.  The probe
  object goes in the unversioned `adoption` bucket, or `nsswitch` if the
  worker took `adoption` first, and is deleted.

Each capture runs the corpus in observe mode.  Besides the axes,
it checks the imported identities (`test_corpus/identity.rs`):  the
account's user is its root, and the IAM user has a row of its own, a
copy of its account's tuple as NooBaa holds it, and runs as that row
under `rgw_nsfs_user_identity_override` and as its account's
otherwise.  Its
capture conf carries the IAM user as `[s3 alt]` and the Admin Ops key
as `[iam]` for that.

### 4.  Verify

```sh
./standup.rs compare
```

The comparison is over the captures `capture/noobaa`,
`capture/rgw-unadopted` and `capture/rgw-converted`, and the `notify`
captures, by `corpus-compare` (`../../src/bin/corpus-compare.rs`).
In `noobaa:unadopted` a difference is an adoption defect;  in
`unadopted:converted` it is a conversion defect;  in
`noobaa:rgw-notify`, and `noobaa:rgw-notify-kafka` when there is a
Kafka half, it is one a consumer of NooBaa's notifications would
meet.  Every
difference must be accounted for by a row of `expected-differences.tsv`
beside this driver, whose header says what a row claims.  The step uses
the modality the tree was written with.  It fails on a difference no
row accounts for, and on one that breaks its row's class.

The RGW sides are captured in observe mode, which writes nothing and
records what RGW says about the objects NooBaa wrote.

## What `capture` produces

`capture` points the conversion corpus at the NooBaa endpoint and keeps
what it produced, per axis, under `$ADOPTION_WORK/capture/noobaa`:

```
coverage-keys.manifest     what S3 says -- one fact per line, sorted
coverage-keys.inventory    what the disk says -- modes, ownership, xattr names
...                        one pair per axis
noobaa.log                 the whole run
```

**The side is in the path, never in the artifacts.**  The comparison
between two gateways is `diff`, so a fact recording which gateway
produced it would differ on every line and the diff would stop saying
anything.  `capture/noobaa` here;  `capture/rgw-unadopted` and
`capture/rgw-converted` are the RGW runs after `import` and `adopt`.

**A failing axis is the finding, not a broken step.**  The corpus
asserts what *our* driver should do.  NooBaa is a different
implementation, and a disagreement is the whole reason to capture.
Each axis writes its manifest and takes its inventory before it
asserts, so a failed axis still leaves a complete pair behind;  the
run is `--no-fail-fast` and a non-zero exit is reported rather than
propagated.  What `capture` does fail on is producing **no** artifacts,
because that means no axis got as far as writing one.

Two things it sets that a normal corpus run does not:

* `CORPUS_OBJECT_MODE=any`.  The corpus checks that every object is
  published `0600`, which is a statement about the nsfs driver rather than
  about S3.  Left strict, every object NooBaa wrote is reported
  as an offender and the differences worth reading are buried.
* a generated `s3tests.conf`, at `$ADOPTION_WORK/s3tests-noobaa.conf`.  The
  suite's own config points at whatever vstart last brought up, and a
  capture that quietly ran against RGW while claiming to have
  run against NooBaa is the worst failure this step could have.  It
  carries all six identity sections the suite's loader demands, with
  placeholders for the five the corpus does not use -- deliberately
  not copies of main's credential, since one NC account serves this
  tree and an axis reaching for a second identity should fail rather
  than silently act as the first.

## Decisions

**Source, not a container.**  Containers are not helpful during
development, and the data root has to be a plain directory that RGW
can open afterwards *as the same uid* — which is the permission
surface the conversion work already spent a long time on.  A container
would pin NooBaa's whole toolchain, which is the stronger answer for a
released version we must re-check later;  it is not worth the cost
now.

**Sequential handoff against one tree.**  NooBaa writes, stops, then
RGW opens the same directory.  That is the adoption story — drain,
then upgrade — and it is what the conversion corpus already assumes.
Both gateways serving at once is a separate feature and must not be
smuggled in here.

**A git worktree at the pin, never a checkout.**  Reading noobaa-core's source
at some other commit is something this project does constantly, and
`standup.rs` must not disturb the clone's HEAD or index to do its job.
The worktree is `--detach`ed, because leaving a named branch in
someone else's clone is a side effect nobody asked for.

**Steps are individually runnable and idempotent.**  A stand-up that
only runs end to end is one you cannot debug.  Running a step twice
should be a no-op, not an error.

## The pin is a hypothesis

`08d063f86` is *"Bumping version from 5.22.0 to 5.23.0"* — the **start**
of the window in which noobaa-core carried the 5.23 version string.
Which commit in that window a shipped 5.23 corresponds to is not
confirmed.  5.23 has no branch and no tag, so there is nothing more
precise to point at.

`standup.rs pin` says so on every run, which is cheaper than someone
rediscovering it.

## Three things this driver got wrong first, now settled

Agreed and built 2026-10-08.  Kept because each was a reasoning error
rather than a typo, and the reasoning is what would recur.

**A prefix has to be a word.**  This driver's environment variables
were `IX_WORK`, `IX_PIN` and so on, and `IX` was glossed as
*interchange* wherever it appeared.  Glossing a token is not the fix:
two letters carry no lexical structure, so a reader has to be told
every time rather than being able to read it.  They are
`ADOPTION_WORK` and `ADOPTION_PIN` now, and the gloss is gone
because nothing needs glossing.  Config keys in `adoption.conf` are
bare -- `work`, `pin` -- and always were.

**Config should direct where things live, and there should be no
invented default.**  `work` is where a gigabyte of worktree goes *and*
where the config store and data root live -- the artifacts this
exercise produces.  The first version defaulted to
`~/.cache/nsfs-interchange`, which was wrong twice:  it put the
deliverable somewhere nominally disposable, and it chose the location
without being asked.  The driver takes the location from config
or from the environment and **stops** if neither supplies one.

**XDG cache semantics do not control this.**  I reached for them to
justify the default;  this work is explicit, and a location given on
purpose needs no appeal to a general convention.  The reasoning, not
just the path, was the mistake.

---

## NooBaa's lock does not `npm ci`, so the tree is pinned here

`npm ci` is the right verb:  it installs a lockfile exactly, refuses
when the lock and `package.json` disagree, and never rewrites the
lock.  `npm install` is permitted to resolve differently and to
rewrite the lock to match — which it did, the first time this driver
was written with the wrong verb, leaving a modified
`package-lock.json` in the worktree and a dependency tree that was not
the pinned one.

**But `npm ci` cannot succeed at `08d063f86`.**  That commit's
`package-lock.json` is incomplete:  it is missing `@emnapi/core` and
`@emnapi/runtime`, which `@napi-rs/wasm-runtime` requires, and carries
a stale `@emnapi/wasi-threads`.  `package.json` declares neither
`engines` nor `packageManager`, so there is no npm version to select
that would make an incomplete lock complete.  This is a property of
NooBaa's commit.

So the dependency tree is pinned **here** instead.  The first run
resolves it with `npm install` and records the result in `locks/`;
every later run copies that lock in and uses `npm ci`, which is exact.
The recorded lock is committed alongside this driver.  Their
`package-lock.json` is restored afterwards, so the worktree stays
identical to the pin, and `build` checks that before and after.

A container would not have helped.  An image bakes in whatever one
arbitrary resolution produced — the same resolution, without recording
that it made a choice.

What remains outside any lock is narrow:  `node` itself, and the
compilation of native modules against the host toolchain.

## Status

Every step is built.  `up` runs `pin`, `worktree`, `patch`, `build`
and `config`;  `serve`, `capture`, `stop` and `adopt` are each run
deliberately, because they start and stop gateways over a tree that is
the artifact, and chaining them past a failure is how a tree gets
destroyed rather than handed over.

`conversion` chains them on a fresh work directory, stopping at the
first failure, through `compare` ("The conversion profile, step by
step" above).

## Which document owns what

| | |
|---|---|
| this file | the stand-up:  the sequence and what each step produces, pinning, the worktree, their build, the config store |
| `../../tests/test_corpus/README.md` | running the corpus, the capture types, adding an axis |
| `expected-differences.tsv` (here) | the differences that are accounted for;  its header says what a row claims |
| `../../docs/validation.md` | the deployments `validate.rs` runs, and reading their result |
