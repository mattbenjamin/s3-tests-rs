# The corpus validation workflow, end to end

**Who this is for.**  A developer running the adoption validation, or
taking it over part-built.

This is the sequence and nothing else;  `README.md` beside this file
owns the stand-up's detail, and `../../tests/test_corpus/README.md` the
corpus's.

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
`scripts/validate.rs` runs `conversion` in each of the six adoption
deployments, from a fresh work directory each time;
`../../docs/validation.md` describes them and how to read the
result.
`standup.rs list` prints the steps and the settings;  `ADOPTION_WORK`
is required and has no default, because it holds the worktree, their
config store, the tree their gateway writes, and the captures.

---

## 1.  Stand NooBaa up

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

`README.md` beside this file owns the detail — why source and not a
container, why a `--detach`ed worktree, why the dependency tree is
pinned on our side, and the one patch their native addon needs.

## 2.  Generate and ingest

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

* **A failing axis is the finding, not a broken step.**  The corpus
  asserts what the nsfs driver should do, and theirs is a different
  implementation.  Every axis writes its artifacts before it asserts,
  so a failed axis still leaves a complete pair.
* **No artifacts at all is a real failure**, and the step fails on it:
  it means no axis reached a write, so read `noobaa.log` from the top.
* `capture` sets `--features fails_on_noobaa`, because three things
  their implementation does not do would otherwise be indistinguishable
  from real disagreements.  The corpus's mode check needs nothing:  it
  expects NooBaa's modes, `0660` for objects and `0770` for directories,
  which both gateways create.

The corpus itself is described by `../../tests/test_corpus/README.md`,
including its two capture formats.

## 3.  Hand off to RGW

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

## 4.  Verify

```sh
./standup.rs compare
```

The comparison is three-way, over the captures `capture/noobaa`,
`capture/rgw-unadopted` and `capture/rgw-converted`, by
`corpus-compare` (`../../src/bin/corpus-compare.rs`).  Two pairs
are compared.  In `noobaa:unadopted` a difference is an adoption
defect;  in `unadopted:converted` it is a conversion defect.  Every
difference must be accounted for by a row of `expected-differences.tsv`
beside this driver, whose header says what a row claims.  The step uses
the modality the tree was written with.  It fails on a difference no
row accounts for, and on one that breaks its row's class.

The RGW sides are captured in observe mode, which writes nothing and
records what RGW says about the objects NooBaa wrote.

---

## Which document owns what

| | |
|---|---|
| `README.md` (here) | the stand-up:  pinning, the worktree, their build, the config store |
| this file | the sequence, and what each step produces |
| `../../tests/test_corpus/README.md` | running the corpus, the capture types, adding an axis |
| `expected-differences.tsv` (here) | the differences that are accounted for;  its header says what a row claims |
| `../../docs/validation.md` | the six adoption deployments `validate.rs` runs, and reading their result |
