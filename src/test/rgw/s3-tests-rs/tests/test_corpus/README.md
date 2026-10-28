# The conversion corpus

A modality of this suite, not a separate tool.  It writes a set of S3
artifacts, records what S3 says about them, records what the filesystem
says about them, and asserts the properties that hold of any S3
implementation.

Comparing two captures is a separate step, `corpus-compare`
(`src/bin/corpus-compare.rs`);  what this produces is its input.

This file is the interface, and the reasoning behind its shape.  The
end-to-end validation workflow this feeds — stand NooBaa up, generate
and ingest, hand off to RGW, verify — is
`../../scripts/adoption/WORKFLOW.md`.

**Why a modality of the suite.**  A declarative spec of what to write
would need its own completeness argument, with nothing behind it but
that we wrote it;  the suite is already the enumeration of S3 features,
maintained by work that happens anyway.  Nor is it the suite run with
cleanup off:  a test leaves the artifacts its assertion needed, so
coverage of on-disk shapes would be accidental, and its names are
random, which makes a capture impossible to diff.  The suite's
primitives are the asset;  its test cases are not.

---

## Running it

```sh
S3TEST_CONF=/path/to/s3tests.conf \
CORPUS_OUT=/tmp/corpus \
CORPUS_DATA_ROOT=<build>/dev/rgw/nsfs/root \
  cargo test --test test_corpus -- --test-threads=1
```

| variable | meaning |
|---|---|
| `CORPUS_TIER` | `coverage` (default), `interaction`, `scale`.  Axes return early for a tier they do not implement |
| `CORPUS_OUT` | where captures are written.  Default `target/corpus`, which is build output |
| `CORPUS_DATA_ROOT` | the gateway's data root.  **Set only when the run is local to it.**  Without it the disk half is not taken and the S3 half still works |

`--test-threads=1` is not required but makes the per-axis progress
readable.  The axes use their own buckets and do not collide.

**Run it against a clean tree.**  Names are derived from parameters so
that manifests diff, which means a second run writes the same keys to
the same buckets.  Use `rgw-vstart.sh --clean`.  One axis — bucket
configuration — is explicitly idempotent because it clears each
configuration before applying it, but that is a property of that axis
and not of the corpus.

---

## What writes the artifacts

Every artifact here is written through S3, by whichever gateway the
run is pointed at.  That is the only generator this has.

A second one is designed and **not built**:  artifacts written
straight to the filesystem, which no gateway produced, plus bulk for
the scale tier.  Its purpose is provenance an S3 request cannot
supply — a directory object in their layout, a name a gateway would
reject.

---

## The two captures

### `Manifest` — what S3 says

```rust
let mut m = Manifest::new();
m.object("key", "size", "128");
m.bucket(&b, "versioning", "Enabled");
std::fs::write(path, m.render())?;
```

One fact per line, `scope \t name \t field \t value`, sorted bytewise
and deduplicated, every component percent-encoded so a value can never
introduce a tab or a newline.  The comparison is `diff`, and a
difference names the fact rather than the object.

`fact()` is the general form; `object()` and `bucket()` are the two
scopes in use.

### `Inventory` — what S3 cannot say

```rust
let mut inv = Inventory::new();
inv.walk(&bucket_dir)?;
std::fs::write(path, inv.render())?;
let problems = inv.check_object_modes();
```

Mode, type, uid, gid, size and **extended attribute names** for every
entry under a root, same line discipline.  No timestamps and no inode
numbers: they differ on every run and carry nothing the comparison
wants.  Names and not values:  the values are what the manifest
covers, and the inventory answers what S3 cannot -- whether anything
NooBaa-spelled remains, and whether a created file got the ownership
and mode it should have.

**Structured values are recorded as a digest.**  A bucket policy,
lifecycle rules or CORS are JSON whose formatting carries no meaning,
so `policy.sha256` over the canonical form compares exactly and says
"go look" when it differs.

**What the format cannot compare:  version ids across two trees.**
They are inode-derived, so they compare exactly within one tree --
NooBaa writes it, RGW reads it, RGW converts it, which is the
comparison this is for -- and would need masking to compare two trees
generated separately.

Three defects this half found that the manifest structurally could
not: a zero-length object at mode 0700, every completed multipart
object at 0700, and conversion leaving NooBaa-spelled attributes on
version entries.

Most axes want the one-call form, which writes the file and returns
what the disk-level invariants object to:

```rust
let problems = inventory::capture(&b, &dir, &format!("{tier}-keys"));
```

`capture()` returns an empty list when `CORPUS_DATA_ROOT` is unset — a
remote run must not fail an invariant it cannot measure.

**`walk()` records what is *under* a root, not the root itself.**  A
bucket's configuration lives on the bucket directory, so that needs
`root_xattr_names()` or `root_xattr_digest()`.  Prefer the digest form
when asking whether something was persisted: a configuration folded
into an existing blob adds no attribute *name*, and a names-only
comparison reads that as "stored nowhere".

## Three tiers

**Coverage**:  one artifact per value of every axis, so each reader
meets each shape at least once.  Minutes;  the default.

**Interaction**:  pairwise over the shape axes, because defects
cluster in pairs, plus the named shapes below, which a generator would
not pick.

**Scale**:  a multiplier over one representative shape, not over the
matrix.  Its job is measurement -- the cost of a first listing, of a
walk, of conversion -- and a million of the cheapest object teaches
that;  a million of every shape teaches nothing more for a thousand
times the runtime.

## Named shapes, and what is carried forward

Cases a generator would not pick, named because there is evidence they
matter:

- **A key that is both an object and a prefix**:  `foo` and `foo/bar`.
  NooBaa's layout produces it as a directory object;  nsfs cannot write
  it (409 `KeyPathConflict`) but adopts and serves it, which no unit
  test can show.
- **Reserved names**:  `.folder`, a key under `.versions/`, a name
  starting `.noobaa-nsfs`.  NooBaa's listing hides those three and
  nothing else, two of them as defects;  nsfs reserves its own
  scaffolding names, and the corpus asserts nsfs's behaviour, not
  NooBaa's.  The key `/` is the root object in both.
- **Degenerate buckets**:  empty;  holding only `.versions/` entries;
  holding only an upload in flight.

**A filesystem object with no S3 meaning is carried forward**, left on
disk untouched.  Conversion classifies each entry without following
links and enqueues only directories and regular files, so a fifo, a
socket or a device node is skipped and never opened.  A symbolic link
is not an object in nsfs:  it is not listed, and adoption reports each
one.

## The parameter space

`names.rs` and `shapes.rs` hold the axes.  Each is an enum with a
`label()` or `key()`, and most carry a `rationale()` saying why that
value is in the set — a named value should be able to explain itself.

| | |
|---|---|
| `KeyClass` | 15 key shapes: reserved names, the length ceiling, a key that is also a prefix |
| `SizeClass`, `MetaClass`, `TagClass` | object body, user metadata, tags |
| `PartCount`, `PartPattern`, `UploadState` | multipart.  **512 and 513** are named, not generated |
| `CopySource`, `CopyDest`, `Directive` | copy |
| `LockClass` | object lock, and `RETAIN_UNTIL_SECS` |

The `*_COVERAGE` constants are the coverage tier: every value of every
axis at least once, **not** the cross product.  The product is the
interaction tier's job.

`bucket(tier, label)` composes the bucket name, so every bucket this
produces is identifiable as corpus output.

---

## Adding an axis

1. Add the shape enum to `shapes.rs`, with a `label()` and a
   `rationale()` where the value needs defending, plus a
   `*_COVERAGE` constant.
2. Add `<axis>.rs` with one `#[tokio::test] async fn corpus_coverage_<axis>()`.
3. Declare it in `mod.rs`.
4. Return early unless `CORPUS_TIER` is a tier you implement.
5. Record both captures; assert reference-free properties only.

### Write the axis to exercise what you are suspicious of

The point is not to produce artifacts; it is to reach the code that
handles them.  Several defects lived behind a *warm listing cache* — a
listing that misses refills from the store and derives every field
correctly, so it reads the same whatever the write path put in the
cache.  The multipart and copy axes therefore list their buckets
*before* their writes complete, deliberately.

An axis that passes has only tested the branches it reached.  The copy
axis passed for a while without a versioned destination, and the branch
it was written to settle was never executed.

### Assert only what holds of any S3 implementation

There is no golden manifest and there will not be one: it would record
what this implementation happened to do, enshrining the first run's
defects as the expected result.  Assert instead that a key the gateway
accepted is listed, that a version id it reported is findable, that a
size matches the bytes written, that nothing appears twice.

The gateway's error code is recorded like any other **fact**, and the
axis carries on.  Only a broken invariant fails the axis — a listing
that disagrees with what was written, a version id that cannot be
found.
Whether a backend supports a feature is stated by the suite's
`fails_on_<backend>` gates, not by this corpus.

### Two cautions, both learned the hard way

**Check the request before believing the answer.**  This corpus has
produced two false positives, and both were the instrument's fault.
The logging step created a target bucket with no policy, got the
correct 403 that S3 requires, and reported the feature as absent.  A
names-only attribute diff reported request-payment as unstored when it
folds into an existing blob.  When an axis finds a defect on its first
run, re-read what it asked for.

**Prefer fixed values to varying ones.**  A moving timestamp or a
generated id makes every manifest differ from the last for a reason
that is not a change in behaviour.  `RETAIN_UNTIL_SECS` is a fixed
date for this reason.  Where a value cannot be fixed — an
inode-derived version id, a retention date computed from an object's
own mtime — record it as a fact and do not compare it.

---

## What this is not

Two things are **deliberately** absent.  Both have a revisit
condition, so neither is an oversight to correct by adding one.

**Concurrency.**  No axis generates concurrent traffic.  A corpus is a
static artifact set, and names derived from parameters are what make
two runs against two gateways diff line for line;  mixing concurrent
traffic in would cost exactly that.  Conversion *while serving* does
need it, and it belongs to a test of conversion under load rather
than here.

**A golden manifest.**  There is no expected copy in the repository to
diff against, and there should not be:  it would record what this
implementation happened to do, enshrining whatever defects were in the
first run's output as the expected result.  What an axis asserts
instead are properties true of any S3 implementation, and comparisons
are between two captures — never against a file in the tree.

---

## What it has found

Every one by an invariant or by reading a capture, none by a golden
file;  each is fixed unless it says otherwise.

- a key over the listing cache's 510 bytes was written and never
  listed;  it is refused now (`KeyTooLongError`), and one already in a
  NooBaa tree empties its bucket's listing -- the known defect, fixed by
  a larger cache key size
- a key colliding with a reserved name was accepted
- `foo/bar` then `foo/bar/xyzzy` answered 500, now 409
- conversion's sentinel was created more permissive than any object,
  and files were created `0700` and narrowed only on the data path --
  found by the inventory's mode column
- conversion never descended into `.versions/`, so a NooBaa delete
  marker stopped reading as one and the object it deleted came back
- on a warm listing cache a demoted version listed as zero-length,
  with no ETag and no owner -- found only by running the corpus twice
  against one gateway
- a copy into an unversioned bucket never reached the listing cache
- a bucket's default retention was not applied at PUT
- an observe-mode capture wrote to the tree, through the bucket
  configuration axis

## Where a bucket's configuration is stored

Measured by `buckets.rs`, which applies each configuration alone and
reads the bucket directory's attributes before and after.  It is what
a conversion has to carry across.

| configuration | what it writes on the bucket |
|---|---|
| tagging | `user.nsfs.rgw.x-amz-tagging` |
| CORS | `user.nsfs.rgw.cors` |
| policy | `user.nsfs.rgw.iam-policy` |
| lifecycle | `user.nsfs.rgw.lc` |
| default encryption | `user.nsfs.rgw.sse-s3.policy` |
| request payment | `user.nsfs.bucket_info`, modified in place |
| logging | `user.nsfs.rgw.logging`, `user.nsfs.rgw.logging-mtime` |

Versioning and object lock are measured by their own axes.  Logging
needs a target bucket that carries a policy, not only one that exists:
S3 grants the log-delivery service `s3:PutObject` on it, and RGW
enforces that, so a bare target is a correct 403.  nsfs stores the
configuration and does not deliver logs.
