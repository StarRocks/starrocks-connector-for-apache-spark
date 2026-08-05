---
name: spark-connector-release
description: >-
  Release the StarRocks Spark connector to Central Publisher Portal and Maven
  Central. Use whenever a user asks to release, publish, deploy, tag, verify, or
  create GitHub release artifacts for starrocks-connector-for-apache-spark, including
  requests mentioning deploy.sh, Spark-version artifacts, Central Portal, Maven
  coordinates, GPG signing, release candidates, or connector version bumps. This
  skill protects the irreversible publication with per-Spark JDK selection, local
  artifact verification, explicit confirmation, and post-publication verification.
---

# Release the StarRocks Spark Connector

Maven Central releases are immutable. Build and verify every intended Spark artifact
from the exact release tag before publishing anything, then verify the downloaded
artifacts after publication.

## Non-negotiable rules

- Run the release in a primary Git checkout. Do not create or use a linked worktree for
  the release build: this repository's Git metadata plugin omits the connector fingerprint
  there. `preflight.sh` enforces this.
- Treat `common.sh` as the only source of supported Spark minor versions. A profile that
  exists only in `pom.xml` is not releasable unless `common.sh` lists it.
- Use JDK 8 for Spark 3.x and JDK 17 for Spark 4.x. The scripts verify the active JDK and
  the profile's Java source/target configuration.
- Before creating the release branch, require an exact release-version row in both user-doc
  `Version requirements` tables. If either row is missing, stop and get the user's explicit
  approval; never set `CONFIRM_DOCS_VERSION` based on the agent's own judgment.
- Stop on any non-zero script result. Never bypass the verification marker or publication
  confirmation.
- Never attempt to overwrite or redeploy coordinates already published to Maven Central.
  If one version fails after others succeed, inspect Central Portal and retry only the
  unpublished version.
- Create a draft GitHub release only. A maintainer reviews and publishes the draft.

For prerequisites and troubleshooting, read
[`references/release-guide.md`](references/release-guide.md).

## Release workflow

Run all commands from the repository root. The scripts locate the repository through Git;
`CONNECTOR_REPO=/absolute/path` is also supported.

```bash
SKILL=.claude/skills/spark-connector-release

$SKILL/scripts/preflight.sh
$SKILL/scripts/prepare_release.sh 1.2.0
$SKILL/scripts/build_verify.sh
git push origin v1.2.0
$SKILL/scripts/publish.sh
$SKILL/scripts/verify_central.sh 1.2.0
$SKILL/scripts/create_github_release.sh RELEASE_NOTES.md
```

### Check the release machine

`preflight.sh` is read-only. It checks the primary-checkout requirement, clean tree,
repository identity, commands, supported Spark profiles, Scala/JDK mappings, both JDK
installations, released Stream Load SDK dependency, Central credentials, GPG key, signing
configuration, and GitHub authentication.

Prefer `JAVA8_HOME` and `JAVA17_HOME`. When unset, the scripts inspect the active Java,
standard JVM installation directories, and `update-alternatives`. They abort unless the
discovered runtime reports the exact required major version.

### Prepare the release commit and local tag

`prepare_release.sh <version>` fetches `origin/main` and first checks that its
`docs/connector-read.md` and `docs/connector-write.md` `Version requirements` tables both
contain an exact row for the requested version, including RC versions. A missing row always
requires the user's explicit confirmation: interactively answer the prompt, or only after
the user agrees, relay that decision with `CONFIRM_DOCS_VERSION=<version>` in a
non-interactive run.

The script then creates `release-<version>`, sets the top-level Maven version, commits it,
and creates local tag `v<version>`. It refuses to reuse a local or remote branch/tag and
does not push anything.

### Build and verify without publishing

`build_verify.sh [spark-minor ...]` invokes the repository's `deploy.sh` with Central
plugin property `skipPublishing=true` and the JDK required by each selected Spark profile.
This exercises the actual release profiles, GPG signing, sources/Javadocs generation, and
Central bundle construction without uploading anything. With no arguments it covers every
version from `common.sh`.

Because each dry run starts with `mvn clean`, the script copies every verified primary JAR
and signed Central bundle to local release state under the repository's Git directory. The
validation checks:

- exact filename and Maven `groupId`, `artifactId`, and release version;
- no connector `SNAPSHOT` version;
- embedded connector commit equals the release-tag commit;
- Spark/Scala artifact suffixes match the selected profile;
- connector-owned classes are present in the shaded JAR;
- the released Stream Load SDK version and its shaded classes/metadata are present;
- the bundle contains the primary JAR, sources, Javadocs, POM, GPG signatures, and all
  configured checksums, and each signature/checksum verifies locally.

The stage records SHA-256 checksums and per-version verification metadata. Push the tag
only after all intended versions pass.

### Publish through Central Portal

`publish.sh [spark-minor ...]` is the irreversible step. It requires:

- `HEAD`, local tag, and origin tag to resolve to the same commit;
- a clean checkout and matching verification state;
- intact staged JARs and signed Central bundles for every requested Spark version;
- coordinates that do not already exist on Maven Central;
- the user to type the release version, or explicitly provide
  `CONFIRM_PUBLISH=<version>` for a non-interactive run.

It uploads each already-verified signed bundle directly to the Central Publisher API with
automatic publishing; it does not rebuild the artifact. The in-flight marker records the
bundle SHA-256 and returned Central deployment ID before polling that deployment to
`PUBLISHED`. Publication stops at the first failure.

### Verify Maven Central

`verify_central.sh <version> [spark-minor ...]` downloads each published POM, primary
JAR, sources, Javadocs, and signature from Maven Central, verifies every GPG signature,
and runs the strict JAR verifier against the public bytes. It retains the verified primary
JARs and their SHA-256 values for the GitHub release.

### Prepare release notes and create the draft

Copy [`assets/release-note-template.md`](assets/release-note-template.md) and fill it from
all merged PRs between the previous release tag and the new tag. Classify each PR by its
subject prefix:

- `[Feature]` → Features
- `[Enhancement]` → Enhancements
- `[BugFix]` → BugFix
- `[Doc]` → Doc
- `[Tool]`, `[Chore]`, build, CI, release, or skill changes → Tool
- anything else → Other

Exclude only the release/version-bump commit itself. Before creating the draft, show the
user every included/excluded PR and its proposed group, then wait for explicit confirmation.

`create_github_release.sh <notes-file>` requires every `common.sh` version to have passed
public Central verification. It uploads those downloaded JARs to a draft release on
`StarRocks/starrocks-connector-for-apache-spark`; it never publishes the draft.

## Re-running after a failure

The build, publish, and Central-verification scripts accept Spark minor versions. For
example:

```bash
$SKILL/scripts/build_verify.sh 4.1
$SKILL/scripts/publish.sh 4.1
$SKILL/scripts/verify_central.sh 1.2.0 4.1
```

Use a subset only when recovering a failed or unpublished version. Local state records
successful publication and refuses to publish the same version twice. If execution was
interrupted during upload, use the deployment ID in `publishing-<spark>.env` to check the
Central Portal before retrying; absence from the public Maven mirror alone does not prove
that an upload never happened.
