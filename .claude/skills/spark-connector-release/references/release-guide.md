# StarRocks Spark Connector Release Guide

This guide explains the release model and machine prerequisites. The executable workflow
is defined in `SKILL.md` and `scripts/` one directory above.

## Published artifacts

Every supported Spark minor version has distinct Maven coordinates:

```text
com.starrocks:starrocks-spark-connector-<spark-minor>_<scala-binary>:<connector-version>
```

For example, connector `1.2.0` produces artifacts such as:

```text
com.starrocks:starrocks-spark-connector-3.5_2.12:1.2.0
com.starrocks:starrocks-spark-connector-4.1_2.13:1.2.0
```

The supported minor-version list comes exclusively from `common.sh`. The Spark profiles in
`pom.xml` provide the full Spark version, Scala binary version, and Java target.

## JDK requirements

The release uses the JDK matching the selected Spark generation:

| Spark generation listed by `common.sh` | Scala binary | Maven runtime JDK |
| --- | --- | --- |
| 3.x | 2.12 | 8 |
| 4.x | 2.13 | 17 |

Set `JAVA8_HOME` and `JAVA17_HOME` when possible. Automatic discovery is a fallback.
Preflight verifies both installations, and build verification executes `java -version`
against the selected home before each Spark build. Publication uploads those verified
bundle bytes directly and does not launch Java.

## Checkout requirement

Use a primary checkout whose `.git` is the repository directory. The Git metadata plugin
version in this project does not correctly generate `starrocks-spark-connector-git.properties`
from a linked worktree. Maven can still report success in that situation, leaving a JAR that
cannot be tied back to its release tag. The preflight rejects linked worktrees explicitly.

## Central Publisher Portal credentials

The POM uses `org.sonatype.central:central-publishing-maven-plugin` with server id `central`
to build the signed bundle. `publish.sh` sends that verified bundle directly to the Central
Publisher API. Create a Central Portal user token for an account authorized to publish
`com.starrocks`, and place it in Maven settings without committing it:

```xml
<settings>
  <servers>
    <server>
      <id>central</id>
      <username>TOKEN_USERNAME</username>
      <password>TOKEN_PASSWORD</password>
    </server>
  </servers>
</settings>
```

The default file is `~/.m2/settings.xml`. Set `MAVEN_SETTINGS` when using another file. The
API uploader can read plain token values from this entry. If the Maven password is encrypted,
export its decrypted values as `CENTRAL_USERNAME` and `CENTRAL_PASSWORD` for the release
process instead. The uploader keeps its authorization header in a mode-600 temporary file
and never writes the credentials to logs or release state.

## GPG signing

Central requires signatures for the main artifact, POM, sources, and Javadocs. Import the
designated release secret key and publish its public key to a public keyserver. The POM's
`release` profile configures Maven GPG signing with loopback pinentry, allowing the passphrase
to come from Maven settings or `gpg-agent`.

Verify the key before release:

```bash
gpg --list-secret-keys --keyid-format LONG
```

Do not print credentials or passphrases in command output, logs, or release state.

## Local no-publish rehearsal

The build-verification stage calls the repository's `deploy.sh` with
`skipPublishing=true`. Central plugin 0.8.0 still stages and builds
`target/central-publishing/central-bundle.zip`, then stops before its upload call. This
validates the release/GPG/profile path and creates the exact bundle later sent to the Portal,
without creating a deployment during rehearsal. The scripts require exactly plugin 0.8.0
for this behavior; after any plugin upgrade, review the plugin behavior and update the guard
before releasing.

## Release state

Local build evidence is stored beneath the repository's Git directory in
`spark-connector-release/v<version>/`. It includes:

- copied primary JARs and signed Central bundles from each local rehearsal;
- their SHA-256 checksums;
- tag commit and verified-version metadata;
- in-flight and completion markers containing the bundle SHA-256 and Central deployment ID;
- artifacts downloaded and verified from Maven Central.

Because this state is outside the tracked work tree, it cannot dirty the release tag. It is
machine-local evidence, not a substitute for checking Central Portal after an interrupted
publication.

## Why publication is gated

Maven Central coordinates are immutable. The safe order is:

1. create a release commit and local tag;
2. build all signed bundles from that exact commit without uploading;
3. inspect the real JAR metadata, signatures, checksums, and required shaded classes;
4. push the verified tag;
5. upload that exact verified bundle with explicit confirmation;
6. download and verify what Maven Central serves;
7. create a draft GitHub release from the verified public JARs.

## Failure recovery

| Symptom | Action |
| --- | --- |
| A JDK cannot be found | Set `JAVA8_HOME` or `JAVA17_HOME` to the required installation. |
| Preflight reports a linked worktree | Use a primary clone/checkout for the release. |
| Connector Git properties are missing | Do not publish; confirm the checkout type and rebuild. |
| GPG signing fails | Check the secret key, passphrase source, loopback support, and `gpg-agent`. |
| Central authentication fails | Regenerate/check the Portal token and namespace permission. |
| One Spark artifact publishes and the next fails | Keep the published coordinates; fix and retry only the unpublished Spark version. |
| The public JAR returns 404 immediately | Wait for mirror synchronization and rerun public verification. |
| Publication was interrupted | Read the deployment ID from `publishing-<spark>.env` and inspect that Portal deployment before any retry. |
| GitHub release creation fails | Published artifacts remain valid; fix authentication/notes and recreate only the draft. |
