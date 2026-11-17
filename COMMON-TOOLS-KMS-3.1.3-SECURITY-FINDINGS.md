# Apache Hadoop 3.1.3 — hadoop-common / hadoop-tools / hadoop-kms Findings

Source audit of `/agent/repos/hadoop-for-compile` (Hadoop 3.1.3).

**Scope:** `hadoop-common-project` (excl. already-known ConfServlet, HttpServer2 `/logs`, FileUtil.unTar, ZKFC world ACL), `hadoop-tools` (distcp, aws, azure, kafka, archive, streaming extras), `hadoop-kms` (beyond simple auth + wildcard ACLs).

**Already excluded:** CVE-2022-25168 FileUtil.unTar, ConfServlet `/conf`, HttpServer2 `/logs`, KMS simple auth + `*` ACLs, ZKFailoverController world ACL, WebHDFS client redirect CVE-2020-9492.

Only MEDIUM+ issues with a complete attacker-controlled input → impact chain are listed.

---

## 1. HIGH — KMS HA ZK cookie-signing secret stored under open ZooKeeper ACL

**Severity:** HIGH  
**Location:** `hadoop-common-project/hadoop-auth/.../ZKSignerSecretProvider.java` + `hadoop-kms/.../kms-default.xml`  
**Works on default config:** No (requires multi-KMS + `signer.secret.provider=zookeeper`, which docs recommend for HA). Defaults for that mode are unsafe (`auth.type=none`).

### Description
`kms-default.xml` tells operators to set `hadoop.kms.authentication.signer.secret.provider=zookeeper` for multiple KMS instances. The matching default `...zookeeper.auth.type` is `none`.

`ZKSignerSecretProvider.createCuratorClient` then uses Curator `DefaultACLProvider` (“open to everyone”) and stores rolling HMAC secrets at `/hadoop-kms/hadoop-auth-signature-secret` with no ZooKeeper auth.

Anyone who can reach the ZK client port can read (or overwrite) those bytes and forge `hadoop.auth` cookies as any principal. That bypasses Kerberos SPNEGO on the KMS HTTP endpoint — so this remains exploitable **after** operators “fix” simple auth.

(Same open-ACL pattern exists in `ZKDelegationTokenSecretManager` when `hadoop.kms.authentication.zk-dt-secret-manager.enable=true` and `zkAuthType=none`.)

### Impact
Forged KMS session cookies → `GET` key material / `DECRYPT_EEK` as any ACL-allowed identity → unwrap HDFS encryption-zone keys and decrypt EZ data. Undermines Kerberos-hardened multi-KMS HA.

### Attack path
1. Operators deploy ≥2 KMS nodes, set `signer.secret.provider=zookeeper`, leave `...zookeeper.auth.type=none` (shipped default for that mode).
2. Attacker opens the ZK client port and `get`s the signer znode (world ACL via `DefaultACLProvider`).
3. Attacker HMAC-forges a `hadoop.auth` cookie for a principal permitted by KMS ACLs (or `*` if ACLs were not tightened).
4. Attacker calls KMS REST (`GET` / `DECRYPT_EEK`) with that cookie; no Kerberos ticket required.

### Evidence
- `ZKSignerSecretProvider.java:366-384` — `auth.type` default `none` → `DefaultACLProvider` (“open to everyone”)
- `ZKSignerSecretProvider.java:193-195`, `312-315` — secret bytes written/read from ZK
- `kms-default.xml:212-247` — recommends `zookeeper` for multi-KMS; `auth.type` default `none`
- `ZKDelegationTokenSecretManager.java:180-182` — same open ACL when DT ZK manager uses `none`

### Default-config note
Single-node KMS keeps `signer.secret.provider=random` (not ZK). Exploit requires the documented HA cookie-sync setting, whose own defaults leave the secret world-readable.

---

## 2. HIGH — `HadoopArchiveLogs` embeds unsanitized HDFS user path names into a bash script executed via DistributedShell

**Severity:** HIGH  
**Location:** `hadoop-tools/hadoop-archive-logs/.../HadoopArchiveLogs.java`  
**Works on default config:** Partial — tool is optional, but cluster default auth is `simple`, which allows arbitrary `HADOOP_USER_NAME` values that become log-aggregation directory names.

### Description
`generateScript` builds a bash script by concatenating `AppInfo` fields into double-quoted assignments. `user` comes from `userLogPath.getName()` (the directory under the aggregated log root, i.e. the application user).

No shell escaping is applied. With `hadoop.security.authentication=simple` (default), a client can set `HADOOP_USER_NAME` to a string containing `"`, `;`, `$()`, etc. YARN log aggregation creates `/<remoteRootLogDir>/<that-name>/...`. When an operator runs `HadoopArchiveLogs`, the malicious name is written into the script and executed by YARN DistributedShell containers as the archive-logs submitter.

### Impact
Remote command execution as the principal that runs archive-logs (often an ops/service account with broad HDFS log and yarn privileges).

### Attack path
1. Cluster uses simple auth (default). Attacker exports `HADOOP_USER_NAME='x"; id; echo "'` (or similar) and submits a YARN app so log aggregation creates a user directory with that name.
2. Operator/cron runs `hadoop archive-logs` (or equivalent) over the aggregated log root.
3. `prepareWorkingDirectory` / eligibility scan sets `context.setUser(userLogPath.getName())`.
4. `generateScript` writes `user="<malicious>"` into the temp `.sh`; `runDistributedShell` launches it via YARN DistributedShell.
5. Bash interprets the injected metacharacters; attacker commands run in the archive-logs containers.

### Evidence
- `HadoopArchiveLogs.java:406-412` — user taken from directory name
- `HadoopArchiveLogs.java:511-524` — unsanitized `fw.write(context.getUser())` into bash
- `HadoopArchiveLogs.java:552-572` — script executed via DistributedShell `--shell_script`
- `UserGroupInformation.java:201-208` — simple auth honors `HADOOP_USER_NAME`
- `core-default.xml:69-70` — `hadoop.security.authentication=simple`

### Default-config note
Daemon install does not auto-run archive-logs; the bug is in the shipped tool. Exploitation needs simple auth (default) plus an operator invoking archive-logs on aggregated logs.

---

## 3. HIGH — `HarFileSystem` trusts `_index` `partName` and will open paths outside the `.har` directory

**Severity:** HIGH  
**Location:** `hadoop-common-project/hadoop-common/.../HarFileSystem.java`  
**Works on default config:** Yes (HAR support is built into common; no special flag). Impact needs a reader with broader HDFS rights than the HAR author (confused deputy), which is common for Hive/Spark/Impala external tables / service accounts.

### Description
`HarFileSystem.open` builds the underlying data path as `new Path(archivePath, hstatus.getPartName())`. `Path(Path,String)` uses `URI.resolve`. Absolute `partName` values (`/user/victim/secret`) or relative escapes (`../../user/victim/secret`) resolve **outside** the archive directory. `startIndex`/`length` then select an arbitrary byte range.

Legitimate `hadoop archives` only emits `part-N` names; an attacker who writes a forged `_index` / `_masterindex` can point “members” at any path on the same FileSystem the archive lives on. There is no check that the resolved path stays under `archivePath`.

### Impact
Confused-deputy arbitrary HDFS read: a privileged service that opens a user-supplied `har://` URI (external table, ETL input, etc.) reads attacker-chosen paths with the service identity (warehouse data, other users’ files the service can access).

### Attack path
1. Attacker uploads a directory ending in `.har` containing crafted `_masterindex` / `_index` where a file entry’s `partName` is `/user/hive/warehouse/...` or `../../<sensitive>`.
2. A privileged reader (HiveServer2, Spark driver as service user, DistCp as proxy user) opens `har://hdfs-nn/user/attacker/evil.har/fake.txt`.
3. `open` → `new Path(archivePath, partName)` resolves outside the archive; `HarFSDataInputStream` reads the chosen range via the underlying FS as the service UGI.
4. Contents are returned to the job/query; attacker recovers data they could not open directly.

### Evidence
- `HarFileSystem.java:691-700` — `open` uses `new Path(archivePath, hstatus.getPartName())`
- `HarFileSystem.java:585-591` — `partName` parsed from index with no sandbox
- `Path.java:144-158` — `URI.resolve` allows absolute/`..` child to replace parent path
- Confirmed: `hdfs://nn/user/alice/foo.har/` + `../../bob/secret` → `hdfs://nn/user/bob/secret`

### Default-config note
No config enables/disables this; any cluster that can read HAR paths is affected. Practical HIGH impact assumes a higher-privilege HAR consumer (standard in Hive/Spark deployments).

---

## 4. MEDIUM — `HostsFileReader` XML host lists parse with XXE-enabled `DocumentBuilder`

**Severity:** MEDIUM  
**Location:** `hadoop-common-project/hadoop-common/.../HostsFileReader.java`  
**Works on default config:** Partial — XML mode only when include/exclude path ends with `.xml`; file is admin-controlled but often on shared config volumes.

### Description
`readXmlFileToMapWithFileInputStream` builds a stock `DocumentBuilderFactory` with no `disallow-doctype-decl` / external-entity lockdown, then `db.parse(inputStream)`. NameNode/ResourceManager refresh include/exclude host XML through this path (sonatype-2022-5820 / HADOOP-18469 class; unfixed on 3.1.3).

### Impact
Attacker who can write the configured hosts XML (compromised config share, weak perms on NN local file, malicious config push) forces the NN/RM process to resolve external entities → local file disclosure as the daemon user and/or DoS via billion-laughs.

### Attack path
1. `dfs.hosts` / `dfs.hosts.exclude` / YARN nodes exclude is configured as an `.xml` path.
2. Attacker replaces that file with XXE payload (`<!ENTITY xxe SYSTEM "file:///etc/passwd">` etc.).
3. Admin or periodic refresh calls `HostsFileReader.refresh`.
4. Daemon parses XXE; entity data is pulled into the process (DoS or content reflected in errors/logs/host map processing).

### Evidence
- `HostsFileReader.java:131-133`, `143-150` — `.xml` → insecure `DocumentBuilderFactory.newInstance()` + `parse`
- Related offline tool: `hadoop-tools/hadoop-rumen/.../JobConfigurationParser.java:54-62` — same pattern on job history `job.xml`

### Default-config note
Default hosts files are often empty/text, not XML. Exploit needs an `.xml` hosts path plus write access to that file.

---

## Checked — no additional MEDIUM+ complete chain in scope

| Area | Result |
|------|--------|
| `ObjectInputStream` in common/tools main | Test-only or JSON `readObject` helpers (S3A commit PendingSet); no new untrusted OIS RCE in scope (YARN ZK conf store already reported elsewhere) |
| `FileUtil.unZip` / `RunJar.unJar` | Canonical-path prefix checks present; Windows symlink tar issue is known CVE-2022-26612 / already-adjacent |
| DistCp | Credential handling uses job Credentials/TokenCache; no standalone HIGH chain beyond job.xml secret exposure already covered via JHS |
| hadoop-aws S3A | URI login stripping exists; secrets-in-URI are warned; no default hardcoded keys |
| hadoop-azure | ShellDecryptionKeyProvider runs admin-configured script only; remote authorizer URLs are config, not attacker request params |
| hadoop-kafka | No secret/command-injection findings in main sources |
| Streaming `PipeMapRed` | Mapper/reducer commands are the job owner’s intentional code execution model |
| Credential provider default password `none` | New jceks created as `600`; no default world-readable provider path |
| Proxyuser | No wildcard defaults in `core-default.xml`; deny unless explicitly configured |
| `AuthenticationFilter` / HTTP signature | Default filter list is only `StaticUserWebFilter`; signature file path is per-node; RandomSigner fallback is per-process (not a forge-across-nodes primitive without shared secret leak) |
| `ShellCommandFencer` env dump of full Configuration | Secrets enter fencing child env, but reader must already be the daemon UID (`/proc/.../environ`) — not a new privilege boundary |
| KMS beyond auth/ACLs / ZK signer | CSRF not a complete standalone HIGH without cookie/auth; keystore password `none` mitigated by `600` local file |

---

## Remediation (summary)

1. **KMS HA:** Set `hadoop.kms.authentication.signer.secret.provider.zookeeper.auth.type=sasl` with keytab/principal; never leave ZK ACLs open. Same for zk-dt-secret-manager.
2. **Archive-logs:** Shell-escape or allow-list `user`/`appId`/paths before writing scripts; prefer argv arrays over `bash -c` templates.
3. **HAR:** Reject `partName` that is absolute, contains `..`, or resolves outside `archivePath` after `URI.resolve`.
4. **HostsFileReader / rumen:** Use secure XML parsers (disable DTDs/external entities) as in HADOOP-18469 / Hadoop 3.3.5+.
