# Apache Hadoop 3.1.3 — DistCp / cloud FS / streaming / FileUtil / identity findings

Source audit of `/agent/repos/hadoop-for-compile` (Hadoop **3.1.3**).

**Scope:** DistCp; S3A/WASB/ABFS/OSS/Swift/Kafka; streaming/pipes; archives (beyond Har partName); FileUtil unTarUsingJava/unZip (CVE-2022-26612); SFTP/FTP; ViewFs; Fuse-dfs; ProxyUsers; LdapGroupsMapping; Configuration XXE; JavaSerialization; WritableRpcEngine/ObjectWritable; ZKDelegationTokenSecretManager; Fair/Leveldb scheduler XML; JobSubmitter cache/job.xml.

**Already excluded (reported elsewhere):** FileUtil.unTar cmd injection; Unix tar traversal; ZK deser CVE-2021-25642; WebHDFS client redirect; ConfServlet; /logs; HttpFS; NFS; KMS wildcard; ZKRM/FileSystem RM tokens; HarFileSystem partName; HadoopArchiveLogs bash injection; ZKSignerSecretProvider; HostsFileReader XXE; ShuffleHandler body MAC (MEDIUM); NM LocalResource http SSRF.

---

## Verdict

**No new MEDIUM+ findings with a complete remote attacker → impact chain** in this scope beyond issues already reported or marked adjacent.

---

## Checks performed

| # | Area | Result |
|---|------|--------|
| 1 | DistCp credentials / `-overwrite` / `-skipcrc` / `-p` / bandwidth / filters | Credentials via job `Credentials`/`TokenCache` only. Filters file is a local path on the DistCp client (`RegexCopyFilter` → `FileInputStream`). Preserve ACL/owner follows HDFS permission rules for the DistCp UGI (self-service or intentional ops job). No standalone HIGH beyond job.xml secret exposure (JHS/AM already covered). |
| 2 | S3A secrets / logging / endpoint / S3Guard / magic committer / encryption / credential chain | No hardcoded keys. Proxy password logged only at DEBUG. `fs.s3a.endpoint` is operator/job conf (exfil of SigV4 to a custom endpoint needs real cloud keys in conf — same secret class as ConfServlet/job.xml). S3Guard/magic committer not default-on; no unauthenticated remote chain. Sensitive keys listed in `hadoop.security.sensitive-config-keys` (secret/session; access key is identifier). |
| 3 | Azure WASB/ABFS | Account keys via KeyProvider / conf; ShellDecryptionKeyProvider is admin-configured. No new remote injection. Secrets-in-conf covered by ConfServlet. |
| 4 | Aliyun OSS / OpenStack Swift | Endpoint/password from conf; no hardcoded secrets; no new remote chain. |
| 5 | hadoop-kafka | `KafkaSink` sets `bootstrap.servers` from metrics config only; no secret/command injection. |
| 6 | Streaming PipeMapRed / StreamJob | `stream.map.streamprocessor` → `ProcessBuilder` is the submitter’s intentional task RCE model. No cross-user or privileged-operator execution path. |
| 7 | Hadoop pipes | `mapreduce.pipes.executable` / interpretor run in the job owner’s task JVM. JobToken binds umbilical. No cross-user RCE. |
| 8 | Hadoop archives (beyond partName) | Creator path writes controlled `part-*` names; escape is in reader (`HarFileSystem` — already reported). |
| 9 | FileUtil unTarUsingJava / unZip (CVE-2022-26612) | **Still present** on 3.1.3. `unpackEntries` creates symlinks without validating `linkName` (`FileUtil.java:984-989`). On Windows, `getCanonicalPath` does not resolve symlinks → write-outside via follow-up entry (CVE-2022-26612 / HADOOP-18155). **Chain:** YARN `FSDownload.unpack` ARCHIVE/PATTERN → `FileUtil.unTar` → `unTarUsingJava` only when `Shell.WINDOWS`. On Unix NM (typical), shell `tar` is used (already reported as tar traversal / cmd injection). `unZip`/`RunJar.unJar` have canonical prefix checks. Not filed as a new HIGH: Windows-only + same unpack class as prior tar findings. |
| 10 | SFTP / FTP | FTP registered in `core-default.xml` (localization SSRF already reported via `FSDownload`+HttpFileSystem). SFTP not in default ServiceLoader list; `StrictHostKeyChecking=no` is MITM risk when SFTP is explicitly used — not a default remote chain. |
| 11 | ViewFs mount table | Mounts come from Configuration (`fs.viewfs.mounttable.*`); client/job conf can only redirect that job’s own FS view. No daemon-side mount injection without operator conf write. |
| 12 | Fuse-dfs | Optional native mount; wrapper passes argv to `fuse_dfs`. No multi-tenant remote attack surface in default cluster daemons. |
| 13 | DefaultImpersonationProvider / ProxyUsers | No `hadoop.proxyuser.*` wildcards in `core-default.xml`. Deny unless operators configure proxy ACL/hosts. |
| 14 | LdapGroupsMapping | User lookup uses JNDI filter args (`{0}`) — not string-concat LDAP injection on the username. `goUpGroupHierarchy` concatenates DNs from LDAP results (requires LDAP write). Bind password in conf → ConfServlet already reported. Not default group mapping. |
| 15 | Configuration DocumentBuilderFactory XXE | `loadResource` uses StAX with `SUPPORT_DTD=false` when restricted. `asXmlDocument` only creates empty docs (export), does not parse attacker XML. Skip (operator-planted config / already covered HostsFileReader). |
| 16 | JavaSerialization / ObjectInputStream | Not in default `io.serializations`. S3A `PendingSet` loads via JSON; `readObject` is Java-serialization compatibility only. |
| 17 | WritableRpcEngine / ObjectWritable | Default engine fallback if protocol unset; modern PB protocols call `setProtocolEngine`. Remaining Writable path: `TaskUmbilicalProtocol` (JobToken-protected). Arbitrary Writable class load ≠ unauthenticated RCE gadget chain on a public RPC. |
| 18 | ZKDelegationTokenSecretManager world ACL | Distinct code from ZKSigner: `authType=none` → Curator `DefaultACLProvider` (`ZKDelegationTokenSecretManager.java:180-182`). **Not default-on** (`zk-dt-secret-manager.enable=false`). KMS HA docs push signer ZK (already reported HIGH), not DT ZK. Same open-ACL pattern when operators enable DT ZK with `none` — treated as adjacent to ZKSigner, not a separate default-complete HIGH. |
| 19 | Fair `AllocationFileLoaderService` / LeveldbConfigurationStore XXE | Fair uses insecure `DocumentBuilderFactory` + `parse` (`AllocationFileLoaderService.java:233-237`); schemes allow `hdfs\|file\|s3a\|viewfs`. Default scheduler is **CapacityScheduler**. Leveldb store uses protobuf WAL, not XML. Complete XXE needs Fair enabled **and** attacker-writable allocation path — not shipped default. |
| 20 | JobSubmitter job.xml / -files / -archives | Full conf written to job.xml (AM/JHS secret leak already reported). Distributed-cache URI fragments are symlink names in the submitter’s own container (expected). `MAPREDUCE_JOB_CREDENTIALS_BINARY` reads a client-local path into the submitter’s credentials only. |
| 21 | ShuffleHandler body MAC | Skipped (already MEDIUM). |

---

## CVE-2022-26612 note (explicit)

| Item | Detail |
|------|--------|
| Present in 3.1.3? | **Yes** — symlink created without target sandbox; Windows canonicalization bypass unfixed (fix versions: 2.10.2 / 3.2.3 / 3.3.3+). |
| Distinct from unTar cmd injection? | **Yes** — different code path (`unTarUsingJava` / `unpackEntries` vs bash `tar` command string). |
| Complete chain? | YARN localization of attacker ARCHIVE on a **Windows** NodeManager. Linux NMs use shell tar (prior finding). |
| New MEDIUM+ filing? | **No** — platform-limited; same unpack/traversal family already accounted for. |
