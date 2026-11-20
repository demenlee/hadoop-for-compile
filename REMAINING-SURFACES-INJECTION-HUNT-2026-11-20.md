# Remaining-surfaces injection / SSRF / deser / path hunt (Hadoop 3.1.3)

**Repo:** `demenlee/hadoop-for-compile` (Apache Hadoop 3.1.3)  
**Scope:** Checklist items 1–12 (exec/OIS/XXE/path/HttpFS/KMS/JN/NFS/streaming/docker/LCE/SNN) for **NEW** MEDIUM+ chains with complete remote/lower-priv → impact paths.  
**Excluded (do not re-report):** CVE-2022-26612 / FileUtil.unTar; HostsFileReader XXE; FairScheduler XXE; ZKConfigurationStore OIS; ResourceMappings OIS; FSDownload http/file SSRF+tar; HadoopArchiveLogs bash; ShellBasedIdMapping bash -c; ShuffleHandler map traversal; NM `log.type` traversal; WebAppProxy SSRF; DN WebHDFS `namenoderpcaddress` SSRF; fencing argv; ZK deser CVE-2021-25642; HarFileSystem `partName`; LocalizationProtocol `FETCH_SUCCESS` `localPath`; CVE-2021-33036 `app_id`; Docker host-net.

---

## NEW validated MEDIUM+

### 1. MEDIUM — YARN Services `HttpProbe` accepts arbitrary URLs (blind SSRF from Service AM)

**Severity:** MEDIUM  
**Primary file:** `hadoop-yarn-project/hadoop-yarn/hadoop-yarn-applications/hadoop-yarn-services/hadoop-yarn-services-core/src/main/java/org/apache/hadoop/yarn/service/monitor/probe/HttpProbe.java`  
**Works when:** YARN Native Services / Service AM is deployed (not a default core daemon, but a supported application surface). Prior Services audit incorrectly marked this “AM-local only.”

#### Description
Component readiness checks of type `HTTP` are built from attacker-controlled service JSON (`readinessCheck.properties.url`). `HttpProbe.create` only does `new URL(urlString)` — there is **no** require-`${THIS_HOST}`, no allow-list, and no block of link-local / RFC1918 / metadata endpoints.

`ServiceMonitor` (running inside the Service AM, started from `ServiceMaster`) periodically calls `ComponentInstance.ping()` → `HttpProbe.ping`, which:

1. Optionally substitutes `${THIS_HOST}` with the container IP (substitution is optional).
2. Opens `HttpURLConnection` to the resulting URL with `setInstanceFollowRedirects(true)`.
3. Treats HTTP status in `[min,max]` (default 200–299) as success.

PortProbe correctly binds to the container IP; HttpProbe does **not**.

#### Impact
Blind SSRF from the Service AM host/process identity: force HTTP(S) requests to cloud metadata (`169.254.169.254`), peer NM/RM admin ports, or internal APIs. Response body is not returned to the client, but readiness success/failure is observable via service instance state (side-channel for reachability / 2xx). Distinct from WebAppProxy SSRF (different daemon and trust boundary).

#### Attack path
1. Cluster runs YARN Services (ServiceClient / ApiServer submission path available to a normal YARN user).
2. Submit a service whose component has `readinessCheck.type=HTTP` and e.g. `properties.url=http://169.254.169.254/latest/meta-data/` (or any internal URL). `${THIS_HOST}` may be omitted.
3. After a container obtains an IP, `ServiceMonitor.ReadinessChecker` invokes the probe on a schedule.
4. AM opens the attacker URL; readiness flips READY/NOT_READY based on status code.

#### Evidence
| Location | Role |
|----------|------|
| `HttpProbe.java:61-71` | Builds probe from unsanitized `url` property |
| `HttpProbe.java:75-80` | `openConnection` + follow redirects |
| `HttpProbe.java:89-93` | Optional `${THIS_HOST}` replace only |
| `MonitorKeys.java:53` | Property key `url` |
| `MonitorUtils.java:71-73` | HTTP type → HttpProbe |
| `ServiceMaster.java:139` | Starts ServiceMonitor in AM |
| `ServiceMonitor.java:102-120` | Periodic `instance.ping()` |
| `ComponentInstance.java:837-843` | Dispatches to configured probe |
| Contrast: `PortProbe.java:77-86` | Connects only to container IP |

#### Remediation
Require `${THIS_HOST}` (or only allow host = instance IP). Deny link-local, loopback, and non-http(s) schemes. Disable redirects. Prefer PortProbe-style binding.

---

## Checked areas — no NEW MEDIUM+ (failed triage)

| # | Area | Why negative / skipped |
|---|------|------------------------|
| 1 | `Runtime.exec` / `ProcessBuilder` / `bash -c` / `Shell.execCommand` (hdfs/yarn/mr/tools/common, non-test) | Streaming/`PipeMapRed` and Pipes `Application` run as job user (LCE) or NM user by DCE design — no cross-user escalation. `ContainerLaunch` env quote-break is same-user / DCE-inherent. FPGA/`GpuDiscoverer`/`NumaResourceAllocator`/`id -u` use argv arrays. `ShellDecryptionKeyProvider` script is admin conf. Fencing / ArchiveLogs / ShellBasedIdMapping already reported. Docker `popen(network inspect %s)` uses allow-listed names (`host,none,bridge` default) — no metacharacters for attacker to choose. |
| 2 | `ObjectInputStream` / `readObject` / `XMLDecoder` | ZKConfigurationStore / ResourceMappings already reported. LeveldbConfigurationStore is local RM path (mutable CS, not remote by default). JavaSerialization / WritableRpcEngine not new network sinks. AWS commit `readObject` is private serialization hooks. |
| 3 | `DocumentBuilderFactory` / SAX without XXE harden | HostsFileReader / FairScheduler already reported. GpuDeviceInformationParser disables external DTD. HttpFS `ConfigurationUtils.load` / rumen / QueueConfigurationParser / ECPolicyLoader parse admin-local XML only — no remote attacker XML. |
| 4 | Path `..` in HTTP/RPC beyond NM logs / shuffle | `metaSave(filename)` joins under `hadoop.log.dir` without sandbox → local overwrite, but gated by `checkSuperuserPrivilege` (same simple-auth superuser spoof class already covered; prior HA audit explicitly declined separate HIGH). AdHocLogDumper filename is hardcoded in `dumpSchedulerLogs`. JN `jid` rejects `/` and leading `.` via `checkJournalId`. |
| 5 | HttpFS remaining | Whitelist `validateNamenode` present; empty/`*` is admin. No shipped `httpfs.proxyuser.*=*`. `GETFILECHECKSUM` / open / ACL ops use authenticated UGI only — no distinct bypass beyond already-reported simple+cookie / `user.name` class. `OwnerParam` unsanitized feeds HDFS metadata only (NFS bash chain already reported). |
| 6 | KMS remaining | Shipped `kms-acls.xml` still `*` for DECRYPT_EEK / defaults — already-reported wildcard ACL class. No separate unauthenticated decrypt when ACLs tightened. ZK signer open ACL excluded by prompt. |
| 7 | JournalNode HTTP beyond `/getJournal` | Only `/getJournal` + shared HttpServer2 stack. `/getJournal` open under simple auth already reported. QJM `acceptRecovery(fromUrl)` SSRF/poison already in QJM RPC reported class. |
| 8 | NFS remaining (write-as-other, SETATTR, symlink) | AUTH_SYS uid → DFSClient user is the known `* rw` export trust model. SETATTR/SYMLINK use that client; SETATTR owner names feed already-reported ShellBasedIdMapping bash sink. No distinct new priv-esc. |
| 9 | Streaming / pipes different user than submitter | Exec as task user; LCE switches to submitter; DCE runs all as NM by design — not a distinct bug. Interpreter/executable from job conf + distcache. |
| 10 | docker-util mounts/env/devices | Mounts use `realpath` + allow-lists; relative mounts must be localized resources (no symlink). Devices only injected by GPU plugin with NM paths; `strncmp` prefix allow-list quirk not attacker-reachable without forging the command file. Env via `--env-file` path written by NM. Privileged/host-pid gated; host-net already reported. |
| 11 | LCE beyond `app_id` | No new argv injection distinct from CVE-2021-33036. Container/app id validation present for docker inspect `popen`. |
| 12 | Secondary/Backup NN HTTP beyond imagetransfer | Only imagetransfer + shared `/conf`/`/logs`/`/logLevel` stack. Imagetransfer already reported class. |

---

## Notes
- Prior Services hunt marked HttpProbe clean; this pass re-validated the code path and files it as **NEW**.
- No other checklist item produced a complete, distinct MEDIUM+ chain after excluding already-reported sinks.
