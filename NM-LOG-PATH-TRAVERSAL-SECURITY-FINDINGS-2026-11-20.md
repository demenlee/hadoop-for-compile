# Apache Hadoop 3.1.3 — NM container log path traversal hunt (2026-11-20)

Source audit of `/agent/repos/hadoop-for-compile`. Scope: remaining injection/deser/XXE/SQL/path/SSRF/crypto surfaces after prior audits; focus validation on NM log path joins.

Skipped as already reported: ShellBasedIdMapping, HadoopArchiveLogs, FileUtil.unTar, HostsFileReader XXE, ResourceMappings/ZKConfigurationStore OIS, Har partName, FSDownload SSRF, WebAppProxy SSRF, DN WebHDFS namenoderpcaddress, ShuffleHandler map traversal, KMS ZK signer, HttpFS cookie secret, ConfServlet, FairScheduler XXE, rumen JobConfigurationParser XXE, ClientAM, Registry DNS, Federation AMRMProxy tokens, SCM, ImageServlet, WebHDFS user.name, block tokens off, JN getJournal, ZKFC ACL, HAServiceProtocol RequestSource, AliasMap, /fsck blockIdCK, /logLevel, NMCollector ATSv2.

Only **NEW** MEDIUM+ issues with a complete attacker-input → impact chain.

---

## 1. HIGH — NodeManager container log viewer path traversal via `log.type` (arbitrary local file read)

**Severity:** HIGH  
**Primary file:** `hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/main/java/org/apache/hadoop/yarn/server/nodemanager/webapp/ContainerLogsUtils.java`  
**Works on shipped defaults:** Yes — `hadoop.security.authentication=simple`, `yarn.acl.enable=false`, `yarn.nodemanager.hostname=0.0.0.0` → webapp `0.0.0.0:8042`, `SecureIOUtils` skips owner checks when security is off.

### Description

The NM web UI route `/containerlogs/:container.id/:app.owner/:log.type` renders `ContainerLogsPage`, which resolves the log file through:

```java
Path logPath = dirsHandler.getLogPathToRead(
    relativeContainerLogDir + Path.SEPARATOR + fileName);
```

`fileName` comes from `$(CONTAINER_LOG_TYPE)` i.e. `log.type`. YARN `View.$()` prefers path `moreParams`, but **falls back to the HTTP query string** when the path omits that segment:

```java
String value = moreParams().get(key);
if (value == null) {
  value = request().getParameter(key);
}
```

`LocalDirsHandlerService.getPathToRead` joins the attacker string under each NM log root with `new Path(tmpFile.getPath(), pathStr)` and returns the first path that `exists` — **without** verifying the resolved file remains under the container log directory. `../` segments therefore escape into arbitrary local paths.

`ContainerLogsUtils.openLogFileForRead` then opens that file via `SecureIOUtils.openForRead`. When `hadoop.security.authentication=simple` (default), `SecureIOUtils` **skips** owner/group checks and returns a raw `FileInputStream`. Combined with `yarn.acl.enable=false` (default) and `checkAccess` allowing a null `remoteUser`, an unauthenticated caller who knows any live `containerId` on that NM can read any local file the NM process can read (configs, keytabs, credentials, other users' logs, `/etc/passwd`, etc.).

Distinct from already-reported NM/aggregated **log IDOR** (reading intended log files without VIEW_APP): this is a **directory sandbox escape** to arbitrary filesystem paths.

### Impact

- **Default simple-auth clusters:** Unauthenticated arbitrary local file disclosure as the NodeManager OS user (`yarn`) via HTTP `:8042`.
- **Kerberos:** Authenticated callers can still escape the log directory to any file owned by the application user (SecureIOUtils owner match), e.g. other job tokens / home files.

### Attack path

1. Discover a container on a reachable NM (`GET http://<nm>:8042/node/allContainers` or RM app reports). Defaults bind NM web on `0.0.0.0:8042`.
2. Request (omit path `log.type` so the query param is used):
   `GET http://<nm>:8042/node/containerlogs/<containerId>/<anyOwner>?log.type=../../../../../../etc/passwd`
   (adjust `../` depth to the NM log root; same for yarn-site, keytabs, usercache paths).
3. `ContainerLogsPage` → `getContainerLogFile` → `getLogPathToRead` resolves outside the container log dir; `SecureIOUtils` opens it without an owner check on simple auth; contents are streamed in the HTML log view.
4. Kerberos variant: same URL after SPNEGO; owner check still allows any file owned by the app submitter.

### Evidence

- `ContainerLogsUtils.java:91-109` — concatenates `fileName` into log path; no sandbox after join
- `ContainerLogsUtils.java:160-200` — `openLogFileForRead` → `SecureIOUtils.openForRead`
- `ContainerLogsUtils.java:132-146` — `checkAccess` no-ops when `remoteUser == null`
- `LocalDirsHandlerService.java:556-583` — `getPathToRead` joins + `exists` only; no canonical-prefix check
- `ContainerLogsPage.java:133-166` — uses `$(CONTAINER_LOG_TYPE)` for local file open
- `View.java:161-167` — query-parameter fallback for missing path params
- `WebServer.java:165-167` — route `pajoin("/containerlogs", CONTAINER_ID, APP_OWNER, CONTAINER_LOG_TYPE)`
- `YarnWebParams.java:31-34` — `log.type` / `container.id` / `app.owner`
- `SecureIOUtils.java:195-200` — owner checks skipped when security disabled
- `yarn-default.xml:269-270` — `yarn.acl.enable=false`
- `yarn-default.xml:1033-1034,1478-1479` — NM hostname/webapp `0.0.0.0:8042`
- `core-default.xml:69-70` — `hadoop.security.authentication=simple`

### Why existing controls fail

VIEW_APP / ACL checks only gate *whether* a container's logs are visible, not *which path* under that container is opened. Jetty path normalization does not apply to query-string `log.type`. SecureIOUtils's symlink/owner hardening is disabled on the default simple-auth profile.

### Remediation

Reject `log.type` / REST `filename` containing `..`, absolute paths, or separators. After resolving the file, require `getCanonicalPath()` (and symlink-safe open) to stay under the container's log directories. Keep SecureIOUtils checks even when Kerberos is off for this servlet, or disable anonymous NM log access.

---

## Areas checked — no additional NEW MEDIUM+ complete chain

| Area | Result |
|------|--------|
| Shell `bash -c` group mapping | `Shell.bashQuote` single-quote escaping on default JNI-fallback path; distinct from already-reported ShellBasedIdMapping NFS sink |
| ShellCommandFencer / PowerShellFencer / DF | Operator config / volume roots only; no remote name→argv injection |
| SQLFederationStateStore | Parameterized `{call sp_*(?...)}` only |
| JobEndNotifier URL | Runs in submitter AM by design (self SSRF) |
| ResourceEstimator `FileInputStream(logFile)` | Optional offline tool; POST opens path but does not return file body (weak chain) |
| GpuDeviceInformationParser XXE | External DTD load disabled; input from local nvidia-smi |
| HttpFS ConfigurationUtils XXE | Loads operator conf files only |
| S3A/ABFS/WASB/ADL credential providers | Admin/job conf; no new remote steal/redirect beyond ConfServlet/job.xml class |
| ZK DTSM world ACL | Not default-on; adjacent to already-reported ZKSigner |
| libhdfs CVE-2021-37404 | Client library; no default remote daemon call chain |
| RunJar CVE-2024-23454 temp perms | Local multi-user only; Apache severity low — not filed as MEDIUM+ here |
| ContainerLaunch USER env in double quotes | Collapses into DCE-runs-as-yarn / simple-auth identity class already covered |
| Federation/Timeline SQL | No string-concat SQLi found |
| Remaining XXE (rumen/Fair/queue) | Already excluded / not default-on attacker XML |
