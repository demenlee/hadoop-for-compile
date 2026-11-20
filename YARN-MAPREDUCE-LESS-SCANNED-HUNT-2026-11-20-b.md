# YARN / MapReduce less-scanned surfaces hunt (pass B, 2026-11-20)

**Repo:** Apache Hadoop **3.1.3** (`hadoop-for-compile`)  
**Verdict:** **NO NEW FINDINGS** (no new MEDIUM/HIGH/CRITICAL with a complete attack chain)

**Excluded (do not re-report):** CVE-2021-33036 LCE app_id; WebAppProxy SSRF; ResourceMappings OIS; Timeline simple HTTP; ATSv2 collector hijack / NMCollectorService; NM log IDOR + log.type traversal; Federation ZK world ACL; Registry DNS; AMRMProxy tokens; ClientAM unauth stop/flex; SCM uploader no auth; Docker host-net; archive-logs bash; ShuffleHandler mapId; AMWebServices live conf; shuffle MAC; RM VIEW_APP IDOR when `yarn.acl.enable=false`; `/conf` `/logs` `/logLevel`; ZKConfigurationStore OIS; FairScheduler XXE; LocalizationProtocol FETCH_SUCCESS localPath; NM REST VIEW_APP skip; HttpProbe SSRF; scheduler app-activities VIEW_APP skip; CMP `signalToContainer` NMToken ownership skip; HistoryFileManager intermediate forgery; JHS `/downloadconf` unredacted; `getJobAttempts` VIEW_JOB skip; `yarn.admin.acl=*`; ResourceTracker `registerNodeManager`; SharedCache SecretManager=null.

---

## Surfaces checked

### 1. YARN Federation Router (ClientRM / RMAdmin / web interceptors)
- `DefaultClientRequestInterceptor` / `FederationClientInterceptor` / `DefaultRMAdminRequestInterceptor` create RM proxies under caller UGI (`createProxyUser` only when username differs from current UGI); mutations forward to RM and inherit RM ACL/`checkAcls`.
- `FederationInterceptorREST` proxies submit/kill/getApp to home subcluster; attempt/container routes throw `NotImplementedException` (no extra ACL skip).
- Router web `genericForward` builds UGI from servlet principal (`RMWebAppUtil.getCallerUserGroupInformation`), not a free-form header.
- No new authz bypass beyond already-reported Federation ZK world ACL / membership hijack.

### 2. YARN RM REST with `yarn.acl.enable=true`
- Re-audited app-scoped GETs: `getApp` uses `hasAccess`; attempt/container detail go through `ClientRMService` VIEW_APP.
- `getAppAttempts` still skips `hasAccess` and leaks AM `nodeId`/`containerId` — **already reported**.
- `getAppActivities` — **already reported**.
- Reads of state/priority/queue/timeouts omit VIEW_APP but expose only metadata already visible via list/`AppInfo` when access is denied (below MEDIUM vs placement leaks).
- Mutations (kill/priority/queue/timeout) and `scheduler-conf` require writable init + `ClientRMService` MODIFY_APP / admin ACL.

### 3. YARN NM web/RPC remaining
- `getNodeContainer` / `getNodeApp` VIEW_APP skip — **already reported**.
- CMP `signalToContainer` ownership skip — **already reported**.
- `start`/`stop`/`status`/`update`/`localize`/`reInitialize` bind NMToken↔application.
- `getNMResourceInfo` is unauthenticated GPU/FPGA inventory only (recon; no complete escalate chain).
- LocalizationProtocol localPath — **already reported**.

### 4. Log aggregation (AggregatedLogsBlock / LogServlet / TFile / HAR)
- HTML `TFileAggregatedLogsBlock` / `IndexedFileAggregatedLogsBlock` call `checkAcls` using owner+ACLs from the TFile.
- AHS REST `getLogs` → `readAggregatedLogs` with soft-redacted `getApp` — **already reported** authz swallow.
- `LogAggregationUtils.getRemoteLogUserDir` uses `new Path(root, user)`; HTML path still ACL-gates after open. HAR `partName` — **already reported**.
- `readAggregatedLogs` (CLI/REST helper) has no ACL by design of that API; web callers that skip ACL are the known AHS path.

### 5. DockerLinuxContainerRuntime remaining
- Privileged / host-pid: default **false**; require cluster enable + (for privileged) ACL.
- User mounts: regex-parsed in Java; **enforced** in `docker-util.c` against `docker.allowed.ro-mounts` / `rw-mounts` (realpath + prefix with trailing `/`).
- Capabilities/devices: from admin conf / allowlists in container-executor; untrusted images drop mounts/caps/devices.
- Default allowed networks include `host` and default network is `host` — covered by **already-reported Docker host-net** (including host port exposure).
- Ports-mapping on bridge: regex-only, no port allowlist — same multi-tenant Docker isolation class as host-net when Docker is enabled (Docker not in default `allowed-runtimes`).

### 6. MapReduce HsWebServices / HSAdmin / JobClient / TaskUmbilical / Shuffle
- HsWebServices: endpoints use `checkAccess` except `getJobAttempts` (**already flagged**); `/conf` redaction gap vs `/downloadconf` already flagged.
- HSAdmin: `checkAcls`; default `admin.acl=*` known config class.
- HistoryClientService: `verifyAndGetJob` + VIEW_JOB when ACLs on; DT issue requires Kerberos when security on.
- TaskUmbilical: JobTokenSecretManager; `getTask` JVM map registration-gated (TODO is same-job only).
- ShuffleHandler: beyond map path + URL HMAC — `user` from registered job map; IndexCache keyed by attempt; no new sink.

### 7. ApplicationHistory
- Attempt/container RPCs call `checkAccess`.
- `generateApplicationReport` soft-redacts on VIEW_APP deny (enables known log path); no new secret channel beyond that.

### 8. YARN Services ApiServer `getProxyUser`
- `ApiServer.getProxyUser` uses `createProxyUser(remoteUser, loginUser)` **without** `ProxyUsers.authorize`.
- Under Kerberos+SPNEGO, `remoteUser` is the authenticated principal → doAs-self (no cross-user escalate). RPC layer still runs `ProxyUsers.authorize` on the RM/NN.
- Under Kerberos RPC + default `hadoop.http.authentication.type=simple`, `user.name` spoof + common `hadoop.proxyuser.*.groups=*` is the **same class as already-reported Timeline simple HTTP**. Standalone ApiServer / RM embed (`yarn.webapp.api-service.enable=false` by default) noted previously.
- Not filed as NEW.

### 9. CapacityScheduler mutable conf
- Default `yarn.scheduler.configuration.store.class=file` → `FileBasedCSConfigurationProvider` (immutable REST).
- Mutable path only for memory/leveldb/zk; PUT `/scheduler-conf` requires admin (`initForWritableEndpoints(..., true)`) + `DefaultConfigurationMutationACLPolicy` (or QueueAdmin policy which gates global params to yarn admin).
- No default-on mutable bypass.

### 10. JavaSandbox / whitelist / GPU / FPGA
- Sandbox default **disabled**; whitelist group empty by default; group policies admin-configured.
- GPU/FPGA plugins admin/resource-plugin paths; Nvidia plugin endpoint is NM config at init (not attacker URL).
- container-executor `check_user` bans root + default banned users; min.user.id enforced.

---

## Summary

No new MEDIUM/HIGH/CRITICAL complete attack chains on the listed less-scanned surfaces beyond issues already excluded or reported earlier on this tree.
