# Hadoop 3.1.3 focus-area security hunt (2026-11-20)

Repo: demenlee/hadoop-for-compile @ 3.1.3. Scope: HttpServer2 extra servlets, SecretManager=null RPCs, YARN Router interceptors, ApplicationHistory/HSAdmin, NMCollector/ATSv2, CSI/GPU/cgroups, Federation SQL, DCE/LCE remaining, log aggregation, RM CORS/CSRF/scheduler-conf.

## NEW validated MEDIUM+ findings

### 1. HIGH — NameNode `/fsck` `blockId` lookup bypasses HDFS permissions (unauth on defaults)

- **Primary file:** `hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/NamenodeFsck.java`
- **Title:** FsckServlet `blockIdCK` discloses arbitrary block→path and DataNode replica locations without authorization
- **Description:** NameNode registers `/fsck` via `addInternalServlet(..., true)`, but Kerberos/SPNEGO is attached only when security is enabled. Under default simple auth the servlet is open. `FsckServlet` builds a UGI (default `dr.who` or `user.name`) and runs `NamenodeFsck.fsck()`. The `blockId` path calls `blockIdCK`, which takes the NameNode read lock and reads `BlockManager` / `INodeFile` directly — no `checkSuperuserPrivilege`, no path ACL check (contrast: `listCorruptFileBlocks` does call `checkSuperuserPrivilege`). It prints the owning HDFS path and every datanode hostname holding a replica.
- **Impact:** On default clusters (simple auth, NN HTTP `0.0.0.0:9870`), any network client can resolve block IDs to private file paths and DN inventory (aids targeted block theft when combined with other primitives). Under Kerberos, any authenticated principal can still perform the same lookup (authz bypass; SPNEGO alone is not an admin gate). Distinct from NN WebHDFS `user.name`: no identity spoof required for the disclosure, and the sink is block-metadata APIs that never check permissions.
- **Attack path:**
  1. Reach NameNode HTTP on `dfs.namenode.http-address` (default `0.0.0.0:9870`).
  2. `GET http://<nn>:9870/fsck?blockId=blk_<id>` (optional `&user.name=anyone`; defaults to static user `dr.who`).
  3. Response includes `Block belongs to: <full HDFS path>` and `Block replica on datanode/rack: <host>/...`.
  4. Kerberos: same URL after SPNEGO as any principal — still no admin/HDFS ACL gate on `blockIdCK`.
- **Evidence:**
  - `NameNodeHttpServer.java:297-298`
  - `HttpServer2.java:880-888` (`requireAuth` only if `isSecurityEnabled()`)
  - `FsckServlet.java:56-69`
  - `JspHelper.java:132-140` (simple: query user or default web user)
  - `NamenodeFsck.java:358-378` (`blockIds` branch)
  - `NamenodeFsck.java:260-317` (`blockIdCK` — no ACL)
  - `FSNamesystem.java:5507-5510` (contrast: `listCorruptFileBlocks` requires superuser)
  - `hdfs-default.xml:164-165` (`0.0.0.0:9870`)
  - `core-default.xml:2267-2268` (`hadoop.http.staticuser.user=dr.who`)
- **Why existing controls fail:** `/fsck` is an internal servlet without `hasAdministratorAccess`. Path-based fsck uses ClientProtocol (permissioned); `blockIdCK` bypasses that. SPNEGO (when present) authenticates but does not authorize.
- **Works on defaults?** Yes (simple auth).
- **Remediation:** Require administrator ACL (`hasAdministratorAccess`) for `/fsck`. In `blockIdCK`, resolve the file path and enforce `FSPermissionChecker` (or require superuser) before printing path/locations. Bind NN HTTP off the public interface.

### 2. MEDIUM — Unauthenticated `/logLevel` mutation on all HttpServer2 daemons

- **Primary file:** `hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/log/LogLevel.java`
- **Title:** LogLevel servlet allows unauthenticated runtime log-level changes when `hadoop.security.authorization=false`
- **Description:** Every HttpServer2 instance mounts `/logLevel`. The servlet gates on `HttpServer2.hasAdministratorAccess`, which returns `true` for all callers when `hadoop.security.authorization` is false (default). With a `level` query parameter it calls `Logger.setLevel` (log4j / JUL). Distinct from already-reported `/conf` (read) and `/logs` (read): this is an unauthenticated **mutation** on NN/DN/RM/NM/JN/etc.
- **Impact:** Fleet-wide DoS via TRACE/DEBUG disk fill; hide attack evidence by raising levels to ERROR/OFF; no credentials required on default installs.
- **Attack path:**
  1. Reach any daemon HTTP port (e.g. NN `:9870`, DN `:9864`, RM `:8088`).
  2. `GET /logLevel?log=org.apache.hadoop&level=TRACE` (or `OFF`).
  3. Confirm via response `Effective Level`.
- **Evidence:**
  - `HttpServer2.java:758-763` (default servlet mount)
  - `LogLevel.java:326-330` (admin gate)
  - `LogLevel.java:372-380` (`setLevel`)
  - `HttpServer2.java:1418-1421` (authorization=false → allow all)
  - `core-default.xml:54-56` (`hadoop.security.authorization=false`)
- **Why existing controls fail:** Admin ACL is skipped entirely unless service authorization is enabled; HTTP authentication is not required on defaults.
- **Works on defaults?** Yes.
- **Remediation:** Always require authenticated admin for `/logLevel` (ignore authorization=false for mutations). Prefer removing SET from the HTTP surface or binding admin HTTP privately.

### 3. HIGH — NMCollectorService unauthenticated ATSv2 collector hijack (when v2 enabled)

- **Primary file:** `hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/main/java/org/apache/hadoop/yarn/server/nodemanager/collectormanager/NMCollectorService.java`
- **Title:** `CollectorNodemanagerProtocol.reportNewCollectorInfo` accepts unauthenticated collector address updates and propagates them via RM
- **Description:** When Timeline Service v2 is enabled, NodeManager starts `NMCollectorService` on `0.0.0.0:8048` with `SecretManager=null`. `reportNewCollectorInfo` has no caller authorization: it writes attacker-chosen `collectorAddr` into `registeringCollectors`, immediately updates `NMTimelinePublisher` for local apps, and the NM heartbeat forwards registering collectors to the RM (`ResourceTrackerService.updateAppCollectorsMap`), which stamps and stores them on `RMApp` then redistributes to other NMs. Under Kerberos, `CollectorNodemanagerSecurityInfo` sets `clientPrincipal=null` and there is still no method-level ACL — any authenticated principal can hijack. Distinct from Timeline HTTP simple+anonymous (different port/protocol and fleet-wide collector routing impact).
- **Impact:** Redirect or steal ATSv2 put-entity traffic from NMs; poison timeline data; `getTimelineCollectorContext` leaks app user/flow metadata for apps present on the NM.
- **Attack path:**
  1. Enable ATSv2 (`yarn.timeline-service.enabled=true` and version includes 2.x).
  2. Call `CollectorNodemanagerProtocol.reportNewCollectorInfo` on any NM `:8048` with a victim `ApplicationId` and attacker `collectorAddr` (simple: no token; Kerberos: any principal).
  3. Local NM timeline publisher immediately targets the attacker; heartbeat updates RM; other NMs sync the hijacked address.
- **Evidence:**
  - `NodeManager.java:458-460` (service only if v2 enabled)
  - `NMCollectorService.java:82-86` (`SecretManager=null`)
  - `NMCollectorService.java:114-141` (no authz; sets publisher + registering map)
  - `NMTimelinePublisher.java:478-483`
  - `ResourceTrackerService.java:683-720` (RM accepts/stamps collector updates)
  - `CollectorNodemanagerSecurityInfo.java:52-60` (`clientPrincipal=null`)
  - `YarnConfiguration.java:1198-1200` (`0.0.0.0:8048`)
  - `yarn-default.xml:2159-2172` (v1.0 / disabled by default)
- **Why existing controls fail:** No delegation-token SecretManager; service ACL only if `hadoop.security.authorization=true`; no check that caller is the node’s timeline collector auxiliary service.
- **Works on defaults?** No (ATSv2 off). Exploitable whenever ATSv2 is turned on, including Kerberos clusters without a tight service ACL.
- **Remediation:** Require a collector-specific token or NM-local-only bind; authorize `reportNewCollectorInfo` to the aux-service identity; validate collector addresses against an allow-list; enable service authorization with a non-wildcard policy.

## Areas checked — no NEW MEDIUM+ (failed triage)

| Area | Why negative / skipped |
|------|------------------------|
| `/stacks`, `/jmx` | Read-only instrumentation; gated only by `instrumentation.requires.admin=false`; info disclosure below MEDIUM without secret material beyond already-reported `/conf` |
| `ReconfigurationServlet` | Class present but **not registered** on any daemon in 3.1.3 |
| `/prof` / Prometheus | Not present in this tree |
| DN/NN `ReconfigurationProtocol` RPC | Uses `checkSuperuserPrivilege` — same simple-auth superuser class as ClientDatanodeProtocol shutdown |
| YARN Router ClientRM / RMAdmin interceptors | `SecretManager=null` but identity/admin checks deferred to backing RMs; no distinct bypass beyond already-reported Federation ZK + `yarn.admin.acl=*` |
| HSAdminServer / MRAdminProtocol | `mapreduce.jobhistory.admin.acl` default `*` — same admin-ACL-wildcard class as AdminService |
| ApplicationHistoryClientService | `SecretManager=null`; Timeline off by default; ACL-off same class as Timeline HTTP finding |
| Federation SQL store | Parameterized stored-proc calls only; no string-concat SQLi; not remotely exposed without admin JDBC config |
| YARN CSI | Not in 3.1.3 sources |
| GPU / FPGA isolation | Admin-configured binary paths; nvidia-smi XML DTD load disabled; no remote attacker-controlled path |
| CGroups path injection | `cGroupId` from container IDs; remaining issues collapse into CVE-2021-33036 class |
| DCE / LCE remaining | DCE runs as NM user by design; LCE beyond app_id/Docker host-net already reported |
| Log aggregation TFile/Har | ACL-off IDOR / Har escape already reported |
| RM CORS / CSRF | CORS and CSRF filters **disabled** by default — not a vulnerability until enabled with wildcards |
| RM `/scheduler-conf` | Requires writable admin endpoint + MutableConfScheduler (Capacity mutable store); not default Capacity config |
| ResourceTracker / AdminService null SM | Already reported (registerNodeManager / yarn.admin.acl) |
| SharedCache *Protocol null SM | Already reported |
