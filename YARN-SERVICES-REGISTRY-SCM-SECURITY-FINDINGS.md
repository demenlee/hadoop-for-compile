# YARN Services / Registry / SCM / AMRMProxy / DistributedShell — 3.1.3 security hunt

Repo: `demenlee/hadoop-for-compile` @ `main` (Hadoop **3.1.3**)

Scope: YARN Native Services + ApiServer, SharedCacheManager, DistributedShell, UnmanagedAMLauncher, Registry/DNS, AMRMProxy, Services REST.

Excluded (already known / prior reports): WebAppProxyServlet SSRF; container-executor app_id CVE-2021-33036; ZKRMStateStore / FileSystemRMStateStore tokens; NMWebServices logs; ResourceMappings OIS; FSDownload HTTP SSRF / tar traversal; Timeline simple auth; Federation ZK membership store; Docker host network; AdminService `yarn.admin.acl=*`; ResourceTracker master keys.

---

## 1. HIGH — Native Services ClientAMProtocol stop/flex/upgrade has no owner check (cross-user control)

**Severity:** HIGH  
**Primary location:** `hadoop-yarn-project/hadoop-yarn/hadoop-yarn-applications/hadoop-yarn-services/hadoop-yarn-services-core/src/main/java/org/apache/hadoop/yarn/service/ClientAMService.java`

### Evidence
- RPC server started with only `ClientToAMTokenSecretManager`; service ACL refresh only if `hadoop.security.authorization=true` (default **false**): `ClientAMService.java:88-96`
- `stop` / `flexComponents` / `upgrade` / `restart` / `decommissionCompInstances` perform the action with **no** caller-vs-owner check (only log `getCurrentUser()`): `ClientAMService.java:117-161`, `168-189`, `228-241`
- `hadoop.security.authorization` default false: `core-default.xml:54-56`
- `yarn.acl.enable` default false → `ApplicationACLsManager.checkAccess` returns true for everyone: `ApplicationACLsManager.java:108-110`; `yarn-default.xml:268-270`
- ApplicationReport therefore exposes AM `host` + `rpcPort` to any RM client: `ClientRMService.java:392-396`, `RMAppImpl.java:719-720`
- ApiServer / ServiceClient stop/flex path calls this protocol: `ApiServer.java:246-258`, `ServiceClient.java:712-719`, `668-669`

### Attack path
1. Victim runs a YARN Native Service (Service AM starts `ClientAMService` on the NM host).
2. Attacker obtains `ApplicationReport` for that app (default ACLs off → any YARN identity; with Simple auth that is any network client who can reach the RM).
3. Attacker opens Hadoop RPC to `host:rpcPort` as `ClientAMProtocol` (Simple auth accepts an arbitrary username; token/Kerberos service ACL is not required under defaults).
4. Attacker invokes `stop`, `flexComponents`, `upgrade`, etc.
5. AM exits or resizes/upgrades components — no ownership gate.

### Impact
Cross-tenant denial of service and unauthorized lifecycle control of any Native Service (stop, flex, upgrade, decommission), including via Services REST when ApiServer/`yarn.webapp.api-service.enable` fronts the same `ServiceClient` path.

### Why controls fail
- Method handlers never compare caller to service owner / MODIFY_APP.
- Service authorization ACL is off by default; even when on, the ACL key is absent from shipped `hadoop-policy.xml`.
- Unlike MapReduce `ClientServiceDelegate`, `ServiceClient.createAMProxy` does not wire `ApplicationReport.getClientToAMToken()` into the UGI — under Simple security the SecretManager does not block callers.

### Works on shipped defaults?
**Yes, whenever a Native Service is running.** No non-default flags required beyond submitting/using YARN Services. ApiServer embedding (`yarn.webapp.api-service.enable`) defaults false but is not required for the RPC chain.

---

## 2. HIGH — Registry DNS publishes attacker-controlled A/AAAA records when registry left insecure (default)

**Severity:** HIGH (when Registry DNS is enabled)  
**Primary location:** `hadoop-yarn-project/hadoop-yarn/hadoop-yarn-registry/src/main/java/org/apache/hadoop/registry/server/dns/RegistryDNSServer.java`

### Evidence
- DNS off by default, but insecure registry is the shipped default: `hadoop.registry.secure=false` (`core-default.xml:2776-2784`; `RegistryConstants.DEFAULT_REGISTRY_SECURE=false`)
- Insecure mode installs world r/w ZK ACLs: `RegistrySecurity.java:318-323` (`WorldReadWriteACL`)
- DNS server watches registry and registers records on every node add: `RegistryDNSServer.java:110-127`, `178-184`
- A-record target host taken from attacker-supplied endpoint address with **no** allow-list: `ApplicationServiceRecordProcessor.java:315-325`; `BaseServiceRecordProcessor.java:372-382`
- DNS names are predictable (`{service}.{user}.{domain}`): `BaseServiceRecordProcessor.java:356-363`

### Attack path
1. Operators enable `hadoop.registry.dns.enabled=true` and point clients / cluster resolvers at Registry DNS (required for YARN Services DNS discovery).
2. `hadoop.registry.secure` remains false (shipped default) → registry znodes are world-writable.
3. Attacker writes/overwrites a `ServiceRecord` under `/registry/...` with `external` endpoints whose host is an attacker IP (or spoofs a victim service path).
4. `RegistryDNSServer` listener calls `registryDNS.register` → zone serves attacker A/AAAA/SRV/TXT.
5. Service clients resolve the service DNS name to the attacker and send traffic/credentials.

### Impact
Cluster service-discovery DNS poisoning / MITM for any name in the Registry DNS domain (credential theft, traffic diversion, fake service endpoints).

### Why controls fail
- Enabling DNS does not force `hadoop.registry.secure=true`.
- Record processors trust endpoint host fields from registry bytes with no ownership or IP validation.
- DNSSEC is optional and off by default (`KEY_DNSSEC_ENABLED`).

### Works on shipped defaults?
**Not on a cold default cluster** (DNS disabled). **Yes on the standard “turn on Registry DNS” operator path** without also flipping registry security — the insecure ACL is the shipped default.

---

## 3. HIGH — Federation AMRMProxy persists UAM AMRM tokens in cleartext registry records readable by other users

**Severity:** HIGH (when AMRMProxy HA / federation registry client is enabled)  
**Primary location:** `hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-common/src/main/java/org/apache/hadoop/yarn/server/federation/utils/FederationRegistryClient.java`

### Evidence
- UAM AMRM token written as URL-encoded cleartext into `ServiceRecord.description`: `FederationRegistryClient.java:134-139`, `291-298`
- Loaded back with `decodeFromUrlString`: `FederationRegistryClient.java:180-189`
- FederationInterceptor always persists renewed tokens when `registryClient != null`: `FederationInterceptor.java:1008-1018`, `1139+`
- Registry client attached when AMRMProxy HA enables registry: `AMRMProxyService.java:133-139`
- Default registry impl is FS-based and **“relies entirely on the configured FS for security and does no extra checks”**: `FSRegistryOperationsService.java:54-56`, `151-154` (`fs.create` → default umask `022` → world-readable files)
- If operators switch to ZK registry with default `hadoop.registry.secure=false`, znodes are world r/w (`RegistrySecurity.java:318-323`)

### Attack path
1. Operators enable federation + `yarn.nodemanager.amrmproxy.enabled` with HA/registry (`yarn.nodemanager.amrmproxy.ha.enable=true` and/or FederationInterceptor pipeline with registry).
2. Federated apps cause AMRMProxy to `writeAMRMTokenForUAM` under `yarn.federation.registry.base-dir` (default `yarnfederation/`).
3. Attacker reads the `_record` file (HDFS other-readable) or ZK registry node (world ACL) and `decodeFromUrlString`s the AMRM token.
4. Attacker presents the token to the subcluster RM as that application attempt and allocates/releases containers / finishes the AM.

### Impact
Theft of live AMRM tokens → hijack of federated application masters’ RM sessions (resource abuse, kill/finish AM, container allocation as the victim app).

### Why controls fail
- Tokens stored in recoverable cleartext in a shared registry without encryption or MAC.
- FS registry performs no ACL hardening; ZK registry defaults to open ACLs.
- Distinct from the already-reported Federation **state-store membership** ZK issue (different znodes / Router steering).

### Works on shipped defaults?
**No** (`amrmproxy.enabled` / `ha.enable` / federation default false). **Yes** on the production federation+AMRMProxy HA path without registry ACL hardening.

---

## 4. MEDIUM — SharedCacheManager client/uploader RPC has no authentication or authorization (YARN-2774)

**Severity:** MEDIUM (when shared cache enabled)  
**Primary location:** `hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-sharedcachemanager/src/main/java/org/apache/hadoop/yarn/server/sharedcachemanager/SharedCacheUploaderService.java`

### Evidence
- Uploader RPC: `Secret manager null` + TODO “security not supported” / “proper security/authorization needs to be implemented”: `SharedCacheUploaderService.java:77-84`, `110-113`
- Client RPC same: `ClientProtocolService.java:100-108`
- `notify()` accepts any `resourceKey`/`fileName` and updates the store: `SharedCacheUploaderService.java:105-125`
- Feature flag default false: `yarn.sharedcache.enabled=false` (`yarn-default.xml:2585-2587`)

### Attack path
1. Operators enable shared cache and run SharedCacheManager.
2. Any caller who can reach the uploader/client ports (Simple: unauthenticated; Kerberos: any authenticated principal — still no service ACL) calls `notify` / `use` / `release`.
3. Attacker claims checksum→filename mappings first so legitimate NM uploads are rejected and deleted (`SharedCacheUploader.java:155-161`), or pins references to block cleaner eviction.

### Impact
Shared-cache metadata poisoning and availability attack (failed localization / cache pollution). Not direct RCE by itself.

### Why controls fail
Explicit unfinished work (YARN-2774); admin protocol has ACL checks but client/uploader do not.

### Works on shipped defaults?
**No** (shared cache off). **Yes** once `yarn.sharedcache.enabled=true` without waiting for a security story that 3.1.3 never shipped.

---

## Checked — no additional NEW MEDIUM+ complete chain

| Area | Result |
|------|--------|
| **Services ApiServer REST IDOR (HDFS path)** | Service defs live under caller home (`CoreFileSystem.getBaseApplicationPath`); cross-user REST needs identity spoof. Standalone ApiServer adds `AuthenticationFilterInitializer` only under Kerberos login and still inherits `hadoop.http.authentication.type` (same class as Timeline, but Timeline already reported). RM embed (`yarn.webapp.api-service.enable`) defaults **false**. Residual cross-user stop is finding #1 via ClientAM, not a separate HDFS IDOR. |
| **Artifact URI SSRF** | Tarball/default providers require FS paths that exist for the submitting user; no http(s) FS wired by default in 3.1.3 `core-default`. Localization still hits excluded FSDownload/tar issues if ARCHIVE used. |
| **HttpProbe SSRF** | AM-local `HttpURLConnection` from service readiness check (`HttpProbe.java:75-94`). Same privilege as the service user; not RM-level SSRF. |
| **DistributedShell cmd injection beyond ArchiveLogs** | `shell_command` / `shell_env` run in the submitter’s containers (or yarn under DefaultContainerExecutor by design). No privileged in-tree caller besides already-reported archive-logs tooling. |
| **UnmanagedAMLauncher** | AMRM token only for unmanaged AM owner (`RMAppImpl.java:728-737`); launcher writes token file mode 600 for the submitting user. |
| **AMRMProxy non-federation** | Listener forced to TOKEN auth (`AMRMProxyService.java:157-172`); local token secret manager. No extra intercept bug without federation registry path (#3). |
| **Docker privileged / host-net in services** | Covered by prior Docker host-network finding when Docker module enabled. |

---

## Remediation (short)
1. Enforce owner (or MODIFY_APP) on every `ClientAMService` mutating RPC; require ClientToAMToken (and wire it in `ServiceClient`) even when cluster Simple auth is used for other protocols.
2. Refuse to start Registry DNS unless `hadoop.registry.secure=true` (or equivalent non-world ACLs); validate endpoint addresses.
3. Never store raw AMRM tokens in world-readable registry/FS paths — encrypt, or use private ACLs + secure registry.
4. Do not enable SCM until YARN-2774 authz exists; network-isolate SCM ports.
