# YARN 3.1.3 security hunt (2026-11-17)

Repo: demenlee/hadoop-for-compile @ main  
Scope: Timeline v1/v2, YARN Services, Federation/Router, DockerLinuxContainerExecutor, Shared cache, Log aggregation, RM web IDOR, AMRM/AM protocol, NM exec/cgroups/GPU, Registry, ATS  
Excluded (already reported): FSDownload tar/zip traversal; ZKConfigurationStore OIS CVE-2021-25642; WebAppProxyServlet SSRF; container-executor app_id CVE-2021-33036; ZKRMStateStore world ACL tokens; FileSystemRMStateStore umask tokens; NMWebServices container logs; ResourceMappings OIS

## NEW MEDIUM+ findings

### 1. HIGH — Federation ZooKeeper state store is world-writable; Router can be steered to attacker RMs

- **Severity:** HIGH  
- **Location:** `ZookeeperFederationStateStore` + Router `FederationInterceptorREST`  
- **Evidence:**
  - `hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-common/src/main/java/org/apache/hadoop/yarn/server/federation/store/impl/ZookeeperFederationStateStore.java:137-140` (roots created with `ZKCuratorManager.getZKAcls`)
  - `.../ZookeeperFederationStateStore.java:600-620` (`put()` → `zkManager.create(znode)` with **null ACL**, then `setData`)
  - `hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/util/curator/ZKCuratorManager.java:85-91,269-286`
  - `hadoop-common-project/hadoop-common/src/main/resources/core-default.xml:3063-3064` (`hadoop.zk.acl=world:anyone:rwcda`)
  - `hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-router/src/main/java/org/apache/hadoop/yarn/server/router/webapp/FederationInterceptorREST.java:436-501` (submit uses store membership + `getRMWebServiceAddress()`)
  - Docs require overwriting Memory store with ZK/SQL for real federation (`yarn.federation.state-store.class`)
- **Attack path:**
  1. Operators enable `yarn.federation.enabled=true` and set `yarn.federation.state-store.class` to `ZookeeperFederationStateStore` (required for multi-node federation; Memory is not shared).
  2. ZooKeeper keeps shipped `hadoop.zk.acl=world:anyone:rwcda`. Store roots under `/federationstore/{memberships,applications,policies}` are world rwcda; child `put()` creates/writes with null/open ACL.
  3. Attacker opens the ZK client port and writes/overwrites a `SubClusterInfo` under `memberships/<id>` whose `RMWebServiceAddress` (and related RM endpoints) point at an attacker host; and/or rewrites `applications/<appId>` home subcluster mappings / queue policies.
  4. YARN Router reads the poisoned store and proxies `submitApplication` / app lifecycle REST/RPC to the attacker “RM”, capturing submission credentials, tokens, and job payloads; existing apps can be redirected on lookup/kill/status.
- **Impact:** Cluster-wide federation control-plane hijack: steal YARN submissions/tokens, black-hole or impersonate subclusters, alter routing policy. Distinct from ZKRMStateStore token theft (different znodes and Router-mediated RM spoofing).
- **Default-config note:** `yarn.federation.enabled=false` and Memory store by default; **any production federation deployment must switch to ZK/SQL**, and ZK inherits world ACL unless operators harden `hadoop.zk.acl`.

### 2. HIGH — Timeline Server HTTP defaults to simple+anonymous, independent of cluster Kerberos

- **Severity:** HIGH  
- **Location:** Timeline v1 ATS HTTP auth + ACL short-circuit  
- **Evidence:**
  - `hadoop-yarn-project/hadoop-yarn/hadoop-yarn-common/src/main/resources/yarn-default.xml:2276-2286` (`yarn.timeline-service.http-authentication.type=simple`, `...simple.anonymous.allowed=true`)
  - `hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-common/src/main/java/org/apache/hadoop/yarn/server/timeline/security/TimelineAuthenticationFilterInitializer.java:54-55,92-108` (filter prefix `yarn.timeline-service.http-authentication.`)
  - `hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-applicationhistoryservice/src/main/java/org/apache/hadoop/yarn/server/applicationhistoryservice/ApplicationHistoryServer.java:239-265` (installs Timeline auth filter; comments note insecure-mode `user.name`)
  - `.../timeline/webapp/TimelineWebServices.java:229-241,357-363` (`getUser` = `request.getRemoteUser()` only)
  - `.../timeline/security/TimelineACLsManager.java:113-114` (if `yarn.acl.enable=false`, all access allowed)
  - ATSv2 reader reuses the same initializer via `TimelineReaderAuthenticationFilterInitializer`
- **Attack path:**
  1. Operators enable ATS (`yarn.timeline-service.enabled=true`) while the rest of the cluster may already use Kerberos RPC. Timeline HTTP auth is a **separate** key and stays `simple` unless explicitly changed.
  2. Attacker hits `:8188/ws/v1/timeline` (or ATSv2 reader) with `user.name=<victim-or-admin>` (Pseudo / PseudoDelegationToken handler).
  3. With default `yarn.acl.enable=false`, reads/writes of all entities/domains succeed. Even if ACLs are later enabled, simple auth still lets the attacker **become** the domain owner via `user.name`, bypassing domain reader/writer lists.
  4. Attacker exfiltrates timeline entities (often include app diagnostics/config fragments) and/or poisons domains/entities; can mint Timeline delegation tokens under the spoofed identity via the DT-capable handler.
- **Impact:** Unauthenticated (or trivially spoofed) read/write of ATS data and Timeline DT minting as any principal, including on Kerberos clusters that never retargeted Timeline HTTP auth.
- **Default-config note:** Timeline is **off** by default (`yarn.timeline-service.enabled=false`). When enabled, shipped HTTP auth is simple+anonymous; `yarn.acl.enable` defaults false.

### 3. MEDIUM — Docker runtime defaults to host networking

- **Severity:** MEDIUM  
- **Location:** `DockerLinuxContainerRuntime`  
- **Evidence:**
  - `yarn-default.xml:1752-1762` / `YarnConfiguration.java:1930-1945` — allowed networks include `host`; **default network = `host`**
  - `DockerLinuxContainerRuntime.java:339-345,817,876` — unspecified network → `defaultNetwork` → `--net=host`
  - `conf/container-executor.cfg` — Docker module disabled until operators set `module.enabled=true`
- **Attack path:**
  1. Operators enable LCE + Docker module for YARN Docker containers.
  2. Any submitter launches a container with `YARN_CONTAINER_RUNTIME_TYPE=docker` and a valid image, omitting `YARN_CONTAINER_RUNTIME_DOCKER_CONTAINER_NETWORK`.
  3. NM runs the container with host network namespace (user remapping still applies by default).
  4. Container reaches NM-localhost services, host-routed cloud metadata, and can bind host ports as the remapped UID—breaking network isolation expected of multi-tenant Docker.
- **Impact:** Cross-tenant host-network access on every NM that enabled Docker; stepping stone to local service abuse / credential scrape. Not root by itself (privileged containers remain off by default).
- **Default-config note:** Docker module off in shipped `container-executor.cfg`; once enabled, host is both allowed and the default network.

## ResourceMappings note (already reported)

`ResourceMappings.AssignedResources.fromBytes` still uses raw `ObjectInputStream` (`ResourceMappings.java:92-99`) via `NMLeveldbStateStoreService`. **`yarn.nodemanager.recovery.enabled` defaults to `false`** (`yarn-default.xml:1996-1998`). Not re-reported.

## Checked — no additional NEW MEDIUM+ complete chain

| Area | Result |
|------|--------|
| Timeline GenericObjectMapper | Jackson `Object.class` without default typing → Maps/Lists, not gadget RCE |
| Timeline Leveldb state store tokens | Dir umask 0700 when recovery enabled |
| ATSv2 collector putEntities | Requires remoteUser; same simple-auth issue covered above |
| YARN Services ApiServer | Optional process; Kerberos path uses `createProxyUser(remoteUser)`; dangerous mainly if HTTP left simple (related to Timeline/HttpFS class of bug). HttpProbe SSRF is AM-local only |
| Registry | Default `FSRegistryOperationsService`; ZK registry open only when `hadoop.registry.secure=false` (documented non-secure mode) |
| Shared cache | Checksum-keyed paths; NM uploader is trusted peer; no path-traversal chain found |
| Log aggregation | App dirs 770 / files umask 640; group-readable by design for yarn group, not world |
| RM web IDOR | `hasAccess` fails open when ACLs off / null UGI; same insecure-default class as known NM logs, not a new primitive |
| AMRMToken in ApplicationReport | Only for unmanaged AM + matching owner (`RMAppImpl.java:728-737`) |
| ClientToAMToken | Issued under security to VIEW_APP callers; bound to caller name |
| Docker mounts/privileged/PID | Native allow-lists; privileged/host-pid default false; trusted-registry gates mounts |
| cgroups / GPU / FPGA | Paths built by NM; GPU deny-list via devices cgroup; no user-controlled root write path beyond already-reported executor issues |
| container-executor resources cgroups | Values come from NM PrivilegedOperation, not user argv directly |
| ApplicationMasterProtocol | Attempt-scoped AMRM token validation intact in reviewed paths |
| Federation SQL store | Parameterized callable statements; creds in config (ops concern, not RCE) |
| LeveldbConfigurationStore OIS | Same family as CVE-2021-25642 (already flagged); local 0700 dir |
| CSRF | Disabled by default on RM/NM/Timeline — needs Kerberos browser session for useful CSRF; not standalone HIGH without cookie auth |
