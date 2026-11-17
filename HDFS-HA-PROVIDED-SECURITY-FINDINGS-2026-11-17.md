# Apache Hadoop 3.1.3 — HA / fencing / admin / provided-storage hunt

Source audit of `/agent/repos/hadoop-for-compile` (Hadoop 3.1.3). Scope: ZKFC / HAServiceProtocol / fencers, NameNode ClientProtocol admin, Secondary/Backup HTTP (beyond `/imagetransfer`), HttpFS/NFS/KMS deltas, short-circuit, EZ re-encryption, provided storage / alias maps.

Skipped as already reported: ImageServlet, WebHDFS `user.name`, block tokens off, JournalNode `/getJournal` + QJM RPC, ZKFC world ACL znodes, DN ClientDatanodeProtocol shutdown, InterDatanodeProtocol recovery, DatanodeProtocol.registerDatanode, HDFS Router admin + ZK store, ConfServlet, `/logs`, HttpFS simple+cookie, NFS AUTH_SYS+`*rw`, KMS simple+wildcard ACLs, HostsFileReader XXE.

Only **NEW** MEDIUM+ issues with a complete attacker-input → impact chain.

---

## 1. CRITICAL — HAServiceProtocol accepts client-chosen `RequestSource` and trusts simple-auth identity for Active/Standby transitions

**Severity:** CRITICAL  
**Location:** `hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/NameNode.java`  
**Works on shipped defaults:** No for single-NN (HA off). **Yes whenever HDFS HA is enabled without Kerberos** (common “internal HA” deploy). `hadoop.security.authentication=simple`, `hadoop.security.authorization=false`, NameNode RPC on `:8020`.

### Description
`NameNodeRpcServer` mounts `HAServiceProtocol` on the client RPC port. `transitionToActive` / `transitionToStandby` / `transitionToObserver` call `nn.checkHaStateChange(req)` then `namesystem.checkSuperuserPrivilege()`.

`checkHaStateChange` only inspects `StateChangeRequestInfo.getSource()`. That enum is taken verbatim from the RPC protobuf (`HAServiceProtocolServerSideTranslatorPB.convert`) — the **client** picks `REQUEST_BY_USER`, `REQUEST_BY_USER_FORCED`, or `REQUEST_BY_ZKFC`. The server never verifies that the caller is ZKFC or that a ZK election epoch matches.

With auto-failover enabled, `REQUEST_BY_USER` is rejected, but:
- `REQUEST_BY_ZKFC` is accepted whenever auto HA is on
- `REQUEST_BY_USER_FORCED` is accepted with only a warning log

`checkSuperuserPrivilege` → `FSPermissionChecker` treats the RPC username as superuser if it equals the NameNode process user (`fsOwner`) or is in `dfs.permissions.superusergroup` (default `supergroup`). Under shipped `hadoop.security.authentication=simple`, that username is a client-claimed string (no SASL).

Independently, `ZKFCProtocol.cedeActive` / `gracefulFailover` on ZKFC `:8019` call `checkRpcAdminAccess`, which allows any caller whose short name equals the ZKFC login user (or `dfs.cluster.administrators`). Same simple-auth claim works.

This is distinct from the already-reported ZKFC **ZooKeeper** world-ACL DoS: here the attacker drives NameNode HA state (and optional fencing) directly over RPC without writing znodes, and can force **both** NameNodes active (split-brain) rather than a single coordinated failover.

### Impact
Unauthenticated HA control-plane takeover on any simple-auth HA cluster: force the active NN to standby (outage), force the standby to active without fencing, or activate both NNs (split-brain edit streams / data corruption). No HDFS admin credentials required.

### Attack path
1. Operators enable HDFS HA (manual or automatic) and leave `hadoop.security.authentication=simple`.
2. Attacker opens NameNode RPC (`:8020`) claiming `HADOOP_USER_NAME=<nn-os-user>` (commonly `hdfs`; discoverable via already-open `/conf` or process conventions).
3. Call `HAServiceProtocol.transitionToStandby` with `reqSource=REQUEST_BY_ZKFC` (or `REQUEST_BY_USER_FORCED`) against the active NN.
4. Call `transitionToActive` with the same spoofed source against the former standby — or against **both** nodes to create split-brain.
5. Optional: claim the ZKFC OS user on `:8019` and call `ZKFCProtocol.gracefulFailover` / `cedeActive` to drive fencing through the configured fencer.

### Evidence
- `NameNodeRpcServer.java:1757-1778` — HA transitions → `checkHaStateChange` + `transitionTo*`
- `NameNode.java:1785-1806` — `transitionToActive`/`Standby` call `checkSuperuserPrivilege()`
- `NameNode.java:2057-2085` — `checkHaStateChange` trusts client `RequestSource` only
- `HAServiceProtocolServerSideTranslatorPB.java:87-112` — protobuf `reqSource` → enum, no caller binding
- `FSNamesystem.java:4745-4749` / `FSPermissionChecker.java:96-97,126-131` — superuser = claimed name vs `fsOwner`/`supergroup`
- `DFSZKFailoverController.java:204-215` — ZKFC admin = claimed name equals ZKFC login user
- `ZKFCRpcServer.java:85-94` — `cedeActive`/`gracefulFailover` → `checkRpcAdminAccess`
- `hadoop-policy.xml:106-116` — `security.ha.service.protocol.acl=*` / `security.zkfc.protocol.acl=*`
- `core-default.xml:69-70` — authentication `simple`
- `hdfs-default.xml:4086-4090` — ZKFC port `8019`

### Why existing controls fail
Auto-HA’s “reject manual `REQUEST_BY_USER`” gate is bypassed by spoofing `REQUEST_BY_ZKFC`. Service ACLs are `*` and authorization is off by default. `dfs.permissions.enabled=true` only compares the **claimed** RPC identity to `fsOwner`; it does not authenticate that claim under simple mode. Kerberos `@KerberosInfo` on `HAServiceProtocol` is inactive when authentication is `simple`.

### Remediation
Require Kerberos for HA clusters. Bind `RequestSource.REQUEST_BY_ZKFC` to an authenticated ZKFC principal (or a mutual-auth channel), not a client-supplied enum. Set `hadoop.security.authorization=true` and replace `security.ha.service.protocol.acl` / `security.zkfc.protocol.acl` with the NN/ZKFC principals only. Treat any simple-auth HA deployment that exposed `:8020`/`:8019` as having had untrusted Active/Standby transitions; reconcile edit logs and fence both NNs before restart.

---

## 2. HIGH — Provided-storage InMemory AliasMap RPC is unauthenticated and cannot run under Kerberos

**Severity:** HIGH  
**Location:** `hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/aliasmap/InMemoryLevelDBAliasMapServer.java`  
**Works on shipped defaults:** No (`dfs.namenode.provided.enabled=false`, `dfs.provided.aliasmap.inmemory.enabled=false`). **Yes on any deploy that enables provided storage with the in-memory alias map** — that combination is the documented DN/NN alias-map path, and the server **refuses to start if Kerberos is on**.

### Description
When both `dfs.namenode.provided.enabled` and `dfs.provided.aliasmap.inmemory.enabled` are true, `NameNode.startAliasMapServerIfNecessary` starts `InMemoryLevelDBAliasMapServer` on `dfs.provided.aliasmap.inmemory.dnrpc-address` (default `0.0.0.0:50200`).

`start()` throws if `UserGroupInformation.isSecurityEnabled()`, so this control plane exists **only** on insecure clusters. The RPC `Builder` installs no `SecretManager`, and `AliasMapProtocolPB` carries a `TODO add Kerberos support` comment and is **not** registered in `HDFSPolicyProvider`.

`write` / `read` / `list` on `InMemoryAliasMap` perform LevelDB get/put with **no** identity check. DataNodes use `InMemoryLevelDBAliasMapClient` to resolve `ProvidedStorageLocation` URIs and `ProvidedReplica` opens those remote paths via `FileSystem.get` / `remoteFS.open`.

### Impact
Any network client can rewrite block → external URI mappings. Provided-volume DataNodes then read attacker-chosen URIs (data substitution into HDFS reads of provided files, or forced DN fetches to attacker endpoints / internal URLs). Listing the map discloses every provided object location.

### Attack path
1. Operators enable provided storage with the in-memory alias map (required to be non-Kerberos by server startup).
2. AliasMap RPC listens on `0.0.0.0:50200`.
3. Attacker calls `AliasMapProtocol.write(block, ProvidedStorageLocation)` for an existing provided block ID (from NN metadata / WebHDFS / FSImage) pointing at an attacker-controlled `hdfs`/`http`/`file` URI the DN can reach.
4. Subsequent client reads of that provided file are served from the attacker location via `ProvidedReplica.getDataInputStream`.

### Evidence
- `NameNode.java:730-740` — starts alias map when provided + inmemory enabled
- `InMemoryLevelDBAliasMapServer.java:68-99` — refuses Kerberos; RPC with no auth
- `InMemoryLevelDBAliasMapServer.java:110-120` — `read`/`write` passthrough
- `InMemoryAliasMap.java:157-163` — LevelDB put, no ACL
- `AliasMapProtocolPB.java:25-32` — “TODO add Kerberos support”
- `HDFSPolicyProvider.java:46-92` — no `AliasMapProtocol` service
- `InMemoryLevelDBAliasMapClient.java:128-132,116-120` — DN client also Kerberos-incompatible; `store` → `write`
- `ProvidedReplica.java:183-199` — opens remote URI from alias map
- `hdfs-default.xml:4840-4844,4879-4900` — feature flags default off; bind `0.0.0.0:50200`; leveldb dir `/tmp`

### Why existing controls fail
There is no authentication path: the feature aborts under Kerberos instead of adding SASL. HDFS path permissions and block tokens never apply to AliasMap RPC. Policy-provider ACLs cannot cover a protocol that is not registered.

### Remediation
Do not enable `dfs.provided.aliasmap.inmemory.enabled` until AliasMap RPC has Kerberos (or mTLS) and an allow-list of writers (NN/admin only). Bind `dfs.provided.aliasmap.inmemory.dnrpc-address` to localhost/private interface. Prefer offline `TextFileRegionAliasMap` / LevelDB file maps loaded from trusted admin-controlled paths. After exposure, rebuild the alias map from a known-good inventory and audit provided file contents.

---

## Checked — no NEW MEDIUM+ complete chain

| Area | Result |
|------|--------|
| **ShellCommandFencer** | `bash -c` runs **operator** fencing string; target host/port only exported as `target_*` env vars. No attacker-controlled interpolation into the command on the HDFS path. |
| **SshFenceByTcpPort** | Hostname used in `nc -z` is from `HAServiceTarget` address. HDFS `DFSZKFailoverController.dataToTarget` **rejects** ZK breadcrumb host/port that does not match configured NN address — blocks ZK→fence hostname injection. Residual risk only if HA config itself is attacker-writable. |
| **PowerShellFencer** | Concatenates hostname/processName into WMI filter without escaping (code smell), but HDFS target host still comes from local NN config after the same mismatch check. No complete remote chain. |
| **ClientProtocol admin** (`setSafeMode`, `saveNamespace`, `rollEdits`, `refreshNodes`, `finalizeUpgrade`, `metaSave`, `setBalancerBandwidth`, `rollingUpgrade`, `restoreFailedStorage`) | All gated by `checkSuperuserPrivilege`. Under simple auth this trusts the claimed name — **same root cause already covered by WebHDFS `user.name` superuser**. `getDelegationToken` additionally requires Kerberos/web auth (`isAllowedDelegationTokenOp`) and fails closed in simple mode. `metaSave(filename)` joins under `hadoop.log.dir` without normalizing `..` (local overwrite as NN user) but still requires that same superuser gate — not filed as separate HIGH. |
| **Secondary NN HTTP** | Only adds `/imagetransfer` beyond the shared HttpServer2 stack (`/conf`, `/logs` already reported). |
| **BackupNode HTTP** | Inherits NameNode HTTP (WebHDFS / imagetransfer) — already covered. |
| **HttpFS** | No new chain beyond already-reported simple auth + cookie secret. |
| **NFS gateway** | No new chain beyond already-reported AUTH_SYS + `* rw`. |
| **KMS ACLs** | Wildcard `*` defaults already reported; no additional chain beyond simple+wildcard. |
| **Short-circuit / domain sockets** | `dfs.client.read.shortcircuit=false`, `dfs.domain.socket.path` empty by default. `getBlockLocalPathInfo` requires Kerberos + allow-list user. `requestShortCircuitFds` still hits `checkAccess` (token skip is the already-reported block-token-off issue) and requires a domain socket peer — no new remote default chain. |
| **Encryption zone / ReencryptionHandler** | `createEncryptionZone` / `reencryptEncryptionZone` require superuser; handler is NN-internal after that. No unauthenticated EZ key/EDEK path beyond already-known simple-auth superuser / KMS ACL issues. |
| **Text/LevelDB file alias maps** | Paths are operator config (`dfs.provided.aliasmap.text.*` / `leveldb.path`), not network-attacker input. |
