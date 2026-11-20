# Apache Hadoop 3.1.3 — Remaining HDFS surfaces hunt (2026-11-20)

Source audit of `/agent/repos/hadoop-for-compile`. Scope: DataXceiver beyond tokens-off, Router HTTP/REST, Secondary/Backup HTTP, fencing, short-circuit, EZ/KMS, snapshot/cache/XAttr/ACL RPCs, DiskBalancer/Balancer/Mover, EC codecs, JournalNode HTTP extras, Observer NN, file-based alias maps.

Skipped as already reported: ImageServlet, WebHDFS `user.name` (NN), block tokens off, JN `/getJournal` + QJM RPC, ZKFC world ACL, DN ClientDatanodeProtocol shutdown, InterDatanodeProtocol, `registerDatanode`, HAServiceProtocol RequestSource, InMemory AliasMap RPC, Router admin RPC + ZK store, ConfServlet, `/logs`, HttpFS, NFS, HostsFileReader XXE.

Only **NEW** MEDIUM+ issues with a complete attacker-input → impact chain.

---

## 1. HIGH — DataNode WebHDFS treats client `namenoderpcaddress` as an open NameNode RPC proxy (SSRF)

**Severity:** HIGH  
**Location:** `hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/datanode/web/webhdfs/WebHdfsHandler.java`  
**Works on shipped defaults:** Yes — `dfs.datanode.http.address=0.0.0.0:9864`, `hadoop.security.authentication=simple`. Also works on Kerberos clusters for any caller who already holds a delegation token (SSRF half of the chain).

### Description
NameNode WebHDFS redirects data ops (`OPEN`/`CREATE`/`APPEND`/`GETFILECHECKSUM`) to a DataNode with a `namenoderpcaddress` query parameter. On the DataNode, Netty `URLDispatcher` routes `/webhdfs/v1/*` to `WebHdfsHandler`, which builds a `DFSClient` from that parameter with **no allow-list**:

```java
URI uri = URI.create(HDFS_URI_SCHEME + "://" + nnId);
return new DFSClient(uri, conf);
```

`ParameterParser.namenodeId()` takes the raw `namenoderpcaddress` string. Unlike HttpFS (`FileSystemAccessService.validateNamenode` whitelist), the DataNode never checks that the target is a configured cluster NameNode.

Under simple auth, `DataNodeUGIProvider` also trusts `user.name` / `doAs` with **no AuthFilter** (comment in source: “the DN does not authenticate the UGI”). Combined, any network client can force every DataNode to open Hadoop RPC to an arbitrary `host:port` as an attacker-chosen HDFS identity (including the NN OS user / superuser).

This is distinct from the already-reported NN WebHDFS `user.name` finding: the new primitive is **client-controlled RPC destination from the DN fleet** (SSRF / open proxy), not merely identity spoofing on the NameNode HTTP port.

### Impact
- **Simple-auth (default):** Unauthenticated HDFS superuser read/write via any reachable DataNode HTTP port, even when NameNode HTTP is firewalled — DN proxies RPC to the real NN as `user.name=<nn-user>`.
- **Any auth mode:** SSRF from the DataNode’s network position to attacker-chosen TCP endpoints (internal NNs, other Hadoop RPC services, port probes).
- **Kerberos:** Same SSRF for any principal that can present a delegation token on DN WebHDFS (token is attached and sent toward the attacker-chosen RPC target).

### Attack path
1. Reach DataNode HTTP (`0.0.0.0:9864`).
2. Simple-auth full control against the real NN:
   `PUT http://<dn>:9864/webhdfs/v1/tmp/pwn?op=CREATE&namenoderpcaddress=<nn>:8020&user.name=hdfs&overwrite=true`
   (or `op=OPEN` / `APPEND` / `GETFILECHECKSUM`).
3. SSRF / alternate target:
   same URL with `namenoderpcaddress=<internal-host>:<port>` — DN initiates DFSClient/RPC from the DN host.
4. Kerberos variant: supply a valid `&delegation=...` token and an attacker-chosen `namenoderpcaddress`.

### Evidence
- `WebHdfsHandler.java:331-335` — `newDfsClient` builds `hdfs://` + client `nnId` with no validation
- `WebHdfsHandler.java:196-217,235-238,247-261,288-291` — CREATE/APPEND/OPEN/GETFILECHECKSUM all use that client
- `ParameterParser.java:80-83,124-137` — `namenoderpcaddress` from query; token service rewritten to that authority
- `DataNodeUGIProvider.java:38-40,85-147` — no DN-side auth; trusts `user.name` when security off
- `URLDispatcher.java:48-51` — `/webhdfs` always handled on DN HTTP
- `DatanodeHttpServer.java:152-168` — Netty binds public DN HTTP
- `hdfs-default.xml:133-134` — `0.0.0.0:9864`
- Contrast: `FileSystemAccessService.java:304-310` — HttpFS **has** a namenode whitelist; DN WebHDFS does not

### Why existing controls fail
Block tokens and path ACLs never see this hop: the DN is acting as an RPC client, not a block server. NN WebHDFS hardening or firewalling `:9870` does not protect `:9864`. HttpFS’s whitelist was never applied to the DataNode handler.

### Remediation
Allow-list `namenoderpcaddress` against the DataNode’s configured/registered NameNode RPC addresses (and HA logical URI names). Reject others. Require authentication on DN WebHDFS even in simple mode, or disable DN WebHDFS on untrusted networks. Prefer binding `dfs.datanode.http.address` to a private interface.

---

## Checked — no NEW MEDIUM+ complete chain

| Area | Result |
|------|--------|
| **DataXceiver replaceBlock / transferBlock / copyBlock / SASL** | All ops still gate only on `checkAccess` → skipped when `dfs.block.access.token.enable=false` (already reported). `replaceBlock`’s client-chosen `proxySource` is DN-to-DN follow-on of that same skip, not a tokens-on bypass. SASL is skipped when security is off (`SaslDataTransferServer.receive`); under Kerberos, SASL password is the block token. Privileged-port SASL skip only applies if operators bind xfer &lt;1024 (default `:9866` does not). |
| **HDFS Router HTTP/REST** | `RouterHttpServer` mounts WebHDFS via same `initWebHdfs` / Pseudo path as NN (same already-reported `user.name` class). `forwardRequest` HTTP-connects only to mount-resolved NN web addresses (poisoning requires already-reported Router admin/ZK write). No separate IDOR/mount-injection over HTTP without that. CREATE/OPEN redirects rewrite `namenoderpcaddress` to router id; DN then talks to router RPC — not a new unauthenticated mount write. |
| **Secondary / Backup NN HTTP** | Secondary only adds `/imagetransfer` beyond shared HttpServer2 (`/conf`,`/logs` already reported). BackupNode inherits NameNode HTTP — covered. |
| **ShellCommandFencer / SshFenceByTcpPort** | Fencing command string is operator config; target host/port only as `target_*` env (shell) or SSH/`nc` args from `HAServiceTarget`. HDFS `dataToTarget` rejects ZK breadcrumb host mismatch. No remote injection into fence argv from attacker-controlled names alone. |
| **Short-circuit / libhadoop** | `dfs.client.read.shortcircuit=false`, empty `dfs.domain.socket.path` by default. `getBlockLocalPathInfo` Kerberos + allow-list user. `requestShortCircuitFds` still uses `checkAccess` (tokens-off already reported). `hdfsDisableDomainSocketSecurity` is test-only. |
| **EZ re-encryption / KMS client** | `createEncryptionZone` / `reencryptEncryptionZone` / `addErasureCodingPolicies` superuser-gated; re-encrypt handler is NN-internal after that. No new unauthenticated EDEK path beyond known simple-auth superuser / KMS ACL issues. |
| **Snapshot / cache / XAttr / ACL RPCs** | `allowSnapshot`/`disallowSnapshot` and cache-pool mutations use `checkSuperuserPrivilege`; createSnapshot/XAttr/ACL use owner/ACL checks on claimed UGI — same simple-auth identity spoof already covered by WebHDFS/`user.name`, not a new skip. |
| **DiskBalancer / Balancer / Mover** | DiskBalancer `submitDiskBalancerPlan`/`cancel`/`query` use same DN `checkSuperuserPrivilege` as already-reported ClientDatanodeProtocol shutdown. Balancer/Mover are admin tools using block tokens via NN; no extra unauthenticated RPC service. |
| **Erasure coding codec loading** | Custom codec class comes from local conf key `io.erasurecode.codec.&lt;name&gt;.coder`, not from client-supplied class name in the policy. `addErasureCodingPolicies` is superuser-gated. No remote Class.forName chain. |
| **JournalNode HTTP remaining** | Only `/getJournal` internal servlet beyond shared `/conf`/`/logs`/`/jmx` (getJournal already reported). |
| **Observer NameNode** | Observer = `StandbyState(true)` with `allowStaleReads()==true`. Writes still rejected. `transitionToObserver` is the already-reported HAServiceProtocol RequestSource spoof. No separate Observer auth bypass. |
| **TextFileRegionAliasMap / LevelDBFileRegionAliasMap** | Paths from operator config only; no network listener (unlike already-reported InMemory RPC alias map). |
