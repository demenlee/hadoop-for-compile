# Apache Hadoop 3.1.3 — HDFS remaining-surfaces hunt (2026-11-20, pass B)

Source audit of `/agent/repos/hadoop-for-compile`. Scope: NFS gateway remaining ops, NameNode HTTP extras, DataNode HTTP extras, JournalNode RPC mutation, HDFS Router (RBF) mount/forward, provided/alias maps remaining, EZ/KMS remaining, inotify/snapshot/XAttr/ACL Kerberos leaks, WebHDFS ACL/JSON/redirect remaining, libhdfs/fuse-dfs, NameNode RPC with no identity check.

Skipped as already reported: ImageServlet, WebHDFS `user.name`, block tokens off, JN `/getJournal` + QJM RPC, ZKFC world ACL, HAServiceProtocol RequestSource, InMemory AliasMap, DN WebHDFS `namenoderpcaddress`, `/fsck` blockIdCK, NFS ShellBasedIdMapping bash -c, DataXceiver tokens-off class, Router HTTP Pseudo, Secondary/Backup imagetransfer, fencing argv, SCR off, EZ re-encrypt superuser, DiskBalancer superuser class, EC local conf, Observer stale reads, ConfServlet/`/logs`/`/logLevel`, HttpFS, NFS AUTH_SYS+`* rw`.

Only **NEW** MEDIUM+ issues with a complete attacker-input → impact chain.

---

## 1. HIGH — RBF Router admin lets any authenticated user add mount entries they “own” (hijack victim paths)

**Severity:** HIGH  
**Location:** `hadoop-hdfs-project/hadoop-hdfs-rbf/src/main/java/org/apache/hadoop/hdfs/server/federation/store/impl/MountTableStoreImpl.java`, `.../router/RouterPermissionChecker.java`  
**Works on shipped RBF defaults:** Yes when Router is deployed — `dfs.federation.router.admin.enable=true`, admin RPC `0.0.0.0:8111`, `dfs.permissions.enabled=true`.

### Description
`RouterAdminServer.addMountTableEntry` delegates to `MountTableStoreImpl`, which authorizes with `RouterPermissionChecker.checkPermission(mountTable, WRITE)` on the **new entry itself**.

`MountTable.newInstance` always sets `ownerName` to the RPC caller and mode `0755`. `checkPermission` then treats “caller equals owner AND owner has WRITE” as success. There is **no** superuser gate, **no** parent-mount check, and **no** restriction that only admins may create mounts.

`MountTableResolver.findDeepest` uses longest-prefix match. An attacker can therefore install a deeper mount under a shared prefix (e.g. `/user/victim`) pointing at a destination path they control on a registered nameservice. Subsequent Router ClientProtocol forwards for that prefix rewrite the remote path via `buildLocation`, so the victim’s reads/writes land under the attacker’s directory (still under the victim’s Kerberos UGI on the destination NN — but the data is written where the attacker can read it with ordinary path ACLs).

`enterSafeMode` / `leaveSafeMode` on the same admin server have **zero** permission checks (reported separately below as MEDIUM).

Distinct from already-reported NN/Router WebHDFS Pseudo `user.name` (HTTP identity spoof) and from ZK state-store world-write: this is an authenticated Kerberos principal abusing mount-table ACL semantics on the admin RPC port.

### Impact
On any RBF cluster with reachable Router admin RPC, a normal authenticated user can redirect other users’ federation traffic for chosen path prefixes into attacker-controlled destinations on a member nameservice → cross-user data theft and tampering via the Router.

### Attack path
1. Authenticate to Router admin RPC (`dfs.federation.router.admin-address`, default `0.0.0.0:8111`) as normal principal `alice` (Kerberos ticket or simple `alice`).
2. `addMountTableEntry` with source `/user/bob`, destination `{ns1 → /tmp/alice}` (owner auto-set to `alice`, mode `0755`).
3. Permission check passes (self-owner WRITE). Entry is stored and picked up by `MountTableResolver.refreshEntries`.
4. When `bob` uses the Router for `/user/bob/...`, `findDeepest` selects alice’s mount; `buildLocation` maps to `ns1:/tmp/alice/...`.
5. Alice reads `/tmp/alice/...` on `ns1` with her own credentials.

### Evidence
- `MountTableStoreImpl.java:57-71` — ADD only checks WRITE on the new entry
- `RouterPermissionChecker.java:72-99` — owner/mode ACL; self-owner with user WRITE succeeds
- `MountTable.java:144-151` — `newInstance` sets owner = remote user, mode `0755`
- `RouterAdminServer.java:227-229` — no superuser gate on ADD
- `MountTableResolver.java:580-590` — `findDeepest` longest prefix
- `MountTableResolver.java:541-567` — `buildLocation` rewrites remote path under attacker dest
- `hdfs-rbf-default.xml:156-166` — admin enabled, `0.0.0.0:8111`
- Contrast: `RouterAdminServer.java:344-350` — `disableNameservice` **does** call `checkSuperuserPrivilege`

### Why existing controls fail
Mount ACL is evaluated on attacker-supplied ownership of a brand-new record, not on an admin-only capability. Destination NN path ACLs still apply to the **victim’s** UGI, so writes succeed into the attacker-chosen path; the attacker then reads that path as themselves. Kerberos authenticates the caller but does not authorize mount creation.

### Remediation
Require `checkSuperuserPrivilege` (or a dedicated mount-admin ACL) for add/update/remove mount table entries. Do not treat self-ownership of a newly created record as authorization to publish federation mounts. Bind admin RPC to a private interface.

---

## 2. HIGH — `ClientProtocol.reportBadBlocks` has no path/owner authorization

**Severity:** HIGH  
**Location:** `hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/NameNodeRpcServer.java`, `FSNamesystem.java`, `BlockManager.java`  
**Works on shipped defaults:** Yes (any authenticated ClientProtocol caller; under Kerberos any principal).

### Description
`NameNodeRpcServer.reportBadBlocks` calls `namesystem.reportBadBlocks` with **no** `checkSuperuserPrivilege`, **no** file path ACL check, and **no** verification that the caller holds a block token or lease for the reported blocks. `FSNamesystem.reportBadBlocks` only takes the write lock and invokes `blockManager.findAndMarkBlockAsCorrupt` for each `(block, datanode)` pair.

When enough live replicas remain, `markBlockAsCorrupt` immediately invalidates (schedules deletion of) the reported replica. Repeating across locations drives under-replication / unavailability. This is reachable as a normal Kerberos user; it does not require simple-auth identity spoof.

Distinct from already-reported `/fsck` `blockIdCK` (metadata disclosure) and from block-tokens-off DataXceiver reads: this is an authenticated ClientProtocol **mutation** that deletes replicas without owning the file. Block IDs/locations can come from files the attacker can read, from `blockIdCK`, or from other metadata leaks.

### Impact
Any authenticated HDFS client can mark arbitrary replicas corrupt and force invalidation, causing availability loss and, with repeated targeting of all replicas / racing reconstruction, potential durable data loss for files the attacker cannot write.

### Attack path
1. Obtain `(block pool, blockId, genstamp, datanode)` for a target file (own readable file, or `/fsck?blockId=...` under Kerberos, or ImageServlet on simple clusters).
2. As any authenticated principal, call `ClientProtocol.reportBadBlocks` with a `LocatedBlock` naming that block and DN.
3. NN marks the replica corrupt; if redundancy is still satisfied, the replica is invalidated.
4. Repeat for other DNs holding the block.

### Evidence
- `NameNodeRpcServer.java:957-959` — no authz before `reportBadBlocks`
- `FSNamesystem.java:5294-5313` — lock + mark corrupt only
- `BlockManager.java:1700-1735` — `findAndMarkBlockAsCorrupt`
- `BlockManager.java:1789-1792` — enough live replicas → `invalidateBlock`
- Contrast: `listCorruptFileBlocks` / admin ops use `checkSuperuserPrivilege` (`FSNamesystem.java:5507-5510`)

### Why existing controls fail
`dfs.permissions.enabled=true` never runs for this RPC. Block tokens gate DataNode data-plane access, not this NameNode control-plane report. Kerberos only proves caller identity; the method never compares that identity to the inode owner/ACL.

### Remediation
Require the caller to own the file (or hold a READ/WRITE block token for the block), or restrict `reportBadBlocks` to DataNodeProtocol / superuser. Rate-limit and audit anonymous-looking corruption reports.

---

## 3. MEDIUM — Router `enterSafeMode` / `leaveSafeMode` have no authorization

**Severity:** MEDIUM  
**Location:** `hadoop-hdfs-project/hadoop-hdfs-rbf/src/main/java/org/apache/hadoop/hdfs/server/federation/router/RouterAdminServer.java`  
**Works on shipped RBF defaults:** Yes when Router admin is enabled.

### Description
`enterSafeMode` and `leaveSafeMode` flip Router safemode and update router state with **no** `getPermissionChecker()` / `checkSuperuserPrivilege()` call. Any client that can invoke Router admin RPC (Kerberos any principal, or simple any claimed user) can take the Router out of service or force it out of safemode.

Distinct from finding #1 (mount hijack): this is pure availability control with a complete missing authz gate on the same port.

### Impact
Unauthenticated-as-admin DoS of the federation Router control plane (clients fail while Router is in manual safemode).

### Attack path
1. Reach Router admin `:8111` as any authenticated principal.
2. Call `enterSafeMode`.
3. Router serves safemode failures until an admin recovers.

### Evidence
- `RouterAdminServer.java:278-295` — `enterSafeMode` no ACL
- `RouterAdminServer.java:298-313` — `leaveSafeMode` no ACL
- Contrast: `disableNameservice` at `344-350` checks superuser

### Remediation
Gate safemode transitions with `checkSuperuserPrivilege` (same as disable/enable nameservice).

---

## Checked — no NEW MEDIUM+ complete chain

| Area | Result |
|------|--------|
| **NFS WRITE/CREATE/SYMLINK/LINK/SETATTR/REMOVE/RENAME** | All gated by `checkAccessPrivilege` + exports; default `nfs.exports.allowed.hosts=* rw` + AUTH_SYS already reported. SETATTR uid path uses `getUserName(int)` (id→name), not the name→`bash -c` sink already reported via getattr/owner mapping. |
| **NFS mountd / portmap / path traversal** | Mount uses same exports table; file ops use fileId handles (`Nfs3Utils.getFileIdPath`), not client path strings with `../`. No distinct traversal chain. |
| **NFS uid mapping remaining** | Beyond already-reported bash -c and AUTH_SYS spoof: no additional MEDIUM+ sink. |
| **NN HTTP startupProgress** | Unauth JSON progress (phase/file names only); below MEDIUM vs already-reported `/conf`/`/imagetransfer`. |
| **NN JSP helpers / getimage / streamFile / browseDirectory / checksum** | Not registered as servlets on 3.1.3 NN HTTP beyond `startupProgress`, `fsck`, `imagetransfer`, WebHDFS, shared HttpServer2 stack. |
| **DN HTTP directory listing / JSP / replica download with tokens on** | Non-WebHDFS traffic proxied to Jetty info server; only `/blockScannerReport` added. WebHDFS OPEN uses DFSClient→NN tokens then DataXceiver `checkAccess` (enforced when tokens on). No tokens-on bypass. |
| **JN RPC unauthenticated edits mutation** | `format`/`newEpoch`/`journal` lack identity checks under simple auth — same already-reported QJM RPC class as `/getJournal`. Epoch fencing only. |
| **RBF RPC forward to attacker NN** | Mount destinations are nameservice IDs resolved via MembershipStore (Router heartbeat), not client `host:port`. No complete forward-to-attacker-NN without already-reported ZK/membership write. |
| **Text/LevelDB file alias maps** | Config-path only; no network listener (InMemory RPC already reported). |
| **EZ / KMS client** | create/list/reencrypt superuser-gated; no new unauth EDEK path. |
| **inotify `getEditsFromTxid`** | `checkSuperuserPrivilege` — no normal-user leak under Kerberos. |
| **snapshot diff / XAttr / ACL APIs** | Path READ/owner checks via `FSPermissionChecker` when permissions enabled; no cross-user leak under Kerberos. |
| **WebHDFS ACL bypass / JSON injection / redirect** | Ops run as authenticated UGI with NN permission checks; Location redirect client issue already CVE-2020-9492. |
| **libhdfs / fuse-dfs** | Client-side; fuse maps FUSE uid via `getpwuid` — local mount trust model, no remote daemon chain. |
| **NN RPC no-identity methods** | `registerDatanode` already reported. `versionRequest` returns NamespaceInfo only (low). `commitBlockSynchronization` needs matching recoveryId from active recovery (incomplete without DN collusion). `refreshUserToGroupsMappings` lacks superuser check but impact is cache refresh DoS only (below MEDIUM bar used here). |
