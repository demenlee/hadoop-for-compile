# HDFS protocol-surface hunt (2026-11-20)

Repository: Apache Hadoop 3.1.3 (`demenlee/hadoop-for-compile`).  
Scope: ClientDatanodeProtocol, InterDatanodeProtocol, DataXceiver (tokens on), NameNodeRpcServer remaining ClientProtocol methods, Router RBF mount update/remove + nameservice enable + ClientProtocol forward, WebHDFS with CSRF off, DataNode HTTP scanners, JournalNode RPC beyond QJM, libhdfs/fuse-dfs.

Skipped as already reported: fsck blockIdCK, ImageServlet, WebHDFS user.name, block tokens off DataXceiver, JN /getJournal + QJM acceptRecovery SSRF, ZKFC world ACL, HAServiceProtocol RequestSource, InMemory AliasMap, DN WebHDFS namenoderpcaddress SSRF, NFS ShellBasedIdMapping bash, Secondary/Backup imagetransfer, fencing argv, SCR off, EZ re-encrypt superuser, DiskBalancer superuser, EC local conf, Observer stale reads, ConfServlet/logs/logLevel, HttpFS, NFS AUTH_SYS * rw, RBF mount **ADD** as self-owner, reportBadBlocks, commitBlockSynchronization, registerDatanode, Router HTTP Pseudo, DN ClientDatanodeProtocol shutdown, InterDatanodeProtocol recovery, Router enterSafeMode.

Only **NEW** MEDIUM+ issues with a complete attacker-input → impact chain.

---

## 1. HIGH — RBF `updateMountTableEntry` authorizes the attacker-supplied record, not the stored mount (overwrite any mount)

**Severity:** HIGH  
**Location:** `hadoop-hdfs-project/hadoop-hdfs-rbf/src/main/java/org/apache/hadoop/hdfs/server/federation/store/impl/MountTableStoreImpl.java`, `.../store/records/MountTable.java`, `.../router/RouterPermissionChecker.java`  
**Works on shipped RBF defaults:** Yes when Router is deployed — `dfs.federation.router.admin.enable=true`, admin RPC `0.0.0.0:8111`, `dfs.permissions.enabled=true`.

### Description
`MountTableStoreImpl.updateMountTableEntry` calls `RouterPermissionChecker.checkPermission(mountTable, WRITE)` on **`request.getEntry()`** — the replacement record from the client — then `getDriver().put(mountTable, true, true)` overwrites the store entry keyed by source path.

It never loads the **existing** mount for authorization (contrast `removeMountTableEntry`, which does `get` then checks WRITE on `deleteEntry`).

`MountTable.newInstance(src, destinations)` always sets `ownerName` to the RPC caller and mode `0755`. Therefore any authenticated principal can craft an update whose owner is themselves, pass the owner-WRITE check, and replace an admin-owned mount (including `/`, `/user`, or any registered path) with attacker-chosen nameservice destinations.

Distinct from already-reported **ADD** self-owner bypass: ADD creates a new deepest-prefix entry; UPDATE **rewrites an existing mount** without consulting the stored owner/mode. Prior ADD remediation text mentioned update/remove generically; this pass validates that UPDATE is still implemented with the wrong authz object and is exploitable independently of ADD.

### Impact
On any RBF cluster with reachable Router admin RPC, a normal Kerberos (or simple) user can hijack existing federation mounts → redirect other users’ Router ClientProtocol traffic into attacker-controlled destinations on a registered nameservice → cross-user data theft/tampering (victim UGI still used on the destination NN; data lands where the attacker can read it).

### Attack path
1. Authenticate to Router admin RPC (`dfs.federation.router.admin-address`, default `0.0.0.0:8111`) as normal principal `alice`.
2. Discover an existing mount source (e.g. `/user/bob` or `/`) via `getMountTableEntries` (READ allowed for other-execute on default `0755` entries) or known layout.
3. Call `updateMountTableEntry` with a `MountTable` for that source whose destinations point at `{ns1 → /tmp/alice}` and owner = `alice` (automatic via `MountTable.newInstance`, or set explicitly in protobuf).
4. Server checks WRITE on alice’s crafted record (succeeds); `put(..., update=true)` overwrites the stored mount.
5. When `bob` uses the Router for that prefix, `MountTableResolver` maps to alice’s destination; alice reads the redirected data under her own ACLs.

### Evidence
- `MountTableStoreImpl.java:75-89` — UPDATE checks WRITE on `request.getEntry()` only, then put-update
- `MountTableStoreImpl.java:93-107` — REMOVE correctly loads existing entry before `checkPermission`
- `MountTable.java:144-151` — `newInstance` sets owner = remote user, mode `0755`
- `RouterPermissionChecker.java:72-99` — owner/mode ACL; self-owner with user WRITE succeeds
- `RouterAdminServer.java:233-236` — no superuser gate on UPDATE
- Contrast `RouterAdminServer.java:344-350` / `381-386` — disable/enable nameservice **do** call `checkSuperuserPrivilege`
- `hdfs-rbf-default.xml:156-166` — admin enabled, `0.0.0.0:8111`

### Why existing controls fail
Mount ACL is evaluated on attacker-chosen ownership of the **replacement** record, not on the mount already published in the state store. Destination NN path ACLs still apply to the **victim’s** UGI, so writes succeed into the attacker-chosen path; the attacker then reads that path as themselves. Kerberos authenticates the caller but does not authorize mount mutation.

### Remediation
For UPDATE (and ADD), require `checkSuperuserPrivilege` or a dedicated mount-admin ACL. If mount ACLs are retained, authorize against the **existing** stored entry (as REMOVE does), and do not treat self-ownership of a client-supplied record as permission to publish or replace federation mounts. Bind admin RPC to a private interface.

---

## Checked — no NEW MEDIUM+ complete chain

### ClientDatanodeProtocol
| Method | Result |
|--------|--------|
| `getReplicaVisibleLength` | When tokens on: requires BlockToken READ in UGI (`DataNode.java:2957-2985`). When tokens off: length-only; subsumed by already-reported tokens-off DataXceiver full read. |
| `getBlockLocalPathInfo` | Kerberos + allow-list `dfs.block.local-path-access.user` (empty by default) + block token (`1906-1926`). |
| `getHdfsBlocksMetadata` | **Absent** from 3.1.3 `ClientDatanodeProtocol` interface. |
| `refreshNamenodes` / `shutdownDatanode` / `evictWriters` / `triggerBlockReport` / `startReconfiguration` / `getReconfigurationStatus` / `listReconfigurableProperties` / `deleteBlockPool` / `getVolumeReport` / DiskBalancer APIs | `checkSuperuserPrivilege` (`3189+`). Shutdown / DiskBalancer already reported for the DN-user/simple-auth superuser class. Reconfig properties limited to `dfs.datanode.data.dir` and balancer movers — no NN address injection. |
| `getDatanodeInfo` / `getBalancerBandwidth` | No superuser check; returns version/uptime or bandwidth only — below MEDIUM. |

### InterDatanodeProtocol
`initReplicaRecovery` / `updateReplicaUnderRecovery` have no method-level ACL or token check (`DataNode.java:2928-2954`) — **already reported** as InterDatanodeProtocol recovery.

### DataXceiver (tokens ON)
All block ops (`READ`/`WRITE`/`TRANSFER`/`COPY`/`REPLACE`/`BLOCK_CHECKSUM`/`REQUEST_SHORT_CIRCUIT_FDS`) call `checkAccess` → `blockPoolTokenSecretManager.checkAccess` when `isBlockTokenEnabled`. `releaseShortCircuitFds` / `requestShortCircuitShm` are domain-socket / slot lifecycle only (no block bytes); SCR-off class already reported. No tokens-on bypass found.

### NameNodeRpcServer remaining methods
| Method | Result |
|--------|--------|
| `getDataEncryptionKey` | Fresh per-call key when encrypt-data-transfer on (default off); not cross-user file access (prior triage). |
| `renewLease` | No holder ownership check; clientName not exposed to non-superusers (`listOpenFiles` superuser-only) — below MEDIUM without oracle. |
| `listCacheDirectives` / `listCachePools` | Pool ACL / permission filtered via `FSNDNCacheOp`. |
| `setStoragePolicy` / unset | Path `FsAction.WRITE` via `FSDirAttrOp`. |
| `getEZForPath` | Path READ via `FSDirEncryptionZoneOp`. |
| `allowSnapshot` / `disallowSnapshot` | `checkSuperuserPrivilege`. |
| `createSnapshot` | Owner check via `FSDirSnapshotOp`. |
| `listOpenFiles` | Superuser. |
| `msync` | Active-only write category; no cross-user impact. |
| `getReconfigurationStatus` / start / list | Superuser on NN. |
| `isFileClosed` / `getContentSummary` | Path READ / traverse permission checks. |
| `fsync` / `recoverLease` | Lease / WRITE ACL (prior triage). |

### Router RBF remaining
| Area | Result |
|------|--------|
| `enterSafeMode` / `leaveSafeMode` | **Already reported** MEDIUM (no ACL). |
| `enableNameservice` / `disableNameservice` | `checkSuperuserPrivilege` (`RouterAdminServer.java:344-386`). |
| `removeMountTableEntry` | Loads existing entry; checks WRITE on **stored** owner/mode — correct; other bits on default `0755` are `r-x` only. |
| ClientProtocol forward | Destinations are nameservice IDs from MembershipStore / mounts, not client `host:port`. No forward-to-attacker-NN without membership/ZK write (already covered classes). Forwarded calls use client UGI (`RouterRpcClient` `getRemoteUser`). |

### WebHDFS + CSRF off
`dfs.webhdfs.rest-csrf.enabled=false` by default; filter not installed. Remaining ops still run as authenticated UGI with NN path ACLs. Pseudo `user.name` already reported for simple auth. CSRF against SPNEGO browser sessions is theoretical and was previously triaged below the MEDIUM bar used here (no new ACL bypass).

### DataNode HTTP
| Endpoint | Result |
|----------|--------|
| `/blockScannerReport` | Unauth stats + volume path + last block id (`BlockScanner.Servlet`, `VolumeScanner.printStats`) — info disclosure below MEDIUM vs already-open `/conf`/ImageServlet class. |
| Directory scanner | No HTTP servlet. |
| Replica recovery servlet | Not present on 3.1.3 DN HTTP (recovery is InterDatanode RPC — already reported). |

### JournalNode RPC besides QJM
`InterQJournalProtocol.getEditLogManifestFromJournal` returns segment manifest + HTTP port (`JournalNodeRpcServer.java:305-317`). KerberosInfo binds JN↔JN principals when security is on; under simple auth this is the same unauthenticated JN control-plane class as already-reported QJM/`/getJournal`. BackupNode `JournalProtocol` verifies `JournalInfo` only — same class, rarely deployed.

### libhdfs / fuse-dfs
C client / FUSE uid→`getpwuid` local trust model; no remote daemon attack chain. `Shell.bashQuote` used for group mapping (distinct from already-reported NFS `ShellBasedIdMapping` `bash -c` sink).
