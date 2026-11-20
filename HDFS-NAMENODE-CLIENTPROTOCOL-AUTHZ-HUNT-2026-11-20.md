# HDFS NameNode ClientProtocol authz hunt (2026-11-20)

Repository: Apache Hadoop 3.1.3 (`demenlee/hadoop-for-compile`).  
Scope: NEW high-impact authz gaps in `NameNodeRpcServer` / `FSNamesystem` methods that mutate state or leak secrets, reachable by a normal authenticated ClientProtocol caller acting on files/blocks they do not own.

## Validated finding

### 1. HIGH — `DatanodeProtocol.commitBlockSynchronization` callable by any ClientProtocol user; no DN identity, path ACL, or lease check; `recoveryId=0` on live writes

**Severity:** HIGH  
**Primary file:** `hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/NameNodeRpcServer.java`  
**Also:** `FSNamesystem.java`, `BlockUnderConstructionFeature.java`

**Description:**  
`DatanodeProtocol` is registered on the NameNode **client** RPC server. `commitBlockSynchronization` is exposed there with **no** `verifyRequest(DatanodeRegistration)`, **no** `checkPermission` / path ACL, and **no** lease-holder check. The only gate is that `newgenerationstamp` equals the block’s `blockRecoveryId`.

For every under-construction block that is not yet in lease recovery, `blockRecoveryId` defaults to **0**. Any authenticated caller who knows (or sequentially guesses) a victim’s UC block id can therefore call:

- `commitBlockSynchronization(block, /*newGS=*/0, newLength, closeFile=true/false, deleteblock=true/false, …)`

and force-close the victim’s open file, delete its last block, or rewrite length/pipeline metadata.

If lease recovery *has* started and `recoveryId != 0`, a wrong-stamp call returns an `IOException` whose message **includes the real recovery id**, which the attacker then replays.

**Impact:** Cross-user data loss / integrity failure on in-progress writes: truncate or drop the last block, force-close another user’s file, poison pipeline locations, mark replicas corrupt on close. No ownership of the path and no lease required.

**Attack path:**

1. Authenticate to the NameNode client RPC port as any normal user (simple auth or Kerberos principal).
2. Obtain the block pool id (`versionRequest` / own `LocatedBlock`) and a recent block-id watermark (allocate a block via `create`/`addBlock`, or any readable UC file).
3. For candidate block ids, invoke `DatanodeProtocol.commitBlockSynchronization` with `newgenerationstamp=0` (default recovery id for live writes).
4. On success with `deleteblock=true` and/or `closeFile=true`, the victim’s open file loses its last block and/or is finalized under the attacker’s parameters.
5. If the exception says recovery id `R`, retry with `newgenerationstamp=R` to hijack an in-flight recovery commit.

**Evidence:**

- `NameNodeRpcServer.java:469-470` — `DatanodeProtocolPB` added to `clientRpcServer`
- `NameNodeRpcServer.java:991-999` — `commitBlockSynchronization` → namesystem only; no `verifyRequest`
- Contrast `NameNodeRpcServer.java:1526-1546` — `sendHeartbeat` / `blockReport` call `verifyRequest(nodeReg)`
- `FSNamesystem.java:3593-3673` — only `checkOperation(WRITE)` + recovery-id equality; no ACL/lease
- `FSNamesystem.java:3675-3680` — `deleteblock` removes last block
- `FSNamesystem.java:3728-3736` — `closeFile` finalizes the inode
- `BlockUnderConstructionFeature.java:60` — `blockRecoveryId = 0` default
- `BlockUnderConstructionFeature.java:232-235` — recovery id set only in `initializeBlockRecovery`
- `hadoop-policy.xml` / shipped defaults — `hadoop.security.authorization=false`; even when enabled, `security.datanode.protocol.acl=*`

**Why existing controls fail:**  
Service ACL is off by default and is `*` when on. Unlike other DN RPCs, this method never verifies a datanode registration. Lease/path checks that protect `complete` / `abandonBlock` / `updatePipeline` are absent. The recovery-id “secret” is either `0` (common case) or leaked in the error string.

**Works on shipped defaults:** Yes (simple auth; authorization disabled). Also works for any Kerberos user when authorization is enabled with default `*` DN ACL.

**Distinct from already-reported:** `registerDatanode`, `reportBadBlocks`, `/fsck` `blockIdCK`, tokens-off DataXceiver. Prior remaining-surfaces note incorrectly dismissed this as “needs matching recoveryId / DN collusion”; that missed the `recoveryId=0` default and the recovery-id leak in the exception text.

---

## Focus methods triaged (no NEW MEDIUM+)

| Method | Why it failed triage |
| --- | --- |
| `recoverLease` | `FSPermissionChecker` + `checkPathAccess(WRITE)` then force recovery — needs write ACL on the path (by design). |
| `fsync` | `checkLease(clientName)`; clientName is `DFSClient_<task>_<randomInt>_<tid>` — not guessable; no path ACL bypass. |
| `concat` | `FSDirConcatOp` checks WRITE on target and READ/parent-WRITE on sources. |
| `truncate` | `FSDirTruncateOp` checks `FsAction.WRITE` before truncate/lease recovery. |
| `updatePipeline` | `checkUCBlock` requires matching lease holder string (random). |
| `updateBlockForPipeline` / `bumpBlockGenerationStamp` | Same lease-holder gate; returns WRITE block token only to holder. |
| `getAdditionalDatanode` | `checkLease`; COPY token only for lease holder. |
| `abandonBlock` / `complete` / `addBlock` | Lease (and path resolution) gated via `FSDirWriteFileOp` / `checkLease`. |
| `setQuota` | `FSDirAttrOp.setQuota` → `pc.checkSuperuserPrivilege()`. |
| `addCacheDirective` / `modifyCachePool` | `FSNDNCacheOp` enforces pool ACLs / superuser as applicable. |
| `metaSave` / `setSafeMode` / `saveNamespace` / `rollEdits` / `refreshNodes` / `setBalancerBandwidth` | `checkSuperuserPrivilege` in `FSNamesystem`. |
| `getDelegationToken` | Issues token for **caller** UGI only; requires Kerberos/web auth op. |
| `createEncryptionZone` / `listEncryptionZones` | Superuser. |
| `getEZForPath` | Path `READ` via `FSDirEncryptionZoneOp`. |
| `getDataEncryptionKey` | Unchecked but returns a **fresh nonce-derived** per-call key when `dfs.encrypt.data.transfer` is on (default **false**); not a master-key leak / not cross-user file access. |
| `getBlockLocations` | Path `READ` via `FSDirStatAndListingOp`. |
| `reportBadBlocks` | **Already reported** — no ACL; excluded. |

## Other negatives from this pass

- `renewLease(holder)` has no holder ownership check, but without a clientName oracle impact is limited to extending an already-known lease; clientName is not exposed to non-superusers (`listOpenFiles` is superuser-only). Below MEDIUM without a separate leak.
- `refreshUserToGroupsMappings` / `refreshSuperUserGroupsConfiguration` lack superuser checks but only reload local NN config (cache refresh / DoS class, not cross-user file mutation).
- `versionRequest` leaks `NamespaceInfo` (cluster/block pool ids) only — assists step 2 above but is not a standalone MEDIUM+ finding.
