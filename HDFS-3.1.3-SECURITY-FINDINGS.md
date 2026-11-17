# Apache Hadoop 3.1.3 — HDFS NameNode/DataNode/JournalNode/ZKFC/WebHDFS Server Findings

Source audit of `/agent/repos/hadoop-for-compile` (Hadoop 3.1.3). Scope: HDFS NameNode, DataNode, JournalNode, ZKFC, WebHDFS **server**, Secondary NameNode HTTP. Excluded (already reported): WebHDFS client Location redirect (CVE-2020-9492), HttpFS simple auth + shipped cookie secret, NFS AUTH_SYS + `* rw`, ConfServlet `/conf`, `/logs`, KMS wildcard ACLs.

Only MEDIUM+ issues with a validated attacker-controlled input → impact chain are listed. Defaults are from `hdfs-default.xml` / `core-default.xml`.

---

## 1. CRITICAL — Unauthenticated FSImage/edits transfer via NameNode `/imagetransfer`

**Severity:** CRITICAL  
**Location:** `hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/ImageServlet.java`  
**Works on default config:** Yes (`hadoop.security.authentication=simple`)

### Description
`NameNodeHttpServer` mounts `ImageServlet` at `/imagetransfer` as an internal servlet with `requireAuth=true`. `HttpServer2.addInternalServlet` only attaches the SPNEGO filter when Kerberos is enabled. With the shipped default (`hadoop.security.authentication=simple`), no authentication filter is applied.

`ImageServlet.validateRequest` also short-circuits the requestor allow-list unless `UserGroupInformation.isSecurityEnabled()` is true. Therefore any network client can:

- **GET** `getimage=1&txid=latest` (or a concrete txid) and download the full FSImage (entire namespace metadata, block IDs, locations, permissions, encryption-zone keys references).
- **GET** edit segments (`getedit=1&startTxId=&endTxId=`).
- **PUT** a crafted checkpoint image (must supply matching `storageInfo`) into the active NameNode’s storage. On a subsequent NameNode restart, `FSImage.loadFSImage` selects the latest checkpoint via `inspector.getLatestImages()`, so a higher-txid attacker image can become the loaded namespace.

The same servlet is mounted on the Secondary NameNode HTTP port (`0.0.0.0:9868`).

### Impact
Complete HDFS metadata theft without any identity. Combined with finding #3 (block tokens off), that metadata is enough to read every block from DataNodes. PUT elevates this to persistent namespace poisoning across restart.

### Attack path
1. Reach NameNode HTTP (`dfs.namenode.http-address` default `0.0.0.0:9870`).
2. `GET http://<nn>:9870/imagetransfer?getimage=1&txid=latest` — no auth headers required.
3. Parse the FSImage for paths, block pool ID, block IDs, generation stamps, and DataNode placements.
4. (Optional) Learn `storageInfo` from the image/VERSION material, then `PUT` a higher-txid image to `/imagetransfer` to stage a malicious checkpoint for the next NN restart.
5. Same GET against Secondary NN `:9868/imagetransfer` if SNN is deployed.

### Evidence
- `ImageServlet.java:71` — `PATH_SPEC = "/imagetransfer"`
- `ImageServlet.java:185-196` — requestor check only if `isSecurityEnabled()`
- `ImageServlet.java:92-118` — GET serves FSImage including `txid=latest`
- `ImageServlet.java:474-486` — PUT uses the same `validateRequest`
- `NameNodeHttpServer.java:299-300` — servlet registered with `requireAuth=true`
- `HttpServer2.java:880-888` — SPNEGO filter added only when security enabled
- `SecondaryNameNode.java:496-497` — same servlet on SNN
- `hdfs-default.xml:164-165` — NN HTTP `0.0.0.0:9870`
- `core-default.xml:69-70` — `hadoop.security.authentication=simple`

### Why existing controls fail
`dfs.permissions.enabled=true` never runs for this servlet (no UGI permission check on the image file transfer). Kerberos requestor ACLs exist but are disabled on the default simple-auth cluster. Internal-servlet registration intentionally skips user-facing filters.

### Remediation
Require authentication for `/imagetransfer` even when `hadoop.security.authentication=simple` (shared secret / mTLS / always-on SPNEGO). Keep the requestor allow-list active regardless of `isSecurityEnabled()`. Bind NN/SNN HTTP to a private interface. Prefer QJM HA with authenticated HTTP. Treat any cluster that exposed `:9870/imagetransfer` without Kerberos as metadata-compromised; rotate encryption keys and review block access.

---

## 2. CRITICAL — WebHDFS server trusts `user.name` (Pseudo auth) and grants HDFS superuser

**Severity:** CRITICAL  
**Location:** `hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/web/AuthFilter.java`  
**Works on default config:** Yes

### Description
WebHDFS is always initialized on the NameNode (`NameNodeHttpServer.initWebHdfs`). `AuthFilter` (default `dfs.web.authentication.filter`) selects `PseudoAuthenticationHandler` when security is off and **forces** `simple.anonymous.allowed=true` if unset.

`JspHelper.getUGI` (used by `UserProvider` and by `/fsck`) trusts the `user.name` query parameter whenever Kerberos is off and builds `UserGroupInformation.createRemoteUser(remoteUser)`. HDFS superuser is the NameNode process principal (`fsOwner = UserGroupInformation.getCurrentUser()` at NN start) or any user in `dfs.permissions.superusergroup` (default `supergroup`). Claiming `user.name=<nn-os-user>` (commonly `hdfs`) therefore bypasses all HDFS permission checks.

The NameNode explorer UI (`explorer.js`) is a thin WebHDFS client on the same port, so the browser file browser inherits the same trust model.

`/fsck` is an internal servlet that also calls `JspHelper.getUGI` and will run as the impersonated user (including `?locations=1` block maps and `?delete=1` on corrupt paths).

### Impact
Unauthenticated full read/write/delete of the HDFS namespace as the HDFS superuser via REST, plus UI browsing and fsck-assisted block-map disclosure.

### Attack path
1. `GET http://<nn>:9870/webhdfs/v1/?op=LISTSTATUS&user.name=hdfs` (or the actual NN OS user).
2. `GET/PUT/DELETE` any path with `op=OPEN|CREATE|MKDIRS|DELETE|GETFILESTATUS|...&user.name=hdfs`.
3. Optionally open `http://<nn>:9870/explorer.html` and use the same WebHDFS endpoints.
4. Optionally `GET http://<nn>:9870/fsck?path=/&locations=1&user.name=hdfs` for block placements.

### Evidence
- `AuthFilter.java:66-72` — Pseudo auth + anonymous forced on
- `PseudoAuthenticationHandler.java:181-196` — `user.name` becomes the auth token
- `JspHelper.java:132-140` — security-off path trusts query username
- `UserProvider.java:54-55` — injects UGI into WebHDFS methods
- `NamenodeWebHdfsMethods.java` — all ops run in `ugi.doAs(...)`
- `FSNamesystem.java:769-772` — `fsOwner` / `supergroup` define superuser
- `FsckServlet.java:56-69` — fsck under impersonated UGI
- `explorer.js` — calls `/webhdfs/v1/...`
- `core-default.xml:69-70` — authentication `simple`
- `hdfs-default.xml:164-165` — HTTP bound `0.0.0.0:9870`

### Why existing controls fail
`dfs.permissions.enabled=true` only compares the *claimed* user to file ACLs; it does not authenticate that claim. Proxy-user rules are irrelevant because the attacker is not going through `doAs` — they directly set `user.name` to the superuser. This is distinct from HttpFS (separate gateway already reported).

### Remediation
Set `hadoop.security.authentication=kerberos` and WebHDFS SPNEGO; set `dfs.web.authentication.simple.anonymous.allowed=false`. Do not expose NN HTTP to untrusted networks while using simple auth. There is no safe way to run WebHDFS in simple mode on a reachable interface.

---

## 3. HIGH — DataNode serves blocks without access tokens (default off)

**Severity:** HIGH  
**Location:** `hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/datanode/DataXceiver.java`  
**Works on default config:** Yes (`dfs.block.access.token.enable=false`)

### Description
`dfs.block.access.token.enable` defaults to `false`. `DataXceiver.checkAccess` only invokes `blockPoolTokenSecretManager.checkAccess` when `datanode.isBlockTokenEnabled` is true. With tokens disabled, `readBlock` / `writeBlock` / `copyBlock` / `replaceBlock` accept any client that can speak the data-transfer protocol on `dfs.datanode.address` (`0.0.0.0:9866`).

`dfs.encrypt.data.transfer` also defaults to `false`, so block bytes go in cleartext.

(When Kerberos is enabled, DataNode startup aborts if block tokens are off — so this is specifically the default/simple and any misconfigured non-Kerberos deployment.)

### Impact
Given a block pool ID + block ID + genstamp (from finding #1 ImageServlet, finding #2 WebHDFS/fsck, or other metadata leaks), any network attacker reads or overwrites block replicas directly on DataNodes, bypassing HDFS path permissions entirely.

### Attack path
1. Obtain block coordinates (e.g. FSImage from `/imagetransfer`, or `GET /webhdfs/v1/<file>?op=GET_BLOCK_LOCATIONS&user.name=hdfs`, or `/fsck?locations=1`).
2. Open TCP to DataNode `:9866`.
3. Send `Op.READ_BLOCK` with a dummy/empty block token — `checkAccess` returns without verification.
4. Receive file bytes. `WRITE_BLOCK` follows the same skipped check.

### Evidence
- `hdfs-default.xml:520-521` — `dfs.block.access.token.enable=false`
- `hdfs-default.xml:125-126` — DataNode `0.0.0.0:9866`
- `hdfs-default.xml:2144-2145` — `dfs.encrypt.data.transfer=false`
- `DataXceiver.java:579-580` — `readBlock` → `checkAccess`
- `DataXceiver.java:717-719` — `writeBlock` → `checkAccess`
- `DataXceiver.java:1429-1457` — token verification gated on `isBlockTokenEnabled`
- `DFSConfigKeys.java:745` — default `false`

### Why existing controls fail
Path ACLs live only on the NameNode. DataNodes rely on block tokens as the capability system; that system is off by default. Wire encryption is also off, so on-path observers can additionally sniff DN traffic.

### Remediation
Set `dfs.block.access.token.enable=true` (mandatory with Kerberos; strongly recommended even for simple auth on any shared network). Set `dfs.encrypt.data.transfer=true` (or `dfs.data.transfer.protection`) for clusters that traverse untrusted networks. Restrict DataNode data ports to the cluster network.

---

## 4. HIGH — JournalNode `/getJournal` serves edit logs without auth when security is off

**Severity:** HIGH  
**Location:** `hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/qjournal/server/GetJournalEditServlet.java`  
**Works on default config:** Yes for QJM/HA deployments (servlet auth gated the same way as ImageServlet; JN HTTP default `0.0.0.0:8480`)

### Description
`JournalNodeHttpServer` mounts `GetJournalEditServlet` at `/getJournal` with `requireAuth=true`, but SPNEGO is only added under Kerberos. `checkRequestorOrSendError` returns success immediately unless `isSecurityEnabled()`. Unit tests fetch edits with a plain `URL` and no credentials.

Edit logs contain namespace mutations and, for some operations, file data payloads — enough to reconstruct recent creates/appends and to assist FSImage-based theft.

### Impact
Unauthenticated download of HA edit segments from any JournalNode HTTP port when the cluster uses QJM without Kerberos.

### Attack path
1. Reach JournalNode HTTP (`0.0.0.0:8480`).
2. Discover or guess `jid` (nameservice/journal cluster id from config, JMX, or naming convention).
3. `GET http://<jn>:8480/getJournal?segmentTxId=1&jid=<journalId>` (storageInfo optional for the auth bypass; mismatch only fails if provided incorrectly).
4. Replay returned edit bytes.

### Evidence
- `GetJournalEditServlet.java:143-151` — auth only if security enabled
- `GetJournalEditServlet.java:182-238` — serves edit file over HTTP
- `JournalNodeHttpServer.java:84-85` — servlet registration
- `TestJournalNode.java:335-342` — HTTP get without credentials succeeds
- `hdfs-default.xml:2252-2253` — `0.0.0.0:8480`

### Why existing controls fail
Same pattern as ImageServlet: requestor allow-list and SPNEGO are Kerberos-only. `/conf` lockdown (if applied) does not cover `/getJournal`.

### Remediation
Enable Kerberos for JournalNode HTTP, or require an equivalent authn gate for `/getJournal` in simple mode. Bind JN HTTP to private interfaces. Restrict JN ports to NameNode hosts.

---

## 5. HIGH — ZKFC HA locks use world-writable ZooKeeper ACLs by default

**Severity:** HIGH  
**Location:** `hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/ha/ZKFailoverController.java`  
**Works on default config:** Default whenever automatic HA failover is configured (`ha.zookeeper.acl` ships as `world:anyone:rwcda`)

### Description
`ZKFailoverController.initZK` parses `ha.zookeeper.acl` with default `world:anyone:rwcda` (also present in `core-default.xml`). `ActiveStandbyElector` creates `ActiveStandbyElectorLock` (ephemeral) and `ActiveBreadCrumb` under `/hadoop-ha/<service>` using that ACL and empty `ha.zookeeper.auth` by default.

Any ZooKeeper client can delete or overwrite the lock/breadcrumb znodes. Deleting the active lock forces session loss / failover; repeatedly doing so produces failover flapping and fencing storms. Writing a fake lock/breadcrumb can confuse fencing and recovery.

### Impact
Cluster-wide HDFS HA availability attack (forced failover / DoS). Depending on fencing configuration, this can also contribute to split-brain risk. No HDFS admin credentials required — only ZK client reachability.

### Attack path
1. Operators enable HDFS HA + ZKFC and leave `ha.zookeeper.acl` at the shipped value.
2. Attacker connects to the ZK ensemble (no digest/SASL required).
3. Delete `/hadoop-ha/<nameservice>/ActiveStandbyElectorLock` (and/or overwrite `ActiveBreadCrumb`).
4. Active NN loses the lock; ZKFC triggers failover. Repeat for sustained outage.

### Evidence
- `ZKFailoverController.java:68-69` — `ZK_ACL_DEFAULT = "world:anyone:rwcda"`
- `ZKFailoverController.java:335-357` — ACL applied to `ActiveStandbyElector`
- `ActiveStandbyElector.java:140-142, 1014` — lock znode created with `zkAcl`
- `core-default.xml:2236-2237` — `ha.zookeeper.acl=world:anyone:rwcda`
- `core-default.xml:2249-2250` — `ha.zookeeper.auth` empty by default

### Why existing controls fail
HDFS Kerberos (if later enabled for RPC) does not change the ZK ACL used by ZKFC unless operators also set `ha.zookeeper.acl` / `ha.zookeeper.auth`. World ACL is the documented default.

### Remediation
Set `ha.zookeeper.acl` to a digest or SASL ACL limited to ZKFC/NN principals, configure matching `ha.zookeeper.auth`, and network-restrict ZooKeeper to HA hosts. Recreate HA znodes after rotating ACLs.

---

## Negatives / not reported

| Topic | Result |
| --- | --- |
| `dfs.permissions.enabled` | Default **true** — not a vulnerability |
| Block tokens with Kerberos | DataNode **aborts** if tokens disabled under Kerberos (`DataNode.checkSecureConfig`) |
| WebHDFS client redirect | Already reported (CVE-2020-9492) — out of scope |
| HttpFS / NFS / `/conf` / `/logs` / KMS | Already reported — out of scope |
| `dfs.encrypt.data.transfer=false` alone | Defense gap; needs MITM — not filed as a standalone MEDIUM+ without the token-off chain above |
| JournalNode `/conf` `/jmx` | Covered by existing ConfServlet finding |

## Complete default-config kill chain (summary)

```
Attacker network
  → GET :9870/imagetransfer?getimage=1&txid=latest   (finding 1)
  → parse block IDs / DN locations
  → TCP :9866 READ_BLOCK without token               (finding 3)
  → full HDFS file contents

OR

  → GET :9870/webhdfs/v1/...?user.name=<nn-user>     (finding 2)
  → superuser OPEN/CREATE/DELETE entire namespace
```
