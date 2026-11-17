# Apache Hadoop 3.1.3 — MapReduce / Shuffle / JobHistory (beyond known) Findings

Source audit of `/agent/repos/hadoop-for-compile` (Hadoop **3.1.3**).

**Scope:** MapReduce, Job History (beyond known job.xml REST), shuffle, streaming, JobSubmitter, AM, `hadoop-mapreduce-project`.

**Excluded (already reported):** Job History REST unredacted job.xml / HsWebServices; FileUtil.unTar command injection; ConfServlet `/conf`; `/logs` endpoint.

Only MEDIUM+ issues with a complete attacker-controlled input → impact chain are listed.

---

## 1. HIGH — ShuffleHandler unsanitized `map` path traversal leaks other users' map outputs

**Severity:** HIGH  
**Location:** `hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-shuffle/src/main/java/org/apache/hadoop/mapred/ShuffleHandler.java`  
**Works on default config:** Yes (`hadoop.security.authentication=simple`; MapReduce clusters enable `mapreduce_shuffle`)

### Evidence
- Request `map` query values are split but never validated as TaskAttemptIDs; concatenated into the local path:
  - `ShuffleHandler.java:899-907` (`splitMaps`)
  - `ShuffleHandler.java:875-880` (`attemptBase = base + key.attemptId` → `.../output/` + attacker `mapId`)
  - `ShuffleHandler.java:1100-1109` (`getBaseLocation`)
- `LocalDirAllocator.getLocalPathToRead` joins onto NM local dirs with no `..` reject (`LocalDirAllocator.java:475-490`); Hadoop `Path` / `URI.normalize` resolves `..` and can leave the victim job's output tree (including into another user's `usercache`).
- HMAC auth (`verifyRequest`, `ShuffleHandler.java:1211-1234`) only proves possession of **some** job's shuffle secret for the `job=` parameter; it does not bind the resolved file to that job's app directory.
- On default simple auth, `SecureIOUtils.openForRandomRead` / `openFSDataInputStream` **skip owner checks**:
  - `SecureIOUtils.java:110-112`, `151-152`
- Shuffle listens on `0.0.0.0:13562` by default (`mapred-default.xml:1209-1210`; bind at `ShuffleHandler.java:544`).

### Attack path
1. Attacker submits any MapReduce job (receives shuffle secret via JobToken / service data) whose maps land on the same NodeManager as a victim job.
2. Attacker learns victim `user` / `application_id` / map attempt id (RM/NM web UIs, logs, or enumeration; attempt ids are structured).
3. Attacker GETs `http://<nm>:13562/mapOutput?job=<attackerJob>&map=../../../../<victimUser>/appcache/<victimApp>/output/<victimAttempt>&reduce=0` with a valid `UrlHash` HMAC for **attackerJob**.
4. ShuffleHandler resolves the path under the NM local dir into the victim's `file.out` / `file.out.index`, skips ownership checks (simple auth), and streams the victim's intermediate map output.

### Impact
Cross-tenant disclosure of MapReduce intermediate data (often the full mapped record stream) to any authenticated job submitter who can co-schedule on the victim NM. On simple-auth clusters this is not limited by Unix file ownership.

### Why existing controls fail
- Job-token HMAC authenticates the URL, not path containment under the job's own `usercache/.../output/`.
- No allow-list / `TaskAttemptID.forName` validation on `map`.
- `SecureIOUtils` owner enforcement is intentionally disabled when Kerberos is off (the shipped default).

### Default-config note
Requires the standard MapReduce shuffle aux-service (`mapreduce_shuffle`) on NMs. Cross-user reads are blocked when Kerberos is on **and** native SecureIO works (owner mismatch). Same-user other-application reads can still succeed under Kerberos.

---

## 2. HIGH — Live MR ApplicationMaster REST serves weakly redacted job conf without effective ACLs

**Severity:** HIGH  
**Location:** `hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-app/src/main/java/org/apache/hadoop/mapreduce/v2/app/webapp/AMWebServices.java`  
**Works on default config:** Yes (`mapreduce.cluster.acls.enabled=false`, StaticUserWebFilter)

### Evidence
- `GET .../ws/v1/mapreduce/jobs/{jobid}/conf` → `ConfInfo` (`AMWebServices.java:306-323`).
- `hasAccess` returns true when `remoteUser` is null, and when ACLs are disabled `JobACLsManager.checkAccess` always returns true (`AMWebServices.java:98-107`; `JobACLsManager.java:45-46`, `113-114`).
- `ConfInfo` only redacts keys listed in opt-in `mapreduce.job.redacted-properties` (empty by default); does **not** apply `hadoop.security.sensitive-config-keys` (`ConfInfo.java:42-51`; `MRJobConfUtil.java:33-37`).
- JobSubmitter still serializes the full client `Configuration` into job.xml (`JobSubmitter.java:283-293`).

### Attack path
1. Victim (or any user) submits an MR job whose client conf contains cloud keys / TLS passwords / similar.
2. While the AM is running, attacker reaches the AM webapp (directly on the container host/port, or via YARN `/proxy/<appId>/` — owner path skips the untrusted-AM warning).
3. `GET /proxy/<appId>/ws/v1/mapreduce/jobs/<jobid>/conf` (or AM-local equivalent) returns JSON/XML properties after the empty opt-in redact list.
4. Attacker replays leaked secrets against object stores / TLS endpoints.

### Impact
Same class of secret theft as the known Job History `HsWebServices` job.xml issue, but against **live** jobs (before history promotion), via the ApplicationMaster API.

### Why existing controls fail
- Default ACL-off short-circuit; null/static remote user still authorized.
- Redaction is submitter opt-in only, not ConfigRedactor / sensitive-config-keys.
- Distinct from the already-reported JHS endpoint (different process and path; same weak controls).

### Default-config note
Exploitable whenever an AM web UI is reachable (typical via RM-embedded WebAppProxy). Enabling Kerberos HTTP auth and `mapreduce.cluster.acls.enabled=true` reduces exposure but redaction remains opt-in.

---

## 3. MEDIUM — Shuffle HTTP authenticates URL only; map-output body has no MAC and SSL is off by default

**Severity:** MEDIUM  
**Location:** `hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-core/src/main/java/org/apache/hadoop/mapreduce/security/SecureShuffleUtils.java`  
**Works on default config:** Yes (`mapreduce.shuffle.ssl.enabled=false`)

### Evidence
- Fetcher / ShuffleHandler HMAC covers only `port + path + "?" + query` (`SecureShuffleUtils.java:133-135`, `99-107`; `ShuffleHandler.java:1219-1234`).
- Reply header is HMAC of the request hash string, not of the streamed spill bytes (`ShuffleHandler.java:1236-1240`).
- `mapreduce.shuffle.ssl.enabled` defaults to `false` (`mapred-default.xml:472-473`; `MRConfig.SHUFFLE_SSL_ENABLED_DEFAULT`).

### Attack path
1. On-path attacker on the shuffle network path (compromised switch, hostile co-tenant with L2/routing position, etc.) observes cleartext map-output transfers on port 13562.
2. Optionally replaces response body after the headers; reducers accept the payload because only the URL/`UrlHash` exchange is verified.

### Impact
Confidentiality and integrity failure for intermediate MapReduce data in transit whenever shuffle SSL is left at the default.

### Why existing controls fail
- Design authenticates the fetch request URL, not the content.
- Encryption is optional and off by default.

---

## Checked — no additional MEDIUM+ complete chain in scope

| Area | Result |
|------|--------|
| **ShuffleHandler JobToken / secretManager** | Tokens registered per app via aux-service data; `verifyRequest` required; no anon fetch. Path binding gap is finding #1. |
| **JobSubmitter / TokenCache** | Staging dir `0700`; shuffle AES key generated into Credentials; `printTokens` feeds known `/logs` issue only. `credentials.json` / binary token file are local client paths (trusted submitter). |
| **JobEndNotifier URL fetch** | User-configured callback from AM; runs as job user inside their container — same egress as mapper code; not privilege escalation. |
| **Streaming `PipeMapRed` / pipes `ProcessBuilder`** | Argv-array exec (no shell); runs as job user by design. |
| **ObjectInputStream / XMLDecoder in mapreduce-project** | No unsafe Java deserialization sinks found in MR production code (pipes `readObject` is Writable, not Java serialization). |
| **Runtime.exec / bash -c in mapreduce-project** | No new command-injection sinks beyond already-reported Common `FileUtil.unTar`. |
| **Job History XXE (CVE-2017-15713 class)** | `HistoryFileManager` loads conf with `restrictedParser=true` (`SUPPORT_DTD` disabled). |
| **History intermediate dirs** | Sticky `01777` base; per-user `0770` / file `0770` — no cross-user read chain found. |
| **JHS delegation tokens** | Issued only when Kerberos enabled. |
| **TaskAttemptListener / umbilical** | RPC `SecretManager` = JobTokenSecretManager. |
| **HsWebServices beyond `/conf`** | counters/tasks/attempts metadata only; secret impact remains `/conf` (already reported). |
| **Fetcher host selection** | Hosts come from AM of the same job (not a third-party SSRF primitive beyond own job). |
