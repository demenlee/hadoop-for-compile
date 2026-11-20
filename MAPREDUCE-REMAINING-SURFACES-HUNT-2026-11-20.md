# MapReduce remaining-surface hunt (2026-11-20)

Repo: `demenlee/hadoop-for-compile` @ Hadoop **3.1.3**.

**Scope:** JobHistory Server HTTP/REST (beyond known `/conf`), HsAdmin, AM web (beyond known live conf), ShuffleHandler remaining, JobClient/TaskUmbilicalProtocol, HSAdminProtocol Refresh*, Streaming/Pipes (new chains only), HistoryFileManager permissions, JHS AuthenticationFilter.

**Excluded (do not re-report):** ShuffleHandler `map` path traversal; AMWebServices live `/conf`; CVE-2021-33036; FileUtil.unTar; WebAppProxy SSRF; HttpServer2 `/conf` `/logs` `/logLevel`; `yarn.acl.enable=false` VIEW_APP IDOR; prior JHS REST `HsWebServices` `/conf` job.xml; `getJobAttempts` VIEW_JOB skip (already flagged); HSAdmin `admin.acl=*`.

---

## NEW validated MEDIUM+ findings

### 1. HIGH — HistoryFileManager accepts attacker-planted intermediate history (no owner / dirname binding)

**Severity:** HIGH  
**Location:** `hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-hs/src/main/java/org/apache/hadoop/mapreduce/v2/hs/HistoryFileManager.java`  
**Title:** Intermediate done-dir history files are trusted by filename only; first writer wins and duplicate authentic files are deleted  
**Works on shipped defaults:** Yes (`done_intermediate` is world-writable sticky `01777`; scan every 180s)

#### Description
The JobHistory Server periodically scans `${mapreduce.jobhistory.intermediate-done-dir}` user subdirectories and promotes `*.jhist` / `*_conf.xml` into the done tree. `scanIntermediateDirectory` builds `JobIndexInfo` solely from the **filename** (`FileNameIndexUtils.getIndexInfo`) and never checks:

- that the HDFS file owner matches the username encoded in the filename, or
- that the parent directory name matches that user, or
- that the caller who wrote the file was the job owner.

`JobListCache.addIfAbsent` keys only on `JobId`. The first `HistoryFileInfo` for a given job id is moved to done; a later authentic file for the same id is treated as a duplicate and **deleted**.

Any MapReduce user who can write under the shared intermediate tree (every job submitter; base dir is `01777`) can plant a well-named history/conf pair for a victim `job_*` id (enumerable from the RM) under **their own** user subdirectory before the victim AM finishes. After the next JHS scan, the forged entry is canonical and the real history is discarded.

#### Impact
Cross-user **integrity** failure of Job History: forge or replace another user's job history and conf as served by JHS REST/UI/`downloadconf`; destroy the authentic audit trail. Job “owner” used for VIEW_JOB ACL checks on the loaded `CompletedJob` is taken from the attacker-controlled filename user field.

#### Attack path
1. Deploy/use a multi-tenant cluster with the default intermediate history layout (`…/history/done_intermediate`, mode `01777`).
2. Observe or predict victim `application_*` / `job_*` from the RM UI/API.
3. As any HDFS/MR user, write into `done_intermediate/<attacker>/` a `*.jhist` (and matching `*_conf.xml`) whose filename encodes the **victim job id** and desired user via `FileNameIndexUtils` format (copy/rename from a prior job run).
4. Wait for JHS `scanIntermediateDirectory` (default `mapreduce.jobhistory.move.interval-ms=180000`) to `addIfAbsent` + `moveToDone`.
5. When the victim AM later publishes real history for the same job id, JHS deletes it as a duplicate (`HistoryFileManager.java` duplicate branch).
6. Clients reading JHS for that job id receive the attacker-controlled history/conf.

#### Evidence
- `HistoryFileManager.java:919-936` — scan all intermediate user dirs; no ownership check  
- `HistoryFileManager.java:945-1001` — parse index from filename only; `addIfAbsent`; duplicate → `fileInfo.delete()`  
- `HistoryFileManager.java:213-219` — cache keyed only by `JobId`  
- `HistoryFileManager.java:478-484` — `CompletedJob` user from `jobIndexInfo.getUser()` (filename)  
- `FileNameIndexUtils.java:131-155` — user/jobId from filename string  
- `JobHistoryUtils.java:91-92` — intermediate base `01777`  
- `JobHistoryEventHandler.java:235-244` — user dir created only if missing (no ownership reclaim)  
- `mapred-default.xml:1718-1719`, `1821-1824` — default paths / 180s scan  

#### Why existing controls fail
Sticky `01777` only prevents deleting *other users' directory entries* in the base dir; it does not bind file content to job ownership. Per-user `0770` dirs still let each user create arbitrarily named files. Filename-derived metadata is trusted for id, user, and ACL owner.

#### Remediation
Before `addIfAbsent` / `moveToDone`, require HDFS owner (and preferably parent dir name) to match the username in the history filename and reject mismatched job ids vs parsed content. Prefer first-write only from the job owner's directory. Consider reducing intermediate base permissions and using a JHS-only ingest path.

---

### 2. HIGH — JHS `/downloadconf` streams raw job.xml with zero redaction

**Severity:** HIGH  
**Location:** `hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-app/src/main/java/org/apache/hadoop/mapreduce/v2/app/webapp/AppController.java`  
**Title:** Job History `downloadConf` attachment bypasses `MRJobConfUtil.redact` (and `hadoop.security.sensitive-config-keys`)  
**Works on shipped defaults:** Yes (`mapreduce.cluster.acls.enabled=false`, StaticUserWebFilter)

#### Description
Distinct from the already-reported HsWebServices REST `/conf` (`ConfInfo` + opt-in `mapreduce.job.redacted-properties`). The JHS HTML app registers `/downloadconf/:jobid` → `AppController.downloadConf` → `writeJobConf`, which calls `job.loadConfFile()` then `Configuration.writeXml` with **no** `MRJobConfUtil.redact` call.

`ConfBlock` (UI table) uses `ConfInfo` (redacts listed keys) but prominently links to `/jobhistory/downloadconf/<jid>` for the “full” file — so configuring `mapreduce.job.redacted-properties` can sanitize the table/REST while the download link still returns secrets.

On default ACL-off + StaticUser (`dr.who`), `requireJob()` authorization is a no-op, so any HTTP client who can reach the JHS webapp can download another user's full job.xml.

#### Impact
Cross-user theft of secrets embedded in submitted job configuration (cloud keys, TLS passwords, tokens, etc.), including after operators attempt remediation via `mapreduce.job.redacted-properties`.

#### Attack path
1. Victim submits an MR job whose client `Configuration` contains secrets (serialized into history conf).
2. Job completes; conf is available on JHS.
3. Attacker GETs `http://<jhs>:19888/jobhistory/downloadconf/job_<cluster>_<id>` (default StaticUser identity).
4. Response is `Content-Disposition` attachment of unredacted XML (`writeXml`).

#### Evidence
- `HsWebApp.java:53-54` — route `/downloadconf`  
- `AppController.java:330-358` — `downloadConf` / `writeJobConf` (no redact)  
- `ConfInfo.java:45-47` — contrast: REST/UI path calls `MRJobConfUtil.redact`  
- `ConfBlock.java:71-73` — UI links to unredacted download  
- `MRJobConfUtil.java:33-37` — redact only opt-in property list  
- `mapred-default.xml:1069-1070` — ACLs off by default  
- `core-default.xml` — `StaticUserWebFilter` default HTTP filter  

#### Why existing controls fail
Redaction is applied only in `ConfInfo`, not in `writeJobConf`. Job ACLs default off, so `requireJob()` does not constrain the static web user.

#### Remediation
Call the same redaction (and ideally `ConfigRedactor` / `hadoop.security.sensitive-config-keys`) before `writeXml`. Prefer not offering a raw download, or require admin + ACLs. Enable `mapreduce.cluster.acls.enabled` and Kerberos HTTP auth on JHS.

---

## Surfaces checked — no additional NEW MEDIUM+ chain

| Surface | Result |
|--------|--------|
| **HsWebServices** counters/tasks/attempts/conf REST | `/conf` already reported; other endpoints use `checkAccess` except `getJobAttempts` (**already flagged**). `getJobs` lists PartialJob metadata without VIEW_JOB / without honoring `yarn.webapp.filter-entity-list-by-user` (HTML-only filter) — metadata-only; below MEDIUM vs placement leaks. |
| **HsController `logs` / `nmlogs`** | `logs` uses `requireJob`; aggregated log bodies use YARN `ApplicationACLsManager` (VIEW_APP). Same ACL-off class as excluded IDOR when `yarn.acl.enable=false`. |
| **JHS AuthenticationFilter / CSRF** | Default filter = StaticUser only; CSRF off — enables finding #2 but not a separate RCE. Spnego keys wired only when Kerberos principals configured. |
| **HSAdminProtocol Refresh*** | Methods call `checkAcls`; default `mapreduce.jobhistory.admin.acl=*` already classified with AdminService wildcard. `refresh*` reloads **server** `Configuration` from disk (not client-supplied) — no remote max-age injection. `getGroupsForUser` has no ACL (group enum) — recon only, below MEDIUM. |
| **ShuffleHandler remaining** | Beyond known `map` traversal + URL-only HMAC: `reduce` bounds-checked via `IndexCache`; `user` from registered job token map; no new path/query sink. Index cache keyed by attempt id (includes job id). |
| **TaskUmbilicalProtocol / JobToken** | RPC built with `JobTokenSecretManager`; per-AM secret. Methods trust attempt ids within the authenticated job (shared job token by design). `getTask` JVM map is registration-gated; TODO on container binding is same-job only — no cross-user chain. |
| **JobClient / HS client RPC** | `HistoryClientService.verifyAndGetJob` enforces VIEW_JOB when ACLs on. DT issue requires Kerberos when security enabled; simple-auth DT path does not exceed existing simple-auth identity spoof. |
| **HistoryFileManager permissions (confidentiality)** | Done/intermediate files `0770`; cross-user **read** needs shared group (intended for JHS service group). Sticky base stops unlinking peer dirs. Confidentiality negative; integrity is finding #1. |
| **Streaming / Pipes** | `PipeMapRed` / pipes `ProcessBuilder` argv-array, job-user. No new chain vs LCE/CVE-2021-33036. |
| **JobEndNotifier** | User-configured URL fetched from AM as job user — same egress as task code; not cross-user priv-esc. |
| **AMWebServices** (non-conf) | `getJobAttempts` already flagged; kill PUT checks VIEW then MODIFY via `MRClientService`. `blacklistednodes`/`info` non-sensitive. Live `/conf` already reported. |

---

## Summary

Two NEW HIGH findings on MapReduce/JHS surfaces not covered by the excluded known classes: **intermediate history forgery via filename-only trust**, and **`/downloadconf` unredacted job.xml** (redaction bypass relative to `ConfInfo`).
