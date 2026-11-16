# Apache Hadoop 3.1.3 YARN Security Findings (MEDIUM+)

Source tree: `/agent/repos/hadoop-for-compile` (Hadoop **3.1.3**).
Excluded known issues: CVE-2022-25168, CVE-2022-26612, CVE-2021-25642, CVE-2020-9492, ConfServlet `/conf`.

---

## 1. CRITICAL — CVE-2021-33036: container-executor path traversal (yarn → root)

**Severity:** CRITICAL  
**Location:** `hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/main/native/container-executor/impl/container-executor.c`

### Evidence
- `app_id` accepted without `..` rejection (`main.c` launch/initialize parsers only validate `container_id`):
  - `main.c:420-421`, `main.c:479-480`
- Log/app paths built by concatenating untrusted `app_id`:
  - `get_app_log_directory` → `container-executor.c:618-620`
  - `get_app_directory` / `NM_APP_DIR_PATTERN` → `container-executor.h:56-59`
- `create_log_dirs` calls `create_directory_for_user` **before** dropping root in `initialize_app`:
  - `container-executor.c:1284-1285` → `1125-1137`
- `create_directory_for_user` elevates to root, `mkdir`/`chown`/`chmod`:
  - `container-executor.c:984-1003`
- Missing `strstr(..., "..")` guards that the CVE fix (`a07293f`) added for `container_dir`, `tmp_dir`, `app_dir`, `app_log_dir`, `user_dir` (3.1.3 only checks log path in one buggy branch at `776-778`).

### Attack path
1. Attacker obtains the **yarn** OS identity on a NodeManager that uses setuid `LinuxContainerExecutor` (typical secure / multi-tenant install; not the default `DefaultContainerExecutor` class, but required whenever LCE is enabled).
2. Invokes `container-executor` with a crafted `app_id` containing `..` (for example `../../../../tmp/evil`) and otherwise valid args (`user`, `container_id`, local/log dirs).
3. Still running with root privileges, `create_log_dirs` / related helpers create and `chown` directories outside NM local/log roots.
4. Attacker plants a root-owned / yarn-writable path (or corrupts parent dirs) and obtains arbitrary command execution as **root** (published as CVE-2021-33036; fixed in 2.10.2 / 3.2.3 / 3.3.2).

### Impact
Full host compromise (root) on every NM that runs setuid `container-executor`.

### Why controls fail
- `validate_container_id` does not cover `app_id`, `work_dir`, or log path components.
- `check_nm_local_dir` only checks ownership of the configured root, not that the resolved path stays under it.
- Default banned-user / min-uid checks do not stop path traversal once yarn can run the binary.

### Default-config note
Requires `yarn.nodemanager.container-executor.class=LinuxContainerExecutor` and a root-owned setuid binary (standard secure-cluster setup). Default XML still ships `DefaultContainerExecutor`, so pure out-of-box insecure installs are not exposed to *this* CVE until LCE is enabled.

---

## 2. HIGH — WebAppProxyServlet open proxy / SSRF via AM tracking URL

**Severity:** HIGH  
**Location:** `hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-web-proxy/src/main/java/org/apache/hadoop/yarn/server/webproxy/WebAppProxyServlet.java`

### Evidence
- RM embeds the proxy servlet by default when proxy host equals RM web address:
  - `ResourceManager.java:1155-1167`
- AM-controlled tracking URL stored with no host allow-list:
  - `RMAppAttemptImpl.java:1836-1839` (`StatusUpdateTransition`)
- Proxy trusts that URL and builds the fetch target:
  - `WebAppProxyServlet.java:415-417`, `533` (`new URI(originalUri)`)
- Server-side fetch with circular redirects allowed; no same-host redirect strategy:
  - `WebAppProxyServlet.java:191-199`, `251`, `457`
- Authenticated viewer identity sent to the fetched host as `proxy-user`:
  - `WebAppProxyServlet.java:244-247`

### Attack path
1. Attacker submits a YARN application and registers/updates `originalTrackingUrl` to an internal target (for example `http://169.254.169.254/latest/meta-data/`, RM/NM localhost admin ports, or an attacker HTTP server).
2. Attacker (as app owner) requests `/proxy/<application_id>/` on the RM web UI — owner path skips the “untrusted AM” warning (`checkUser` only when security is on **and** viewer ≠ owner).
3. `proxyLink` performs the HTTP GET/PUT **from the RM/proxy host** and returns the body to the client; redirects may hop to further internal hosts (`ALLOW_CIRCULAR_REDIRECTS=true`).
4. If a victim later opens the same proxy URL, the proxy also forwards `Cookie: proxy-user=<victim>` to the attacker-controlled tracking host.

### Impact
- SSRF from the ResourceManager/web-proxy trust boundary into cloud metadata and internal cluster services.
- Cross-user identity disclosure via the `proxy-user` cookie when others view the app UI.

### Why controls fail
- Warning page only gates *different* users under Kerberos; owners and simple-auth clusters get a direct proxy.
- No allow-list that the tracking host must be the AM container / NM address.
- HttpClient follows cross-host redirects (later mitigated in trunk via same-host redirect strategy; absent in 3.1.3).

### Default-config note
Exploitable on default RM installs: proxy servlet is registered on the RM webapp; any authenticated (or simple-auth) submitter can set the tracking URL.

---

## 3. HIGH — ResourceMappings NM state ObjectInputStream deserialization

**Severity:** HIGH (when NM recovery is enabled)  
**Location:** `hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/main/java/org/apache/hadoop/yarn/server/nodemanager/containermanager/container/ResourceMappings.java`

### Evidence
- Unrestricted Java deserialization:
  - `ResourceMappings.java:92-99` (`AssignedResources.fromBytes` → `ObjectInputStream.readObject`)
- Loaded from NM LevelDB during container recovery:
  - `NMLeveldbStateStoreService.java:428-434`
- Persisted with `toBytes()` ObjectOutputStream:
  - `ResourceMappings.java:110-117`, `NMLeveldbStateStoreService` store path ~1463-1468

### Attack path
1. Cluster enables `yarn.nodemanager.recovery.enabled=true` (default is `false`; commonly enabled for supervised/HA NMs). State lives under `yarn.nodemanager.recovery.dir` (default `${hadoop.tmp.dir}/yarn-nm-recovery`).
2. Attacker who can write that LevelDB as the yarn user (local yarn compromise, or arbitrary file write as yarn via other bugs) plants a gadget payload under the assigned-resources key.
3. NM restart / recovery calls `fromBytes` → `readObject()`.
4. Code executes as the NodeManager (yarn) process.

### Impact
Remote/local code execution as yarn on the NodeManager once recovery state is attacker-writable.

### Why controls fail
- No allow-list / ValidatingObjectInputStream.
- Recovery bytes are treated as trusted without integrity protection beyond filesystem permissions.

### Default-config note
**Not** exploitable on stock defaults (`yarn.nodemanager.recovery.enabled=false`). Severity is HIGH for production NMs that turn recovery on.

---

## Checked — no new MEDIUM+ exploitable chain (summary)

| Area | Result |
|------|--------|
| **ContainerLaunch `/bin/bash -c` + env export** | Commands/env come from the AM `ContainerLaunchContext`. With LCE they run as the submitting user (intended). With default `DefaultContainerExecutor` they run as yarn by design (insecure mode), not a separate injection bug. Env values are double-quoted without escaping, but that only expands attacker code inside their own container identity. |
| **ResourceLocalizationService beyond unTar/zip traversal** | `FSDownload` still routes ARCHIVE through `FileUtil.unTar`/`unZip`/`RunJar.unJar`. Jar path has canonical-path checks (`RunJar.java:122-126`). Remaining tar/zip escape is the excluded CVE-2022-26612/25168 class. No additional distinct HIGH sink found in RLS itself. |
| **YARN REST IDOR / doAs** | `yarn.acl.enable` defaults to `false`; `hasAccess` allows when ACLs are off / caller null. `getCallerUserGroupInformation` uses the servlet principal (not a raw `doAs` forge when Kerberos is correctly configured). CVE-2018-11764 does **not** affect 3.1.3. Residual risk is “insecure-by-default ACLs,” not a new authz bypass. |
| **Timeline Server** | `yarn.timeline-service.enabled=false` by default. If enabled, defaults are `http-authentication.type=simple` and `simple.anonymous.allowed=true`, and `TimelineACLsManager` returns true when `yarn.acl.enable` is false — open read/write, but feature-off by default. |
| **YARN Services (slider-like) command injection** | `launch_command` is substituted and placed into the container CLC (`AbstractProviderService.buildContainerLaunchCommand`). Executes inside the service user’s containers only; no privileged shell sink in the AM. `HttpProbe` can hit arbitrary URLs from the AM process (user-level SSRF), not RM-level. |

---

## Remediation priorities
1. Upgrade off 3.1.3 to ≥ 3.2.3 / 3.3.2 (pulls in CVE-2021-33036 path checks) or newer maintained lines for the broader YARN fix set.
2. Until upgrade: ensure only yarn can execute setuid `container-executor`; treat yarn→root as one step; restrict who can become yarn.
3. Restrict WebAppProxy: do not proxy to non-AM hosts; refuse cross-host redirects; avoid sending `proxy-user` to untrusted origins.
4. If NM recovery is on, protect recovery dirs and replace `ObjectInputStream` for `ResourceMappings`.
