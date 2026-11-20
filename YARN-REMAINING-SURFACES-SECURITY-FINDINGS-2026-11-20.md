# YARN remaining-surface security hunt (Hadoop 3.1.3)

**Repo:** `demenlee/hadoop-for-compile`  
**Scope:** YARN surfaces prior audits likely missed (localization, AuxServices, RM/Router web with ACLs on, AHS/Timeline defaults, AM/NM protocol auth, LCE/docker remaining, XSS, log-agg TFile, SharedCache beyond unauth RPC).  
**Excluded (do not re-report):** CVE-2021-33036; WebAppProxy SSRF; ResourceMappings OIS; Timeline simple HTTP / ATSv2 collector hijack; NM log IDOR + `log.type` path traversal; Federation ZK world ACL / Registry DNS / AMRMProxy tokens; ClientAM unauth stop/flex; SCM uploader no auth; Docker host-net; archive-logs bash; ShuffleHandler mapId / AMWebServices live conf / shuffle MAC; RM VIEW_APP IDOR when `yarn.acl.enable=false`; `/conf` `/logs` `/logLevel`; ZKConfigurationStore OIS; FairScheduler XXE; cgroups↔CVE-2021-33036; FSDownload tar/http SSRF class.

---

## NEW validated MEDIUM+

### 1. HIGH — Unauthenticated `LocalizationProtocol` heartbeat can poison localized resource paths (default simple auth)

**Attack path (complete):**

1. Cluster runs with default `hadoop.security.authentication=simple` (Kerberos off).
2. `ResourceLocalizationService.createServer()` only constructs `LocalizerTokenSecretManager` when `UserGroupInformation.isSecurityEnabled()` is true; otherwise the RPC server is started with `secretManager=null` (`ResourceLocalizationService.java:408-415`).
3. Localizer tokens are also only written into the NM-private credentials file under Kerberos (`writeCredentials`, `ResourceLocalizationService.java:1341-1347`). Under simple auth, any network client may call `LocalizationProtocol.heartbeat` on `yarn.nodemanager.localizer.address` (default `${yarn.nodemanager.hostname}:8040`).
4. Attacker learns a live `localizerId` (= container ID string, `ResourceLocalizationService.java:535`) via NM REST (`GET /ws/v1/node/containers`, unfiltered by default — see finding 2) or RM app/container listings.
5. While that container is localizing PRIVATE/APPLICATION resources, attacker sends heartbeats:
   - Empty status → `LocalizerRunner.findNextResource()` assigns the next download to the attacker (`ResourceLocalizationService.java:1058-1088`, `1173-1208`).
   - Or, after the real localizer has the resource in `scheduled`, attacker reports `FETCH_SUCCESS` for the same `LocalResourceRequest`.
6. On `FETCH_SUCCESS`, NM trusts the client-supplied `stat.getLocalPath()` with **no sandbox** against the path NM assigned (`ResourceLocalizationService.java:1133-1138` → `LocalizedResource.FetchSuccessTransition` at `LocalizedResource.java:247-254` → `ResourceSet.resourceLocalized` at `ResourceSet.java:125-135`).
7. Container launch symlinks that poisoned absolute path into the work dir (`ContainerLaunch` / `getLocalizedResources`). Attacker points `localPath` at a world-readable malicious jar/script under e.g. `/tmp`. Victim container executes attacker-controlled content as the application user.
8. PRIVATE cache entries stay `LOCALIZED` for the user on that NM; later containers requesting the same resource reuse the poisoned path (`LocalizedResource.LocalizedResourceTransition`).

**Authz failure:** No LocalizerToken / no per-call authorization when security is disabled; heartbeat authenticity is entirely attacker-controlled. Kerberos clusters mint and require LocalizerTokens (not exploitable without a stolen token).

**Impact:** Cross-user code execution inside victims’ containers on the NM (application user identity). Persistent per-user/per-app local cache poison until eviction.

**Distinct from excluded list:** Not CVE-2021-33036, not FSDownload tar/http SSRF, not NM log IDOR. New sink: unauthenticated localization RPC write → trusted `localPath`.

**Evidence:**

| Location | Role |
|----------|------|
| `ResourceLocalizationService.java:408-415` | Token secret manager only if Kerberos |
| `ResourceLocalizationService.java:1341-1347` | LocalizerToken only if Kerberos |
| `yarn-default.xml:1161-1162` | Localizer bind `:8040` |
| `ResourceLocalizationService.java:750-764`, `1099-1144` | Heartbeat dispatch; trusts `getLocalPath()` |
| `LocalizedResource.java:247-254` | Sets `localPath` from event |
| `ResourceSet.java:125-135`, `184-190` | Symlink map for launch |

---

### 2. MEDIUM — NM REST container/app detail endpoints skip `VIEW_APP` even when `yarn.acl.enable=true`

**Attack path:**

1. Admin enables application ACLs (`yarn.acl.enable=true`) expecting NM web to honor `VIEW_APP`.
2. `GET /ws/v1/node/containers/{containerid}` (`NMWebServices.getNodeContainer`, `NMWebServices.java:246-265`) returns full `ContainerInfo` (user, diagnostics, state, log links) with **no** `hasAccess` / `VIEW_APP` check.
3. `GET /ws/v1/node/apps/{appid}` (`getNodeApp`, `NMWebServices.java:200-211`) similarly returns `AppInfo` with no ACL check.
4. List endpoints (`/containers`, `/apps`) only filter when `yarn.webapp.filter-entity-list-by-user=true`; default is `false` (`yarn-default.xml:3557-3558`, `NMWebServices.java:126-128`, `235-237`), so lists also expose other users’ entities by default even with ACLs on.
5. Contrast: log *content* paths go through `ContainerLogsUtils.checkAccess` (already-reported IDOR class when ACLs off). These detail APIs never call that gate.

**Impact:** Cross-user container/app metadata disclosure on every NM HTTP port when operators believe ACLs are enforced; supplies container IDs for finding 1.

**Distinct from excluded list:** Not “RM VIEW_APP IDOR when `yarn.acl.enable=false`” and not NM *log content* IDOR — this is missing authz on NM entity detail/list APIs while ACLs are **on**.

**Evidence:** `NMWebServices.java:200-211`, `246-265`, `126-128`, `215-242`; `yarn-default.xml:3557-3558`; contrast `ContainerLogsUtils.java:132-146`.

---

## Checked — no new MEDIUM+ complete distinct chain

| # | Area | Result |
|---|------|--------|
| 1 | PublicLocalizer / FSDownload `file://` `http://` / ARCHIVE dest | Scheme fetch + Linux tar slip already excluded (FSDownload SSRF / CVE-2022-25168/26612 class). Dest path uses unique numeric dirs + `Path.getName()`; no new dest traversal. PUBLIC `isPublic` gates non-world-readable local files. |
| 2 | AuxServices / TimelineCollector aux | Default aux-services empty (`yarn-default.xml:1893-1895`). `mapreduce_shuffle` issues already reported. App `serviceData` only forwarded to configured plugins; no new deserializer. ATSv2 collector hijack excluded. |
| 3 | RMWebServices node-labels / scheduler-conf / reservations / node-attributes | Labels use `YarnAuthorizationProvider.isAdmin` (`yarn.admin.acl=*` default — AdminService `*` class). `scheduler-conf` requires MutableConfScheduler (not default) + admin when ACLs on. Reservations go through `checkReservationACLs`. No node-attributes REST in this 3.1.3 RM web surface. Secrets on `/scheduler-conf` GET gated admin. |
| 4 | Router REST / Federation StateStore | No distinct auth bypass beyond already-reported Federation ZK world ACL / membership hijack. |
| 5 | ApplicationHistory / Timeline v1 | `yarn.timeline-service.enabled=false` by default; when on, simple+anonymous already reported. |
| 6 | AmIpFilter / NMClientAsync / ContainerManagementProtocol | AmIpFilter redirects non-proxy IPs; cookie trust is WebAppProxy design (SSRF excluded). `ContainerManagerImpl` always requires NMToken (`authorizeUser`). |
| 7 | LCE / docker / nvidia / FPGA / runc | Docker privileged default `false`; host-net default already reported; mounts validated in `docker-util.c` via `realpath` + allow-lists. FPGA `aocl` paths admin-configured, not default. No new argv injection distinct from CVE-2021-33036. |
| 8 | YARN web stored XSS | Hamlet `__()` HTML-escapes by default (`HamletImpl.java:107`); no validated stored XSS→admin cookie chain under Kerberos. |
| 9 | Log aggregation TFile/Har | No new chain beyond VIEW_APP IDOR / Har `partName` exclusions. |
| 10 | SharedCache | Feature off (`yarn.sharedcache.enabled=false`); client/uploader unauth RPC already excluded. |

---

## Remediation (concise)

1. Always install `LocalizerTokenSecretManager` (or equivalent MAC) for `LocalizationProtocol`, including simple auth; reject heartbeats without a valid per-localizer token; verify `FETCH_SUCCESS` `localPath` is under the NM-assigned localization directory.
2. Call `hasAccess` / `VIEW_APP` on NM `getNodeContainer` / `getNodeApp` (and default `yarn.webapp.filter-entity-list-by-user=true` when ACLs are enabled).
