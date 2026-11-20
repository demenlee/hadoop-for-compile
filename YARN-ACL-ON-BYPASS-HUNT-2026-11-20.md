# YARN ACL-ON authorization bypass hunt (2026-11-20)

Repository: Apache Hadoop 3.1.3 (`hadoop-for-compile`).  
Scope: NEW YARN REST/RPC authorization bypasses that still fire when `yarn.acl.enable=true`.

## NEW validated MEDIUM+

### 1. MEDIUM — RM `GET /ws/v1/cluster/scheduler/app-activities` skips VIEW_APP

**Primary file:**  
`hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org/apache/hadoop/yarn/server/resourcemanager/webapp/RMWebServices.java`

**Description:**  
With CapacityScheduler (the default), `getAppActivities` accepts any `appId`, calls `turnOnAppActivitiesRecording`, and returns allocation activity for that application. It never calls `hasAccess` / `ApplicationAccessType.VIEW_APP`. Sibling detail paths that return placement data do enforce access: `getApp` gates AM host/container behind `hasAccess`, and `getAppAttempt` / `getContainers` / `getContainer` go through `ClientRMService`, which throws when VIEW_APP fails.

**Impact:**  
Any authenticated (or simple-auth) caller who can reach the RM webapp can, for another user’s app when ACLs are ON:

1. Force the RM to start recording that app’s scheduler activities (`turnOnAppActivitiesRecording`).
2. Read `nodeId`, `allocatedContainerId`, queue, priority, allocation state, and diagnostics once allocations are recorded.

That is container/node placement used for follow-on (NM targeting), beyond the non-sensitive fields `AppInfo` still returns when `hasAccess=false`. Works with default `yarn.webapp.filter-entity-list-by-user=false` (appIds listed) and still works when that filter is true if the attacker can guess/obtain an `applicationId`.

**Attack path:**

1. Operator sets `yarn.acl.enable=true` and applications publish restrictive VIEW_APP ACLs.
2. Attacker obtains a victim `applicationId` (cluster apps list, or guess cluster-timestamp + counter).
3. `GET /ws/v1/cluster/scheduler/app-activities?appId=<victimAppId>` (optional `maxTime`).
4. Response eventually includes allocation entries with `nodeId` and `allocatedContainerId` for the victim app; RM also begins recording activities for that app.

**Evidence:**

- `RMWebServices.java:640-687` — no `hasAccess`; calls `turnOnAppActivitiesRecording` then `getAppActivitiesInfo`
- `RMWebServices.java:257-274` — `hasAccess` / VIEW_APP used elsewhere
- `RMWebServices.java:800-801` — `getApp` passes `hasAccess` into `AppInfo`
- `RMWebServices.java:834-866` — attempt/container detail delegated to `ClientRMService` (ACL enforced)
- `ClientRMService.java:405-433`, `475-507`, `517-550` — VIEW_APP hard-deny on attempt/container RPCs
- `AppInfo.java:183-200` — AM host / container logs gated by `hasAccess`
- `AppAllocationInfo.java:39-60` — leaks `nodeId`, `allocatedContainerId`, diagnostics
- `ActivitiesManager.java:105-109`, `73-84` — recording + return path
- `yarn-default.xml:383-384` — default CapacityScheduler
- `yarn-default.xml:269-270`, `3557-3558` — ACL-on target; list filter default false

---

## Checked — failed triage / not NEW MEDIUM+

### RMWebServices

| Surface | Result |
|---|---|
| `getAppAttempt` / `getContainers` / `getContainer` | Enforced via `ClientRMService` VIEW_APP when ACLs on |
| `getAppAttempts` | **Already reported** — skips `hasAccess`, leaks AM node/container |
| `updateAppState` (kill) / priority / queue / timeouts PUT | `doAs` → `ClientRMService` MODIFY_APP |
| `getAppState` / `getAppPriority` / `getAppQueue` / `getAppTimeouts` | No VIEW_APP, but same or weaker than ungated `AppInfo` fields / list metadata |
| Delegation tokens | Kerberos channel required (`createKerberosUserGroupInformation`) |
| Node-label mutate | `nodeLabelManager.checkAccess` |
| `scheduler-conf` GET/PUT | Admin ACL via `initForWritableEndpoints(..., true)` |
| Reservations | `ClientRMService.checkReservationACLs` under `doAs` |
| Nodes / cluster / scheduler info | Cluster status, not app VIEW_APP bypass |
| `getActivities` (node) | No admin gate; cluster scheduler dump — not a per-app VIEW_APP bypass of the getAppAttempts class |

### AHSWebServices / ApplicationHistoryManagerOnTimelineStore

| Surface | Result |
|---|---|
| `getAppAttempt` / `getContainers` / `getContainer` | `checkAccess` before return |
| `getLogs` / `getContainerLogsInfo` | **Already reported** — swallows authz and still serves aggregated logs |
| Other swallow in `generateApplicationReport` | Soft-redacts host/tracking on VIEW_APP deny; enables the already-reported log path, no new secret channel beyond that |

### TimelineWebServices / TimelineDataManager

| Surface | Result |
|---|---|
| `postEntities` / `putDomain` | Requires caller UGI; MODIFY_APP / owner checks when timeline ACLs on |
| Reads | Filtered by TimelineACLsManager VIEW_APP when ACLs on |
| Timeline HTTP simple+anonymous | **Already reported** (independent of yarn.acl.enable) |

### HsWebServices

| Surface | Result |
|---|---|
| `getJobAttempts` | **Already reported** |
| Other job/task/attempt/counter/conf endpoints | `checkAccess(VIEW_JOB)` |
| `getJobs` list | Partial job metadata only; no attempt/NM location |

### AMWebServices (live MR AM)

| Surface | Result |
|---|---|
| `getJobAttempts` | Same skip pattern as JHS, but gated by `mapreduce.cluster.acls.enabled` (default **false**), not `yarn.acl.enable`. Treat as same class as already-reported JHS finding when MR ACLs are separately enabled; not a yarn.acl.enable finding. |
| Task-attempt kill PUT | REST checks VIEW_JOB then `MRClientService` enforces MODIFY_JOB |

### YARN Router

| Surface | Result |
|---|---|
| Default interceptor | Proxies to RM; inherits RM checks / RM getAppAttempts gap (already reported) |
| Federation interceptor | Several attempt/container routes `NotImplementedException`; no extra ACL skip |

### NMWebServices

| Surface | Result |
|---|---|
| `getNodeContainer` / `getNodeApp` | **Already reported** VIEW_APP skip |
| Log content / path traversal | **Already reported** |
| `getContainerLogsInfo` | No VIEW_APP; log filenames/sizes — same NM log IDOR class |
| `getNMResourceInfo` | GPU `assignedGpuDevices` includes `containerId` only when `yarn.nodemanager.resource-plugins` enables GPU (default empty). Redundant with already-reported unfiltered container detail when GPU is on; not default. |

---

## Summary

One NEW MEDIUM ACL-on bypass: **RM scheduler app-activities IDOR** leaking container/node placement and enabling activity recording for apps the caller cannot VIEW_APP.
