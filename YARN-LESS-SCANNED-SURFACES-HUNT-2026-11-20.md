# YARN 3.1.3 less-scanned surfaces hunt (2026-11-20)

**Repo:** `demenlee/hadoop-for-compile` (Apache Hadoop **3.1.3**)  
**Scope:** ApplicationMasterProtocol / CMP (tokens on) / RMAppSecurity·JWT·TLS / reservations / node labels·attributes / queue ACL·move·priority·timeout / RM DT / AdminService / Capacity·Fair remaining / Timeline v1·v2 / ATSv2 / SharedCache / Services / Registry / Federation Router / log aggregation / RM REST / WebAppProxy / AMRMProxy / CSI·GPU·FPGA / TimelineReader·Writer / Router interceptors / Placement constraints.

**Do not re-report:** CVE-2021-33036, WebAppProxy SSRF, ResourceMappings OIS, Timeline simple HTTP, ATSv2 collector hijack, NM log IDOR + `log.type` traversal, Federation ZK world ACL, Registry DNS, AMRMProxy tokens, ClientAM unauth stop/flex, SCM uploader no auth, Docker host-net, archive-logs bash, ShuffleHandler mapId, AMWebServices live conf, shuffle MAC, RM VIEW_APP IDOR when `yarn.acl.enable=false`, `/conf` `/logs` `/logLevel`, ZKConfigurationStore OIS, FairScheduler XXE, LocalizationProtocol heartbeat poison, NM REST skip VIEW_APP, HttpProbe SSRF, scheduler app-activities skip VIEW_APP, NMCollectorService.

---

## NEW MEDIUM+ finding

### HIGH — `ContainerManagementProtocol.signalToContainer` skips application ownership check

**Summary:** After YARN-5836 fixed cross-app `stopContainers`/`getContainerStatuses`, `signalToContainer` was never given the same ownership gate. Any AM that holds a valid NMToken for a NodeManager can send `GRACEFUL_SHUTDOWN` / `FORCEFUL_SHUTDOWN` / `OUTPUT_THREAD_DUMP` to **any** container on that NM.

**Evidence:**

```1795:1798:hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/main/java/org/apache/hadoop/yarn/server/nodemanager/containermanager/ContainerManagerImpl.java
  public SignalContainerResponse signalToContainer(
      SignalContainerRequest request) throws YarnException, IOException {
    internalSignalToContainer(request, "Application Master");
    return new SignalContainerResponsePBImpl();
```

```1941:1954:hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/main/java/org/apache/hadoop/yarn/server/nodemanager/containermanager/ContainerManagerImpl.java
  private void internalSignalToContainer(SignalContainerRequest request,
      String sentBy) {
    ContainerId containerId = request.getContainerId();
    Container container = this.context.getContainers().get(containerId);
    if (container != null) {
      ...
      this.dispatcher.getEventHandler().handle(
          new SignalContainersLauncherEvent(container,
              request.getCommand()));
```

Contrast — sibling CMP ops **do** bind the NMToken’s application to the target container:

- `stopContainers` / `getContainerStatuses` → `authorizeGetAndStopContainerRequest` (throws on app mismatch) — `ContainerManagerImpl.java:1527-1560`
- `localize` / `reInitialize` / rollback / commit → `preReInitializeOrLocalizeCheck` app-id equality — `ContainerManagerImpl.java:1915-1924`
- RM client path `ClientRMService.signalToContainer` correctly requires `MODIFY_APP` — `ClientRMService.java:1653-1686`

CMP RPC always requires TOKEN auth (`HADOOP_SECURITY_AUTHENTICATION=TOKEN` + `NMTokenSecretManager`) — `ContainerManagerImpl.java:622-632` — so the caller must present a real NMToken, but that token’s application is never compared to `request.getContainerId()`.

Signal mapping (SIGTERM / SIGKILL / SIGQUIT):

```968:984:hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/main/java/org/apache/hadoop/yarn/server/nodemanager/containermanager/launcher/ContainerLaunch.java
  public static Signal translateCommandToSignal(
      SignalContainerCommand command) {
    ...
      case GRACEFUL_SHUTDOWN:
        signal = Signal.TERM;
        break;
      case FORCEFUL_SHUTDOWN:
        signal = Signal.KILL;
```

**Attack chain (works with Kerberos + `yarn.acl.enable=true`):**

1. Attacker submits a normal YARN application and obtains an NMToken for a NodeManager that also runs victim containers (multi-tenant co-location).
2. Attacker learns victim `ContainerId`s via default-unfiltered RM/NM entity lists (`yarn.webapp.filter-entity-list-by-user=false`) or NM `GET /ws/v1/node/containers/{id}` (already noted VIEW_APP skip), or by enumerating predictable container id sequences.
3. Attacker calls `ContainerManagementProtocol.signalToContainer(victimId, FORCEFUL_SHUTDOWN)` authenticated with **their own** NMToken.
4. NM delivers SIGKILL to the victim process as the container user — no MODIFY_APP / app-ownership check.

**Impact:** Cross-tenant availability / integrity — kill or dump any co-located containers. Same class as YARN-5836 (fixed for stop/get only; `signalToContainer` still open in 3.1.3 and remains unfixed on current trunk).

**Remediation:** In `signalToContainer`, require NMToken + `authorizeUser`, then enforce the same application-id match as `authorizeGetAndStopContainerRequest` / `preReInitializeOrLocalizeCheck` before calling `internalSignalToContainer`. Keep the RM→NM `SIGNAL_CONTAINERS` event path as the trusted admin path.

---

## Surfaces checked — no new MEDIUM+ complete chain

| Surface | Result |
|---|---|
| **ApplicationMasterProtocol** register / allocate / finish | AMRMToken via `YarnServerSecurityUtils.authorizeRequest`; release list validated by `RMServerUtils.validateContainerReleaseRequest` (attempt-id match). |
| **RMAppSecurity / JWT / TLS for AM-RM** | Not present in 3.1.3 tree (later Hadoop feature). |
| **CMP remaining (tokens ON)** | start/stop/status/update/localize/reInit authorize NMToken↔app; **signalToContainer is the gap (above)**. |
| **Reservations** | `checkReservationACLs` + creator-or-ACL path in `ClientRMService` — no bypass beyond prior admin.acl=* class. |
| **Node labels / attributes** | Mutations gated by `RMNodeLabelsManager.checkAccess` / admin; no ClientRM unauth path. |
| **Queue ACL / move / priority / timeout** | `MODIFY_APP` + target-queue SUBMIT/ADMINISTER checks; CapScheduler `AppPriorityACLsManager` consulted. |
| **RMDelegationTokenSecretManager** | Issue/renew/cancel require Kerberos (`isAllowedDelegationTokenOp`). |
| **AdminService remaining** | Methods call `checkAcls` / `verifyAdminAccess`; default `yarn.admin.acl=*` is known config, not a new code bug. |
| **Capacity / Fair remaining** | No new ACL bypass beyond FairScheduler XXE / ZKConfigurationStore OIS (excluded). |
| **Timeline v1 remaining** | Mutations need UGI; anonymous/simple defaults already reported. |
| **ATSv2 / TimelineCollector putEntities** | No per-app ownership on put beyond collector existence; collector hijack already reported; ATSv2 off by default. |
| **TimelineReader / Writer** | Read filter only when `TIMELINE_SERVICE_READ_AUTH_ENABLED` (default false) and ATSv2 on — informational, not a default-on HIGH. |
| **SharedCache remaining** | Feature off; unauth Client/Uploader/Admin SecretManager=null already reported (YARN-2774). |
| **YARN Services remaining** | Beyond HttpProbe SSRF + ClientAM stop/flex — no new complete chain. |
| **Registry remaining** | Beyond DNS / insecure WorldReadWriteACL — no new chain. |
| **Federation Router remaining** | Beyond ZK world ACL / membership hijack — interceptors proxy without distinct authz bypass. |
| **Log aggregation remaining** | Beyond VIEW_APP IDOR + Har partName — AggregatedLogFormat paths use containerId string under app dirs. |
| **RM REST remaining** | Beyond VIEW_APP when acl=false + app-activities skip — kill/priority/queue/timeout PUT go through ClientRMService ACL. |
| **WebAppProxy remaining** | Beyond SSRF — AmIpFilter / proxy path only. |
| **AMRMProxy remaining** | Beyond federation registry token leak — local TOKEN auth OK. |
| **CSI / GPU / FPGA** | Resource plugins/admin; no unauth remote mutate path found. |
| **Router web interceptors** | Proxy-only; no distinct bypass. |
| **ApplicationClassification / Placement constraints** | Scheduler-side tag cardinality; no authz bypass found. |
