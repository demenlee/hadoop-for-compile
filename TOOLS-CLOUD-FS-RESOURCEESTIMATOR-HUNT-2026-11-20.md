# Hadoop 3.1.3 — hadoop-tools / cloud FS / ResourceEstimator hunt (2026-11-20)

Repo: `demenlee/hadoop-for-compile` @ 3.1.3. Scope: hadoop-aws (S3A), hadoop-azure/WASB, hadoop-azure-datalake, hadoop-aliyun, DistCp, HAR remaining, HttpFS remaining, KMS remaining, gridmix/rumen/sls/kafka/openstack/nfs, ResourceEstimator, dynamometer.

**Do not re-report:** HttpFS simple+cookie/`user.name`, KMS wildcard ACL, HadoopArchiveLogs bash, ShellBasedIdMapping, FileUtil.unTar, ZKConfigurationStore OIS, Har `partName` escape, ZKSigner open ACL.

---

## NEW validated MEDIUM+ findings

### 1. HIGH — Unauthenticated ResourceEstimator REST opens arbitrary local paths (LFI + store mutate)

- **Severity:** HIGH
- **Location:** `hadoop-tools/hadoop-resourceestimator/.../ResourceEstimatorService.java` + `LogParserUtil.java`
- **Title:** ResourceEstimatorService `POST /translator/{logFile}` accepts any filesystem path with no authentication
- **Description:** `ResourceEstimatorServer` starts an `HttpServer2` on `http://0.0.0.0:9998` (defaults) with Jersey resources and **no authentication filter**. `POST /resourceestimator/translator/{logFile:.+}` passes the path parameter directly to `LogParserUtil.parseLog` → `new FileInputStream(logFile)`. `ParserValidator.validate` is a stub that always returns `true`. There is no allow-list, chroot, or authn/authz.

  Parsed lines feed `NativeSingleLineParser` / optional `RmSingleLineParser`. Uncaught `NumberFormatException` from field parsing is mapped by `GenericExceptionHandler` into an HTTP body that includes `e.getMessage()` (e.g. `For input string: "<token>"`), leaking whitespace-delimited tokens from attacker-chosen readable files. Successful native-format lines store `pipelineId`/`jobId` tokens into the in-memory SkylineStore, queryable via unauthenticated `GET .../skylinestore/history/*/*`. Unauthenticated `DELETE .../skylinestore/history/{pipelineId}/{runId}` mutates the store.

  Shipped docs (`ResourceEstimator.md`) and `bin/start-estimator.sh` instruct operators to deploy this microservice with the default config (no auth keys exist in `resourceestimator-config.xml`).

- **Impact:** Any network client that can reach the estimator port can open any path readable by the estimator process (configs, key material files if readable, logs), obtain existence/permission oracles, exfiltrate tokens via exception messages / skyline JSON, DoS via large files, and delete history entries. Distinct from ConfServlet/`/logs` (different service and LFI sink).

- **Attack path:**
  1. Operator starts Resource Estimator (`bin/start-estimator.sh`) — binds `0.0.0.0:9998` by default.
  2. Attacker: `POST http://<host>:9998/resourceestimator/translator/%2Fpath%2Fto%2Freadable%2Ffile` (URL-encoded absolute path or CWD-relative path).
  3. Server opens the file as the estimator user; parser reads all lines.
  4. If a line has ≥8 whitespace fields and a non-integer skyline unit, response returns `NumberFormatException` with the leaked token; or attacker queries `GET /resourceestimator/skylinestore/history/*/*` after a successfully parsed line.
  5. Optional: `DELETE /resourceestimator/skylinestore/history/<id>/<run>` without credentials.

- **Evidence:**
  - `ResourceEstimatorConfiguration.java:41-51` — `SERVICE_URI=http://0.0.0.0/`, default port `9998`
  - `ResourceEstimatorServer.java:90-103` — HttpServer2 + Jersey, no auth
  - `ResourceEstimatorService.java:124-128` — `parseFile` → `parseLog(logFile)`
  - `LogParserUtil.java:88-97` — `new FileInputStream(logFile)`
  - `ParserValidator.java:37-39` — always `true`
  - `NativeSingleLineParser.java:88-90,103-104` — token/`Integer.parseInt` on file content
  - `GenericExceptionHandler.java:90-104` — NFE → response entity with `e.getMessage()`
  - `ResourceEstimatorService.java:182-194,226-232` — unauth GET history / DELETE
  - `ResourceEstimator.md:73-104` — default deploy + REST examples with no auth

- **Works on defaults?** Yes for the service’s own defaults (no auth, `0.0.0.0:9998`). Not started by core NN/RM daemons; exploitable whenever the shipped estimator is running (docs present it as the supported deployment).

- **Remediation:** Require authentication (Kerberos/SPNEGO or equivalent) and authorization on all estimator endpoints. Restrict `logFile` to an allow-listed directory; reject absolute paths and `..`. Do not return raw exception messages that embed file tokens. Bind to localhost unless explicitly configured otherwise. Disable unauthenticated DELETE.

---

## Areas checked — no NEW MEDIUM+ (failed triage)

| Area | Result |
|------|--------|
| **S3A URI credentials** | `S3xLoginHelper.extractLoginDetails` still accepts `user:pass@bucket`; warns only. Secrets-in-URI leak into job history / logs is the same secret-exposure class as already-reported job.xml/ConfServlet, not a new remote chain. `SimpleAWSCredentialsProvider` intentionally does not take URI keys. |
| **S3A endpoint / STS SSRF** | `fs.s3a.endpoint` / `fs.s3a.assumed.role.sts.endpoint` are operator/job conf. Signing traffic to a custom endpoint requires real AWS keys already in conf — no unauthenticated remote SSRF. |
| **S3A SSE keys / signer override** | SSE-C/KMS key and `fs.s3a.signing-algorithm` from conf only; no remote injection. |
| **S3A proxy password DEBUG log** | Password logged only at DEBUG when proxy configured — operator misconfig, below MEDIUM remote. |
| **WASB account key / SAS** | Keys via `fs.azure.account.key.*` / `fs.azure.sas.*` / KeyProvider; not URI userinfo. `ShellDecryptionKeyProvider` uses `ProcessBuilder`-style `Shell.execCommand(String[])` (no shell metachar injection). Remote SAS/cred service URLs are admin conf (`fs.azure.cred.service.urls`) — not attacker-controlled without conf write. |
| **ABFS** | No `azurebfs` package in this 3.1.3 tree (WASB + ADL only). |
| **ADL OAuth** | Client id/secret/refresh token from `Configuration.getPassword`; refresh URL admin conf. No open redirect/OAuth callback sink in Hadoop code. |
| **Aliyun OSS** | Endpoint/keys/proxy from conf; no URI secret parser; no new remote chain. |
| **OpenStack Swift** | Bindings from conf + URI host/container; password in conf. No new remote chain. |
| **DistCp** | Filters file is client-local path; credentials via job `Credentials`; overwrite/delete follow submitter UGI. No new cross-user HIGH. |
| **HAR remaining** | Beyond already-reported `partName` escape: index parsing otherwise sandbox-relative; no second distinct escape found. |
| **HttpFS OPEN redirect** | `createUploadRedirectionURL` rebuilds from `uriInfo.getRequestUriBuilder()` (Host-derived). No attacker-controlled external URL query param. Browser clients set Host to the real HttpFS host; Host-header open redirect needs a broken intermediary. Client follows `Location` for CREATE/APPEND (same pattern as WebHDFS redirect class already accounted for) — not filed as a new distinct HttpFS-only HIGH. |
| **HttpFS SETACL** | ACL spec validated against `DFS_WEBHDFS_ACL_PERMISSION_PATTERN_DEFAULT`; ops require authenticated UGI (already-reported simple auth is the authz gate). No ACL injection beyond that. |
| **HttpFS CSRF** | No CSRF filter in httpfs module; with already-reported anonymous/simple identity, CSRF is not a distinct new chain. |
| **HttpFS JSON injection** | Responses built via `json-simple` maps/objects from FS metadata, not string-concatenated attacker HTML. |
| **KMS remaining** | Beyond wildcard ACL / ZK signer: JSON APIs use structured parse/`KMSUtil`; no additional unauthenticated crypto sink found on defaults. |
| **gridmix / rumen** | Offline/batch tools. Rumen `JobConfigurationParser` insecure `DocumentBuilder` XXE requires operator-fed XML (same class as HostsFileReader MEDIUM; not a default daemon). |
| **SLS** | Simulator web UI for offline SLS runs; `readObject` is servlet deserialization hygiene only, not a network gadget entry. |
| **kafka** | `KafkaSink` sets `bootstrap.servers` from metrics config only. |
| **NFS remaining** | Beyond already-reported ShellBasedIdMapping: XDR request parsers only; no new cmd-exec/path sink. |
| **dynamometer** | Not present in this tree. |
| **HadoopArchiveLogs** | Skipped (already reported bash injection). |

---

## Verdict

**One NEW HIGH:** ResourceEstimator unauthenticated arbitrary local file open + history store mutation/leak when the shipped estimator service is running.
