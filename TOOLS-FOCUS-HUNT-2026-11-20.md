# Hadoop 3.1.3 — hadoop-tools focus hunt (2026-11-20)

Repo: `demenlee/hadoop-for-compile` @ 3.1.3.

**Scope:** hadoop-distcp, hadoop-aws, hadoop-azure, hadoop-azure-datalake, hadoop-aliyun, hadoop-archives, hadoop-archive-logs, hadoop-gridmix, hadoop-rumen, hadoop-sls, hadoop-streaming, hadoop-openstack, hadoop-fs2img, hadoop-dynamometer, hadoop-extras, hadoop-datajoin.

**Excluded (do not re-report):** FileUtil.unTar / CVE-2022-26612; HadoopArchiveLogs bash injection; ResourceEstimator unauth LFI; FSDownload http/file SSRF+tar; HarFileSystem partName traversal; ShellBasedIdMapping bash -c; HostsFileReader XXE; FairScheduler XXE.

---

## Verdict

**No NEW MEDIUM+ findings** with a complete attacker-controlled input → impact chain in this scope.

---

## Surfaces checked (failed triage / negatives)

| Module / area | Result |
|---------------|--------|
| **DistCp** (`CopyMapper`, `RetriableFileCopyCommand`, `CopyCommitter`, `FileBasedCopyListing`, `GlobbedCopyListing`, `RegexCopyFilter`, `DistCpSync`) | Filters file is client-local `FileInputStream`. Listing/`-f` paths open as DistCp UGI. Preserve ACL/owner/xattr follow target FS permission checks for the job user. Snapshot-diff sync builds `new Path(targetDir, diffPath)`; HDFS `DFSUtilClient.isValidName` rejects `..` in normal paths, so no cross-target rename/delete escape from attacker-planted snapshot names. Credentials via job `Credentials` / `TokenCache` only. |
| **hadoop-aws / S3A** | URI userinfo still parsed (`S3xLoginHelper.extractLoginDetails`) with warn-only — secret-in-URI / job.xml leak class, not a new remote chain. Endpoint / STS / proxy / signer override are operator or job conf; signed traffic needs real keys already in conf. Magic/staging committers load JSON `PendingSet` (not remote OIS gadgets); magic disabled by default. Proxy password logged only at DEBUG. SSL on by default. |
| **hadoop-azure / WASB** | Account keys via KeyProvider/conf; `ShellDecryptionKeyProvider` uses `Shell.execCommand(String[])` (no shell metachar injection). Remote SAS/auth URLs (`fs.azure.cred.service.urls`, `fs.azure.authorization.remote.service.urls`) are admin conf. `RemoteWasbAuthorizerImpl` auto-allows `*-RenamePending.json` only when `fs.azure.authorization` is on (default **false**, also requires secure mode) — scoped to that suffix, not a default remote HIGH. |
| **hadoop-azure-datalake / ADL** | OAuth client id/secret/refresh token and refresh URL from `Configuration.getPassword` / conf; custom token provider class from conf. Insecure HTTP only if scheme configured. No open redirect/OAuth callback sink in Hadoop code. |
| **hadoop-aliyun / OSS** | Endpoint, keys, proxy from conf; no URI secret parser; no new remote chain. |
| **hadoop-openstack / Swift** | Bindings from conf + URI host/container; password/apiKey in conf. HttpClient used for configured auth/endpoint only. |
| **hadoop-archives** | `checkValidName` requires depth-1 name ending in `.har`. Creator writes controlled `part-*` / `_index`; reader escape is HarFileSystem `partName` (already excluded). |
| **hadoop-archive-logs** | Skipped bash script generation (already known). Runner path join uses `File.separator` + user/appId from tool args derived from HDFS dir names; HDFS rejects `..` components. Proxy/`doAs` requires configured proxyuser. |
| **hadoop-streaming** | `PipeMapRed` `ProcessBuilder` / `stream.map.streamprocessor` is submitter intentional task RCE. `Environment` runs fixed `env`/`cmd /C set`. `TypedBytesWritableInput.readWritable` class-loads from stream into task JVM as job user. `IdentifierResolver` maps fixed IDs only. |
| **hadoop-rumen** | Offline tool. `JobConfigurationParser` / `ParsedConfigFile` insecure `DocumentBuilder` XXE needs operator-fed XML — same offline class as HostsFileReader, not a default daemon. |
| **hadoop-sls** | Offline simulator. Jetty metrics UI has no auth but is not a production cluster daemon; `readObject` is servlet hygiene only. Track JSON embeds parameter text without a remote exploit chain on default deployments. |
| **hadoop-gridmix** | Batch replay tool; `RoundRobinUserResolver` proxy users require operator user-list + proxyuser ACLs. No remote unauth sink. |
| **hadoop-fs2img** | Offline image writer; UGI/block resolvers from CLI/conf; temp `FileInputStream` of writer-local files only. |
| **hadoop-dynamometer** | **Not present** in this tree. |
| **hadoop-extras** (`DistCh`, `GetGroups`) | DistCh `setOwner`/`setPermission` as job UGI; path list from CLI or `-f` URI readable by submitter. No cross-user escalation. |
| **hadoop-datajoin** | Library MapReduce join helpers; no exec/deser/SSRF sinks. |
| **Cmd-exec / OIS / XXE sinks (tools main)** | Only streaming `Runtime.exec("env")`, Azure shell key provider (argv array), rumen XXE (offline), SLS/PendingSet private `readObject` hooks. No new unauthenticated network gadget. |

---

## Notes

- Prior tools audits (`TOOLS-FS-IDENTITY-3.1.3-SECURITY-FINDINGS.md`, `TOOLS-CLOUD-FS-RESOURCEESTIMATOR-HUNT-2026-11-20.md`) already covered overlapping ground; this pass re-validated DistCp sync path joining, WASB remote authz bypass suffix, cloud credential/SSRF surfaces, and the remaining offline tools.
- ResourceEstimator unauth LFI remains in-tree but is explicitly excluded from this report.
