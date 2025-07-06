using System;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using Raven.Client.Documents.Operations.Backups;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Maintenance;

public class PeriodicBackupStatusReport
        {
            public long TaskId { get; set; }
            public BackupType BackupType { get; set; }
            public DateTime? LastFullBackupInternal { get; set; }
            public DateTime? LastIncrementalBackupInternal { get; set; }
            public long? LastRaftIndexEtag { get; set; }
            public long? LastFullBackupRaftIndexEtag { get; set; }
            public DateTime? DelayUntil { get; set; }
            public bool IsErrored { get; set; }

            public static PeriodicBackupStatusReport Deserialize(BlittableJsonReaderObject backupStatus)
            {
                if (backupStatus == null)
                    return null;

                var statusReport = new PeriodicBackupStatusReport();

                if (backupStatus.TryGet(nameof(PeriodicBackupStatus.TaskId), out long taskId))
                    statusReport.TaskId = taskId;

                if (backupStatus.TryGet(nameof(PeriodicBackupStatus.BackupType), out BackupType backupType))
                    statusReport.BackupType = backupType;

                if (backupStatus.TryGet(nameof(PeriodicBackupStatus.LastFullBackupInternal), out DateTime? lastFullBackupInternal))
                    statusReport.LastFullBackupInternal = lastFullBackupInternal;

                if (backupStatus.TryGet(nameof(PeriodicBackupStatus.LastIncrementalBackupInternal), out DateTime? lastIncrementalBackupInternal))
                    statusReport.LastIncrementalBackupInternal = lastIncrementalBackupInternal;

                if (backupStatus.TryGet(nameof(PeriodicBackupStatus.LastRaftIndex), out BlittableJsonReaderObject lastRaftIndexBlittable) && lastRaftIndexBlittable != null)
                {
                    lastRaftIndexBlittable.TryGet(nameof(LastRaftIndex.LastEtag), out long? lastEtag);
                    statusReport.LastRaftIndexEtag = lastEtag;

                    lastRaftIndexBlittable.TryGet(nameof(LastRaftIndex.LastFullBackupEtag), out long? lastFullBackupEtag);
                    statusReport.LastFullBackupRaftIndexEtag = lastFullBackupEtag;
                }

                if (backupStatus.TryGet(nameof(PeriodicBackupStatus.DelayUntil), out DateTime? delayUntil))
                    statusReport.DelayUntil = delayUntil;

                if (backupStatus.TryGet(nameof(PeriodicBackupStatus.Error), out BlittableJsonReaderObject errorBlittable) && errorBlittable != null)
                    statusReport.IsErrored = true;

                return statusReport;
            }

            [SuppressMessage("ReSharper", "HeuristicUnreachableCode")]
            internal bool IsValid(string nodeTag, Action<string> onDiagnosticLog)
            {
                if (LastFullBackupInternal != null)
                    return true;

                Debug.Assert(false, $"Should not happen, if {nameof(LastFullBackupInternal)} is null, it means that we stored a corrupted status in the cluster. Node: '{nodeTag}', taskId: '{TaskId}'");
                onDiagnosticLog.Invoke($"Node '{nodeTag}' has a null LastFullBackupInternal for taskId '{TaskId}', should not happen, probably a bug.");
                return false;
            }

            public DynamicJsonValue ToJson() =>
                new()
                {
                    [nameof(TaskId)] = TaskId,
                    [nameof(BackupType)] = BackupType,
                    [nameof(LastFullBackupInternal)] = LastFullBackupInternal,
                    [nameof(LastIncrementalBackupInternal)] = LastIncrementalBackupInternal,
                    [nameof(LastRaftIndexEtag)] = LastRaftIndexEtag,
                    [nameof(LastFullBackupRaftIndexEtag)] = LastFullBackupRaftIndexEtag,
                    [nameof(DelayUntil)] = DelayUntil,
                    [nameof(IsErrored)] = IsErrored
                };

            public override string ToString()
            {
                using var ctx = JsonOperationContext.ShortTermSingleUse();
                return ctx.ReadObject(ToJson(), "backup-status").ToString();
            }

            public PeriodicBackupStatus ToPeriodicBackupStatus() =>
                new()
                {
                    TaskId = TaskId,
                    BackupType = BackupType,
                    LastFullBackup = LastFullBackupInternal,
                    LastFullBackupInternal = LastFullBackupInternal,
                    LastIncrementalBackup = LastIncrementalBackupInternal,
                    LastIncrementalBackupInternal = LastIncrementalBackupInternal,
                    LastRaftIndex = new LastRaftIndex { LastEtag = LastRaftIndexEtag, LastFullBackupEtag = LastFullBackupRaftIndexEtag},
                    DelayUntil = DelayUntil,
                    Error = IsErrored ? new Error { Exception = "Periodic backup task is in error state." } : null
                };
        }
