using System;
using System.Collections.Generic;
using System.Diagnostics;
using NCrontab.Advanced;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.OngoingTasks;
using Raven.Client.Json.Serialization;
using Raven.Client.ServerWide.Operations;
using Raven.Client.Util;
using Raven.Server.Documents;
using Raven.Server.Documents.PeriodicBackup;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow.LowMemory;

namespace Raven.Server.Utils;

internal static class BackupUtils
{
    internal static BackupInfo GetBackupInfo(BackupInfoParameters parameters)
    {
        var oneTimeBackupStatus = GetBackupStatusFromCluster(parameters.ServerStore, parameters.Context, parameters.DatabaseName, taskId: 0L);

        if (parameters.PeriodicBackups.Count == 0 && oneTimeBackupStatus == null)
            return null;

        var lastBackup = 0L;
        PeriodicBackupStatus lastBackupStatus = null;
        var intervalUntilNextBackupInSec = long.MaxValue;
        if (oneTimeBackupStatus?.LastFullBackup != null && oneTimeBackupStatus.LastFullBackup.Value.Ticks > lastBackup)
        {
            lastBackup = oneTimeBackupStatus.LastFullBackup.Value.Ticks;
            lastBackupStatus = oneTimeBackupStatus;
        }

        foreach (var periodicBackup in parameters.PeriodicBackups)
        {
            var status = ComparePeriodicBackupStatus(periodicBackup.Configuration.TaskId,
                backupStatus: GetBackupStatusFromCluster(parameters.ServerStore, parameters.Context, parameters.DatabaseName, periodicBackup.Configuration.TaskId),
                inMemoryBackupStatus: periodicBackup.BackupStatus);

            if (status.LastFullBackup != null && status.LastFullBackup.Value.Ticks > lastBackup)
            {
                lastBackup = status.LastFullBackup.Value.Ticks;
                lastBackupStatus = status;
            }

            if (status.LastIncrementalBackup != null && status.LastIncrementalBackup.Value.Ticks > lastBackup)
            {
                lastBackup = status.LastIncrementalBackup.Value.Ticks;
                lastBackupStatus = status;
            }

            var nextBackup = GetNextBackupDetails(new NextBackupDetailsParameters
            {
                // OnParsingError and OnMissingNextBackupInfo are null's - for skipping error messages notification and log
                Configuration = periodicBackup.Configuration,
                BackupStatus = status,
                ResponsibleNodeTag = parameters.ServerStore.NodeTag,
                NodeTag = parameters.ServerStore.NodeTag
            });
            if (nextBackup == null)
                continue;
            
            if (nextBackup.TimeSpan.Ticks < intervalUntilNextBackupInSec)
                intervalUntilNextBackupInSec = nextBackup.TimeSpan.Ticks;
        }

        return new BackupInfo
        {
            LastBackup = lastBackup == 0L ? (DateTime?)null : new DateTime(lastBackup),
            IntervalUntilNextBackupInSec = intervalUntilNextBackupInSec == long.MaxValue ? 0 : new TimeSpan(intervalUntilNextBackupInSec).TotalSeconds,
            BackupTaskType = lastBackupStatus?.TaskId == 0 ? BackupTaskType.OneTime : BackupTaskType.Periodic,
            Destinations = AddDestinations(lastBackupStatus)
        };
    }

    internal static PeriodicBackupStatus GetBackupStatusFromCluster(ServerStore serverStore, TransactionOperationContext context, string databaseName, long taskId)
    {
        var statusBlittable = serverStore.Cluster.Read(context, PeriodicBackupStatus.GenerateItemName(databaseName, taskId));

        if (statusBlittable == null)
            return null;

        var periodicBackupStatusJson = JsonDeserializationClient.PeriodicBackupStatus(statusBlittable);
        return periodicBackupStatusJson;
    }

    internal static PeriodicBackupStatus ComparePeriodicBackupStatus(long taskId, PeriodicBackupStatus backupStatus, PeriodicBackupStatus inMemoryBackupStatus)
    {
        if (backupStatus == null)
        {
            backupStatus = inMemoryBackupStatus ?? new PeriodicBackupStatus { TaskId = taskId };
        }
        else if (inMemoryBackupStatus?.Version > backupStatus.Version && inMemoryBackupStatus.NodeTag == backupStatus.NodeTag)
        {
            // the in memory backup status is more updated
            // and is of the same node (current one)
            backupStatus = inMemoryBackupStatus;
        }

        return backupStatus;
    }

    private static List<string> AddDestinations(PeriodicBackupStatus backupStatus)
    {
        if (backupStatus == null)
            return null;

        var destinations = new List<string>();
        if (backupStatus.UploadToAzure?.Skipped == false)
            destinations.Add(nameof(BackupConfiguration.BackupDestination.Azure));
        if (backupStatus.UploadToGlacier?.Skipped == false)
            destinations.Add(nameof(BackupConfiguration.BackupDestination.AmazonGlacier));
        if (backupStatus.UploadToFtp?.Skipped == false)
            destinations.Add(nameof(BackupConfiguration.BackupDestination.FTP));
        if (backupStatus.UploadToGoogleCloud?.Skipped == false)
            destinations.Add(nameof(BackupConfiguration.BackupDestination.GoogleCloud));
        if (backupStatus.UploadToS3?.Skipped == false)
            destinations.Add(nameof(BackupConfiguration.BackupDestination.AmazonS3));
        if (backupStatus.LocalBackup?.TempFolderUsed == false)
            destinations.Add(nameof(BackupConfiguration.BackupDestination.Local));
        if (destinations.Count == 0)
            destinations.Add(nameof(BackupConfiguration.BackupDestination.None));

        return destinations;
    }

    public static NextBackup GetNextBackupDetails(NextBackupDetailsParameters parameters)
    {
        var nowUtc = SystemTime.UtcNow;
        var lastFullBackupUtc = parameters.BackupStatus.LastFullBackupInternal ?? parameters.DatabaseWakeUpTimeUtc ?? nowUtc;
        var lastIncrementalBackupUtc = parameters.BackupStatus.LastIncrementalBackupInternal ?? parameters.BackupStatus.LastFullBackupInternal ?? parameters.DatabaseWakeUpTimeUtc ?? nowUtc;
        var nextFullBackup = GetNextBackupOccurrence(new NextBackupOccurrenceParameters
        {
            BackupFrequency = parameters.Configuration.FullBackupFrequency,
            Configuration = parameters.Configuration,
            LastBackupUtc = lastFullBackupUtc,
            OnParsingError = parameters.OnParsingError,
        });
        var nextIncrementalBackup = GetNextBackupOccurrence(new NextBackupOccurrenceParameters
        {
            BackupFrequency = parameters.Configuration.IncrementalBackupFrequency,
            Configuration = parameters.Configuration,
            LastBackupUtc = lastIncrementalBackupUtc,
            OnParsingError = parameters.OnParsingError
        });

        if (nextFullBackup == null && nextIncrementalBackup == null)
        {
            parameters.OnMissingNextBackupInfo?.Invoke(parameters.Configuration);
            return null;
        }

        Debug.Assert(parameters.Configuration.TaskId != 0);

        var isFullBackup = IsFullBackup(parameters.BackupStatus, parameters.Configuration, nextFullBackup, nextIncrementalBackup, parameters.ResponsibleNodeTag );
        var nextBackupTimeLocal = GetNextBackupDateTime(nextFullBackup, nextIncrementalBackup);
        var nowLocalTime = SystemTime.UtcNow.ToLocalTime();
        var timeSpan = nextBackupTimeLocal - nowLocalTime;

        TimeSpan nextBackupTimeSpan;
        if (timeSpan.Ticks <= 0)
        {
            // overdue backup of current node or first backup
            if (parameters.BackupStatus.NodeTag == parameters.NodeTag || parameters.BackupStatus.NodeTag == null)
            {
                // the backup will run now
                nextBackupTimeSpan = TimeSpan.Zero;
                nextBackupTimeLocal = nowLocalTime;
            }
            else
            {
                // overdue backup from other node
                nextBackupTimeSpan = TimeSpan.FromMinutes(1);
                nextBackupTimeLocal = nowLocalTime + nextBackupTimeSpan;
            }
        }
        else
        {
            nextBackupTimeSpan = timeSpan;
        }

        return new NextBackup
        {
            TimeSpan = nextBackupTimeSpan,
            DateTime = nextBackupTimeLocal.ToUniversalTime(),
            IsFull = isFullBackup,
            TaskId = parameters.Configuration.TaskId
        };
    }

    internal static DateTime? GetNextBackupOccurrence(NextBackupOccurrenceParameters parameters)
    {
        if (string.IsNullOrWhiteSpace(parameters.BackupFrequency))
            return null;

        try
        {
            var backupParser = CrontabSchedule.Parse(parameters.BackupFrequency);
            return backupParser.GetNextOccurrence(parameters.LastBackupUtc.ToLocalTime());
        }
        catch (Exception e)
        {
            parameters.OnParsingError?.Invoke(new OnParsingErrorParameters
            {
                Exception = e,
                BackupFrequency = parameters.BackupFrequency,
                Configuration = parameters.Configuration
            });

            return null;
        }
    }

    private static DateTime GetNextBackupDateTime(DateTime? nextFullBackup, DateTime? nextIncrementalBackup)
    {
        Debug.Assert(nextFullBackup != null || nextIncrementalBackup != null);

        if (nextFullBackup == null)
            return nextIncrementalBackup.Value;

        if (nextIncrementalBackup == null)
            return nextFullBackup.Value;

        var nextBackup = nextFullBackup <= nextIncrementalBackup ? nextFullBackup.Value : nextIncrementalBackup.Value;
        return nextBackup;
    }

    private static bool IsFullBackup(PeriodicBackupStatus backupStatus,
        PeriodicBackupConfiguration configuration,
        DateTime? nextFullBackup, DateTime? nextIncrementalBackup, string responsibleNodeTag)
    {
        if (backupStatus.LastFullBackup == null ||
            backupStatus.NodeTag != responsibleNodeTag ||
            backupStatus.BackupType != configuration.BackupType ||
            backupStatus.LastEtag == null)
        {
            // Reasons to start a new full backup:
            // 1. there is no previous full backup, we are going to create one now
            // 2. the node which is responsible for the backup was replaced
            // 3. the backup type changed (e.g. from backup to snapshot)
            // 4. last etag wasn't updated

            return true;
        }

        // 1. there is a full backup setup but the next incremental backup wasn't setup
        // 2. there is a full backup setup and the next full backup is before the incremental one
        return nextFullBackup != null &&
               (nextIncrementalBackup == null || nextFullBackup <= nextIncrementalBackup);
    }

    internal static void CheckServerHealthBeforeBackup(ServerStore serverStore, string name)
    {
        if (serverStore.Server.CpuCreditsBalance.BackgroundTasksAlertRaised.IsRaised())
        {
            throw new BackupDelayException($"Failed to start Backup Task: '{name}'. The task cannot run because the CPU credits allocated to this machine are nearing exhaustion.")
            {
                DelayPeriod = serverStore.Configuration.Server.CpuCreditsExhaustionBackupDelay.AsTimeSpan
            };
        }

        if (LowMemoryNotification.Instance.LowMemoryState)
        {
            throw new BackupDelayException($"Failed to start Backup Task: '{name}'. The task cannot run because the server is in low memory state.")
            {
                DelayPeriod = serverStore.Configuration.Backup.LowMemoryBackupDelay.AsTimeSpan
            };
        }

        if (LowMemoryNotification.Instance.DirtyMemoryState.IsHighDirty)
        {
            throw new BackupDelayException($"Failed to start Backup Task: '{name}'. The task cannot run because the server is in high dirty memory state.")
            {
                DelayPeriod = serverStore.Configuration.Backup.LowMemoryBackupDelay.AsTimeSpan
            };
        }
    }

    public class NextBackupOccurrenceParameters
    {
        public string BackupFrequency { get; set; }

        public DateTime LastBackupUtc { get; set; }

        public PeriodicBackupConfiguration Configuration { get; set; }

        public Action<OnParsingErrorParameters> OnParsingError { get; set; }
    }

    public class OnParsingErrorParameters
    {
        public Exception Exception { get; set; }

        public string BackupFrequency { get; set; }

        public PeriodicBackupConfiguration Configuration { get; set; }
    }

    public class NextBackupDetailsParameters
    {
        public PeriodicBackupConfiguration Configuration { get; set; }

        public PeriodicBackupStatus BackupStatus { get; set; }

        public string ResponsibleNodeTag { get; set; }

        public DateTime? DatabaseWakeUpTimeUtc { get; set; }

        public string NodeTag { get; set; }

        public Action<OnParsingErrorParameters> OnParsingError { get; set; }

        public Action<PeriodicBackupConfiguration> OnMissingNextBackupInfo { get; set; }
    }

    public class BackupInfoParameters
    {
        public TransactionOperationContext Context { get; set; }
        public ServerStore ServerStore { get; set; }
        public List<PeriodicBackup> PeriodicBackups { get; set; }
        public string DatabaseName { get; set; }
    }
}
