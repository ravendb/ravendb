using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using NCrontab.Advanced;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.OngoingTasks;
using Raven.Client.Json.Serialization;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Client.Util;
using Raven.Server.Commercial;
using Raven.Server.Config;
using Raven.Server.Config.Settings;
using Raven.Server.Documents;
using Raven.Server.Documents.PeriodicBackup;
using Raven.Server.NotificationCenter;
using Raven.Server.Rachis;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Commands.PeriodicBackup;
using Raven.Server.ServerWide.Context;
using Sparrow.Logging;
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
            LastBackup = lastBackup == 0L ? null : new DateTime(lastBackup),
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

        var isFullBackup = IsFullBackup(parameters.BackupStatus, parameters.Configuration, nextFullBackup, nextIncrementalBackup, parameters.ResponsibleNodeTag);
        var nextBackupTimeLocal = GetNextBackupDateTime(nextFullBackup, nextIncrementalBackup, parameters.BackupStatus.DelayUntil);

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
            OriginalBackupTime = parameters.BackupStatus.OriginalBackupTime,
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

    private static DateTime GetNextBackupDateTime(DateTime? nextFullBackup, DateTime? nextIncrementalBackup, DateTime? delayUntil)
    {
        Debug.Assert(nextFullBackup != null || nextIncrementalBackup != null);
        DateTime? nextBackup;

        if (nextFullBackup == null)
            nextBackup = nextIncrementalBackup;
        else if (nextIncrementalBackup == null)
            nextBackup = nextFullBackup;
        else
            nextBackup = nextFullBackup <= nextIncrementalBackup ? nextFullBackup.Value : nextIncrementalBackup.Value;

        return delayUntil.HasValue && delayUntil.Value.ToLocalTime() > nextBackup.Value
            ? delayUntil.Value.ToLocalTime() 
            : nextBackup.Value;              
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

    public static PathSetting GetBackupTempPath(RavenConfiguration configuration, string dir, out PathSetting basePath)
    {
        basePath = configuration.Backup.TempPath ?? configuration.Storage.TempPath ?? configuration.Core.DataDirectory;
        return basePath.Combine(dir);
    }

    public static IdleDatabaseActivity GetEarliestIdleDatabaseActivity(EarliestIdleDatabaseActivityParameters parameters)
    {
        IdleDatabaseActivity earliestAction = null;
        using (parameters.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
        using (context.OpenReadTransaction())
        {
            var rawDatabaseRecord = parameters.ServerStore.Cluster.ReadRawDatabaseRecord(context, parameters.DatabaseName);

            foreach (var backup in rawDatabaseRecord.PeriodicBackups)
            {
                var nextAction = GetNextIdleDatabaseActivity(new NextIdleDatabaseActivityParameters(parameters, backup, context));

                if (nextAction == null)
                    continue;

                earliestAction =
                    earliestAction == null || nextAction.DateTime < earliestAction.DateTime
                        ? nextAction
                        : earliestAction;
            }
        }
        return earliestAction;
    }

    private static IdleDatabaseActivity GetNextIdleDatabaseActivity(NextIdleDatabaseActivityParameters parameters)
    {
        // we will always wake up the database for a full backup.
        // but for incremental we will wake the database only if there were changes made.

        if (parameters.Configuration.Disabled ||
            parameters.Configuration.IncrementalBackupFrequency == null && parameters.Configuration.FullBackupFrequency == null ||
            parameters.Configuration.HasBackup() == false)
            return null;

        var backupStatus = GetBackupStatusFromCluster(parameters.ServerStore, parameters.Context, parameters.DatabaseName, parameters.Configuration.TaskId);
        if (backupStatus == null)
        {
            // we want to wait for the backup occurrence
            if (parameters.Logger.IsOperationsEnabled)
                parameters.Logger.Operations($"Backup Task '{parameters.Configuration.TaskId}' of database '{parameters.DatabaseName}' is never backed up yet.");

            return new IdleDatabaseActivity(IdleDatabaseActivityType.WakeUpDatabase, DateTime.UtcNow);
        }

        var topology = parameters.ServerStore.LoadDatabaseTopology(parameters.DatabaseName);
        var responsibleNodeTag = WhoseTaskIsIt(parameters.ServerStore, topology, parameters.Configuration, backupStatus, parameters.NotificationCenter, keepTaskOnOriginalMemberNode: true);
        if (responsibleNodeTag == null)
        {
            // cluster is down
            if (parameters.Logger.IsOperationsEnabled)
                parameters.Logger.Operations($"Could not find the responsible node for backup task '{parameters.Configuration.TaskId}' of database '{parameters.DatabaseName}'.");

            return new IdleDatabaseActivity(IdleDatabaseActivityType.WakeUpDatabase, DateTime.UtcNow);
        }

        if (responsibleNodeTag != parameters.ServerStore.NodeTag)
        {
            // not responsible for this backup task
            if (parameters.Logger.IsOperationsEnabled)
                parameters.Logger.Operations($"Current server '{parameters.ServerStore.NodeTag}' is not responsible node for backup task '{parameters.Configuration.TaskId}' of database '{parameters.DatabaseName}'. Backup Task responsible node is '{responsibleNodeTag}'.");

            return null;
        }

        var nextBackup = GetNextBackupDetails(new NextBackupDetailsParameters
        {
            OnParsingError = parameters.OnParsingError,
            Configuration = parameters.Configuration,
            BackupStatus = backupStatus,
            ResponsibleNodeTag = responsibleNodeTag,
            DatabaseWakeUpTimeUtc = parameters.DatabaseWakeUpTimeUtc,
            NodeTag = parameters.ServerStore.NodeTag,
            OnMissingNextBackupInfo = parameters.OnMissingNextBackupInfo
        });

        if (nextBackup == null)
        {
            if (parameters.Logger.IsOperationsEnabled)
                parameters.Logger.Operations($"Backup Task '{parameters.Configuration.TaskId}' of database '{parameters.DatabaseName}' doesn't have next backup. Should not happen and likely a bug.");
            return null;
        }

        var nowUtc = SystemTime.UtcNow;
        if (nextBackup.DateTime < nowUtc)
        {
            // this backup is delayed
            if (parameters.Logger.IsOperationsEnabled)
                parameters.Logger.Operations($"Backup Task '{parameters.Configuration.TaskId}' of database '{parameters.DatabaseName}' is delayed.");
            return new IdleDatabaseActivity(IdleDatabaseActivityType.WakeUpDatabase, DateTime.UtcNow);
        }

        if (backupStatus.LastEtag != parameters.LastEtag)
        {
            // we have changes since last backup
            var type = nextBackup.IsFull ? "full" : "incremental";
            if (parameters.Logger.IsOperationsEnabled)
                parameters.Logger.Operations($"Backup Task '{parameters.Configuration.TaskId}' of database '{parameters.DatabaseName}' have changes since last backup. Wakeup timer will be set to the next {type} backup at '{nextBackup.DateTime}'.");
            return new IdleDatabaseActivity(IdleDatabaseActivityType.WakeUpDatabase, nextBackup.DateTime);
        }

        if (nextBackup.IsFull)
        {
            if (parameters.Logger.IsOperationsEnabled)
                parameters.Logger.Operations($"Backup Task '{parameters.Configuration.TaskId}' of database '{parameters.DatabaseName}' doesn't have changes since last backup. Wakeup timer will be set to the next full backup at '{nextBackup.DateTime}'.");
            return new IdleDatabaseActivity(IdleDatabaseActivityType.WakeUpDatabase, nextBackup.DateTime);
        }

        // we don't have changes since the last backup and the next backup are incremental
        var lastFullBackup = backupStatus.LastFullBackupInternal ?? nowUtc;
        var nextFullBackup = GetNextBackupOccurrence(new NextBackupOccurrenceParameters
        {
            BackupFrequency = parameters.Configuration.FullBackupFrequency,
            LastBackupUtc = lastFullBackup,
            Configuration = parameters.Configuration,
            OnParsingError = parameters.OnParsingError
        });

        if (nextFullBackup < nowUtc)
        {
            if (parameters.Logger.IsOperationsEnabled)
                parameters.Logger.Operations($"Backup Task '{parameters.Configuration.TaskId}' of database '{parameters.DatabaseName}' doesn't have changes since last backup but has delayed backup.");
            // this backup is delayed
            return new IdleDatabaseActivity(IdleDatabaseActivityType.WakeUpDatabase, DateTime.UtcNow);
        }

        if (parameters.Logger.IsOperationsEnabled)
            parameters.Logger.Operations($"Backup Task '{parameters.Configuration.TaskId}' of database '{parameters.DatabaseName}' doesn't have changes since last backup. Wakeup timer set to next full backup at {nextFullBackup}, and will skip the incremental backups.");

        return new IdleDatabaseActivity(IdleDatabaseActivityType.UpdateBackupStatusOnly, nextBackup.DateTime, parameters.Configuration.TaskId, parameters.LastEtag);
    }

    public static void SaveBackupStatus(PeriodicBackupStatus status, string databaseName, ServerStore serverStore, Logger logger,
        BackupResult backupResult = default, Action<IOperationProgress> onProgress = default, OperationCancelToken operationCancelToken = default)
    {
        try
        {
            var command = new UpdatePeriodicBackupStatusCommand(databaseName, RaftIdGenerator.NewId()) { PeriodicBackupStatus = status };

            AsyncHelpers.RunSync(async () =>
            {
                var index = await BackupHelper.RunWithRetriesAsync(maxRetries: 10, async () =>
                    {
                        var result = await serverStore.SendToLeaderAsync(command);
                        return result.Index;
                    },
                    infoMessage: "Saving the backup status in the cluster",
                    errorMessage: "Failed to save the backup status in the cluster",
                    backupResult, onProgress, operationCancelToken);

                await BackupHelper.RunWithRetriesAsync(maxRetries: 10, async () =>
                    {
                        await serverStore.WaitForCommitIndexChange(RachisConsensus.CommitIndexModification.GreaterOrEqual, index);
                        return default;
                    },
                    infoMessage: "Verifying that the backup status was successfully saved in the cluster",
                    errorMessage: "Failed to verify that the backup status was successfully saved in the cluster",
                    backupResult, onProgress, operationCancelToken);
            });

            if (logger.IsInfoEnabled)
                logger.Info($"Periodic backup status with task id {status.TaskId} was updated");
        }
        catch (Exception e)
        {
            const string message = "Error saving the periodic backup status";

            if (logger.IsOperationsEnabled)
                logger.Operations(message, e);

            backupResult?.AddError($"{message}{Environment.NewLine}{e}");
        }
    }

    public static string WhoseTaskIsIt(
            ServerStore serverStore,
            DatabaseTopology databaseTopology,
            IDatabaseTask configuration,
            IDatabaseTaskStatus taskStatus,
            AbstractNotificationCenter notificationCenter,
            bool keepTaskOnOriginalMemberNode = false)
    {
        var whoseTaskIsIt = databaseTopology.WhoseTaskIsIt(
            serverStore.Engine.CurrentState, configuration,
            getLastResponsibleNode:
            () =>
            {
                var lastResponsibleNode = taskStatus?.NodeTag;
                if (lastResponsibleNode == null)
                {
                    // first time this task is assigned
                    return null;
                }

                if (databaseTopology.AllNodes.Contains(lastResponsibleNode) == false)
                {
                    // the topology doesn't include the last responsible node anymore
                    // we'll choose a different one
                    return null;
                }

                if (taskStatus is PeriodicBackupStatus)
                {
                    if (databaseTopology.Rehabs.Contains(lastResponsibleNode) &&
                        databaseTopology.PromotablesStatus.TryGetValue(lastResponsibleNode, out var status) &&
                        (status == DatabasePromotionStatus.OutOfCpuCredits ||
                         status == DatabasePromotionStatus.EarlyOutOfMemory ||
                         status == DatabasePromotionStatus.HighDirtyMemory))
                    {
                        // avoid moving backup tasks when the machine is out of CPU credit
                        return lastResponsibleNode;
                    }
                }

                if (serverStore.LicenseManager.HasHighlyAvailableTasks() == false)
                {
                    // can't redistribute, keep it on the original node
                    RaiseAlertIfNecessary(databaseTopology, configuration, lastResponsibleNode, serverStore, notificationCenter);
                    return lastResponsibleNode;
                }

                if (keepTaskOnOriginalMemberNode &&
                    databaseTopology.Members.Contains(lastResponsibleNode))
                {
                    // keep the task on the original node
                    return lastResponsibleNode;
                }

                return null;
            });

        if (whoseTaskIsIt == null && taskStatus is PeriodicBackupStatus)
            return taskStatus.NodeTag; // we don't want to stop backup process

        return whoseTaskIsIt;
    }

    private static void RaiseAlertIfNecessary(DatabaseTopology databaseTopology, IDatabaseTask configuration, string lastResponsibleNode,
        ServerStore serverStore, AbstractNotificationCenter notificationCenter)
    {
        // raise alert if redistribution is necessary
        if (notificationCenter != null &&
            databaseTopology.Count > 1 &&
            serverStore.NodeTag != lastResponsibleNode &&
            databaseTopology.Members.Contains(lastResponsibleNode) == false)
        {
            var alert = LicenseManager.CreateHighlyAvailableTasksAlert(databaseTopology, configuration, lastResponsibleNode);
            notificationCenter.Add(alert);
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

    public class EarliestIdleDatabaseActivityParameters
    {
        public string DatabaseName { get; set; }

        public DateTime? DatabaseWakeUpTimeUtc { get; set; }

        public long LastEtag { get; set; }

        public Logger Logger { get; set; }

        public AbstractNotificationCenter NotificationCenter { get; set; }

        public Action<OnParsingErrorParameters> OnParsingError { get; set; }

        public Action<PeriodicBackupConfiguration> OnMissingNextBackupInfo { get; set; }

        public ServerStore ServerStore { get; set; }
    }

    public class NextIdleDatabaseActivityParameters : EarliestIdleDatabaseActivityParameters
    {
        public PeriodicBackupConfiguration Configuration { get; set; }

        public TransactionOperationContext Context { get; set; }

        public NextIdleDatabaseActivityParameters(EarliestIdleDatabaseActivityParameters parameters, PeriodicBackupConfiguration configuration, TransactionOperationContext context)
        {
            Context = context;
            Configuration = configuration;
            DatabaseName = parameters.DatabaseName;
            DatabaseWakeUpTimeUtc = parameters.DatabaseWakeUpTimeUtc;
            LastEtag = parameters.LastEtag;
            Logger = parameters.Logger;
            NotificationCenter = parameters.NotificationCenter;
            OnParsingError = parameters.OnParsingError;
            OnMissingNextBackupInfo = parameters.OnMissingNextBackupInfo;
            ServerStore = parameters.ServerStore;
        }
    }
}
