using System;
using System.Diagnostics;
using NCrontab.Advanced;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.OngoingTasks;
using Raven.Client.Util;
using Raven.Server.Documents;
using Raven.Server.NotificationCenter.Notifications;

namespace Raven.Server.Utils;

internal static class BackupUtils
{

    public static NextBackup GetNextBackupDetails(NextBackupDetailsParameters parameters)
    {
        var nowUtc = SystemTime.UtcNow;
        var lastFullBackupUtc = parameters.BackupStatus.LastFullBackupInternal ?? parameters.DatabaseWakeUpTimeUtc ?? nowUtc;
        var lastIncrementalBackupUtc = parameters.BackupStatus.LastIncrementalBackupInternal ?? parameters.BackupStatus.LastFullBackupInternal ?? parameters.DatabaseWakeUpTimeUtc ?? nowUtc;
        var nextFullBackup = GetNextBackupOccurrence(new GetNextBackupOccurrenceParameters
        {
            BackupFrequency = parameters.Configuration.FullBackupFrequency,
            Configuration = parameters.Configuration,
            LastBackupUtc = lastFullBackupUtc,
            OnParsingError = parameters.OnParsingError
        });
        var nextIncrementalBackup = GetNextBackupOccurrence(new GetNextBackupOccurrenceParameters
        {
            BackupFrequency = parameters.Configuration.IncrementalBackupFrequency,
            Configuration = parameters.Configuration,
            LastBackupUtc = lastIncrementalBackupUtc,
            OnParsingError = parameters.OnParsingError
        });

        if (nextFullBackup == null && nextIncrementalBackup == null)
        {
            parameters.OnMissingNextBackupInfo?.Invoke();
            
            // if (database != null)
            // {
            //     var message = "Couldn't schedule next backup " +
            //                   $"full backup frequency: {configuration.FullBackupFrequency}, " +
            //                   $"incremental backup frequency: {configuration.IncrementalBackupFrequency}";
            //     if (string.IsNullOrWhiteSpace(configuration.Name) == false)
            //         message += $", backup name: {configuration.Name}";
            //
            //     database.NotificationCenter.Add(AlertRaised.Create(
            //         database.Name,
            //         "Couldn't schedule next backup, this shouldn't happen",
            //         message,
            //         AlertType.PeriodicBackup,
            //         NotificationSeverity.Warning));
            // }

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

    internal static DateTime? GetNextBackupOccurrence(GetNextBackupOccurrenceParameters parameters)
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
                //TODO-v: e, parameters.BackupFrequency, parameters.Configuration 
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

    public class GetNextBackupOccurrenceParameters
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

        public OnMissingNextBackupInfo OnMissingNextBackupInfo { get; set; }
    }

    internal delegate void OnMissingNextBackupInfo(); // Action with no parameters
}
