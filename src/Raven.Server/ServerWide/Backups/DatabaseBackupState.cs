using System;
using System.Collections.Generic;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.OngoingTasks;
using Raven.Client.Util;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.NotificationCenter.Notifications.Details;
using Raven.Server.Rachis;
using Raven.Server.ServerWide.Commands;
using Raven.Server.Utils;
using Sparrow.Threading;
using static Raven.Server.ServerWide.Backups.ServerBackupRunner;

namespace Raven.Server.ServerWide.Backups
{
    public class DatabaseBackupState
    {
        public const int MaxDecisionLogSize = 32;

        internal readonly string DatabaseName;

        public readonly string OriginalDatabaseName;

        public long OperationId { get; internal set; }

        public PeriodicBackupConfiguration Configuration { get; set; }

        public PeriodicBackupStatus BackupStatus { get; set; }

        public PeriodicBackupStatus RunningBackupStatus { get; set; }

        public NextBackup NextBackup { get; set; }

        public DateTime CreatedAt { get; set; }

        public DateTime StartTimeInUtc { get; set; }

        public DateTime DatabaseWakeUpTimeUtc { get; set; }

        public MultipleUseFlag Stale { get; } = new();

        private readonly List<(DateTime Time, string Reason)> _decisionLog = new();

        public MultipleUseFlag Running { get; } = new();

        public RunningBackupTask RunningTask { get; set; }

        internal ServerStore _serverStore;

        public DatabaseBackupState([NotNull] string databaseName, [NotNull] PeriodicBackupConfiguration configuration, bool isSharded, ServerStore serverStore)
        {
            DatabaseName = databaseName ?? throw new ArgumentNullException(nameof(databaseName));
            Configuration = configuration ?? throw new ArgumentNullException(nameof(configuration));
            BackupStatus = new PeriodicBackupStatus
            {
                BackupType = configuration.BackupType,
                TaskId = configuration.TaskId,
                LastFullBackup = null,
                LastIncrementalBackup = null
            };
            CreatedAt = DateTime.UtcNow;
            OriginalDatabaseName = isSharded ? ShardHelper.ToDatabaseName(databaseName) : databaseName;
            _serverStore = serverStore;
        }

        public DatabaseBackupState()
        {
        }

        public NextBackup GetNextBackupDetails(out string responsibleNodeTag)
        {
            var backupStatus = GetMostUpdatedLocalBackupStatus(Configuration.TaskId, inMemoryBackupStatus: null, DatabaseName);
            var taskStatus = _serverStore.BackupRunner.GetTaskStatus(Configuration, DatabaseName, out responsibleNodeTag, disableLog: true);
            BackupStatus = backupStatus;
            return taskStatus == ServerBackupRunner.TaskStatus.Disabled ? null : GetNextBackupDetails(skipErrorLog: true);
        }

        private void OnMissingNextBackupInfo(PeriodicBackupConfiguration configuration)
        {
            var message = "Couldn't schedule next backup " +
                          $"full backup frequency: {configuration.FullBackupFrequency}, " +
                          $"incremental backup frequency: {configuration.IncrementalBackupFrequency}";
            if (string.IsNullOrWhiteSpace(configuration.Name) == false)
                message += $", backup name: {configuration.Name}";

            _serverStore.NotificationCenter.Add(AlertRaised.Create(
                _serverStore.NodeTag,
                "Couldn't schedule next backup, this shouldn't happen",
                message,
                AlertReason.PeriodicBackup,
                NotificationSeverity.Warning));
        }

        private void OnParsingError(BackupUtils.OnParsingErrorParameters parameters)
        {
            var message = "Couldn't parse periodic backup " +
                          $"frequency {parameters.BackupFrequency}, task id: {parameters.Configuration.TaskId}";
            if (string.IsNullOrWhiteSpace(parameters.Configuration.Name) == false)
                message += $", backup name: {parameters.Configuration.Name}";

            message += $", error: {parameters.Exception.Message}";

            _serverStore.NotificationCenter.Add(AlertRaised.Create(
                _serverStore.NodeTag,
                "Backup frequency parsing error",
                message,
                AlertReason.PeriodicBackup,
                NotificationSeverity.Error,
                details: new ExceptionDetails(parameters.Exception)));
        }

        private NextBackup GetNextBackupDetails(bool skipErrorLog = false)
        {
            return BackupUtils.GetNextBackupDetails(new BackupUtils.NextBackupDetailsParameters
            {
                OnParsingError = skipErrorLog ? null : OnParsingError,
                Configuration = Configuration,
                BackupStatus = BackupStatus,
                DatabaseWakeUpTimeUtc = DatabaseWakeUpTimeUtc,
                NodeTag = _serverStore.NodeTag,
                OnMissingNextBackupInfo = OnMissingNextBackupInfo
            });
        }

        public PeriodicBackupStatus GetMostUpdatedLocalBackupStatus(long taskId, PeriodicBackupStatus inMemoryBackupStatus, string databaseName)
        {
            var backupStatus = _serverStore.DatabaseInfoCache.BackupStatusStorage.GetBackupStatus(databaseName, taskId);
            return BackupUtils.ComparePeriodicBackupStatus(taskId, backupStatus, inMemoryBackupStatus);
        }

        public override string ToString()
        {
            return $"'{Configuration.Name} ({Configuration.TaskId})' for database '{DatabaseName}'";
        }

        public void AddToDecisionLog(string reason, DateTime now)
        {
            lock (_decisionLog)
            {
                _decisionLog.Insert(0, (now, reason));

                if (_decisionLog.Count > MaxDecisionLogSize)
                    _decisionLog.RemoveAt(_decisionLog.Count - 1);
            }
        }

        public List<(DateTime Time, string Reason)> GetDecisionLog()
        {
            lock (_decisionLog)
            {
                return new List<(DateTime Time, string Reason)>(_decisionLog);
            }
        }
    }
}
