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
        internal readonly string DatabaseName;

        public readonly string OriginalDatabaseName;

        public long OperationId { get; internal set; }

        public PeriodicBackupConfiguration Configuration { get; set; }

        public PeriodicBackupStatus BackupStatus { get; set; }

        public PeriodicBackupStatus RunningBackupStatus { get; set; }

        public NextBackup NextBackup { get; set; }

        public DateTime CreatedAt { get; set; }

        public DateTime StartTimeInUtc { get; set; }

        public MultipleUseFlag Stale { get; } = new();

        private readonly BackupDecisionLog _decisionLog = new(BackupDecisionTracker.MaxDecisionLogSize);

        public MultipleUseFlag Running { get; } = new();

        internal MultipleUseFlag InQueue { get; } = new();

        internal MultipleUseFlag StatusUpdateInProgress { get; } = new();

        public RunningBackupTask RunningTask { get; set; }

        public OperationCancelToken RunningCancel { get; internal set; }

        private string _cancelReason;

        internal string CancelReason => Volatile.Read(ref _cancelReason);

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

        public NextBackup GetNextBackupDetails(PeriodicBackupStatus backupStatus, out string responsibleNodeTag)
        {
            var taskStatus = _serverStore.ServerBackupRunner.GetTaskStatus(Configuration, DatabaseName, out responsibleNodeTag, disableLog: true);
            return taskStatus == ServerBackupRunner.TaskStatus.Disabled ? null : GetNextBackupDetails(backupStatus, skipErrorLog: true);
        }

        /// <summary>Posts a warning notification when the backup schedule cannot be computed from the configuration.</summary>
        private void OnMissingNextBackupInfo(PeriodicBackupConfiguration configuration)
        {
            var message = "Couldn't schedule next backup " +
                          $"full backup frequency: {configuration.FullBackupFrequency}, " +
                          $"incremental backup frequency: {configuration.IncrementalBackupFrequency}";
            if (string.IsNullOrWhiteSpace(configuration.Name) == false)
                message += $", backup name: {configuration.Name}";

            _serverStore.ServerBackupRunner.AddDatabaseAlert(DatabaseName, AlertRaised.Create(
                DatabaseName,
                "Couldn't schedule next backup, this shouldn't happen",
                message,
                AlertReason.PeriodicBackup,
                NotificationSeverity.Warning,
                key: DatabaseName));
        }

        private void OnParsingError(BackupUtils.OnParsingErrorParameters parameters)
        {
            var message = "Couldn't parse periodic backup " +
                          $"frequency {parameters.BackupFrequency}, task id: {parameters.Configuration.TaskId}";
            if (string.IsNullOrWhiteSpace(parameters.Configuration.Name) == false)
                message += $", backup name: {parameters.Configuration.Name}";

            message += $", error: {parameters.Exception.Message}";

            _serverStore.ServerBackupRunner.AddDatabaseAlert(DatabaseName, AlertRaised.Create(
                DatabaseName,
                "Backup frequency parsing error",
                message,
                AlertReason.PeriodicBackup,
                NotificationSeverity.Error,
                key: DatabaseName,
                details: new ExceptionDetails(parameters.Exception)));
        }

        // skipErrorLog: the runner recalculates the next backup on every pass (BackupTimePolicy) and after each backup,
        // so the alerts are raised only when the configuration changes, otherwise the same alert is stored again every pass.
        internal NextBackup GetNextBackupDetails(PeriodicBackupStatus backupStatus, bool skipErrorLog = false)
        {
            return BackupUtils.GetNextBackupDetails(new BackupUtils.NextBackupDetailsParameters
            {
                OnParsingError = skipErrorLog ? null : OnParsingError,
                Configuration = Configuration,
                BackupStatus = backupStatus,
                NodeTag = _serverStore.NodeTag,
                OnMissingNextBackupInfo = skipErrorLog ? null : OnMissingNextBackupInfo
            });
        }

        public PeriodicBackupStatus GetMostUpdatedLocalBackupStatus(PeriodicBackupStatus inMemoryBackupStatus)
        {
            var taskId = Configuration.TaskId;
            var backupStatus = _serverStore.DatabaseInfoCache.BackupStatusStorage.GetBackupStatus(DatabaseName, taskId);
            return BackupUtils.ComparePeriodicBackupStatus(taskId, backupStatus, inMemoryBackupStatus);
        }

        public override string ToString()
        {
            return $"'{Configuration.Name} ({Configuration.TaskId})' for database '{DatabaseName}'";
        }

        public void AddToDecisionLog(BackupDecisionCategory category, string detail, string reason, DateTime now)
        {
            _serverStore.ServerBackupRunner.Decisions.AddDecision(this, _decisionLog, category, detail, reason, now);
        }

        internal void CancelRunningBackup(string reason)
        {
            // Snapshot once: FinishRunning nulls RunningCancel right after clearing RunningTask
            // (before lowering Running), mirroring the OnGoingBackup snapshot guard in ServerBackupRunner. Reading it
            // into a local means a concurrent FinishRunning that nulls the field cannot turn our null-check
            // into an NRE.
            var runningCancel = RunningCancel;
            if (runningCancel == null)
                return;

            SetCancelReason(reason);

            try
            {
                runningCancel.Cancel();
            }
            catch (ObjectDisposedException)
            {
                // The backup already finished and its token was disposed by the operation continuation
                // before we got here. Cancellation is moot — nothing to stop.
            }
        }

        internal void SetCancelReason(string reason)
        {
            Interlocked.CompareExchange(ref _cancelReason, reason, null);
        }

        internal void ResetCancelReason()
        {
            Volatile.Write(ref _cancelReason, null);
        }

        internal NextBackup GetNextOccurrenceAfter(DateTime nowUtc)
        {
            var nextFullBackup = BackupUtils.GetNextBackupOccurrence(new BackupUtils.NextBackupOccurrenceParameters
            {
                BackupFrequency = Configuration.FullBackupFrequency,
                Configuration = Configuration,
                LastBackupUtc = nowUtc
            });

            var nextIncrementalBackup = BackupUtils.GetNextBackupOccurrence(new BackupUtils.NextBackupOccurrenceParameters
            {
                BackupFrequency = Configuration.IncrementalBackupFrequency,
                Configuration = Configuration,
                LastBackupUtc = nowUtc
            });

            if (nextFullBackup == null && nextIncrementalBackup == null)
                return null;

            var nextOccurrence = BackupUtils.GetNextBackupDateTime(nextFullBackup, nextIncrementalBackup, delayUntil: null);
            var nextOccurrenceUtc = nextOccurrence.ToUniversalTime();

            return new NextBackup
            {
                TaskId = Configuration.TaskId,
                IsFull = nextOccurrence == nextFullBackup,
                DateTime = nextOccurrenceUtc,
                TimeSpan = nextOccurrenceUtc - nowUtc
            };
        }

        public List<BackupDecision> GetDecisionLog()
        {
            return _decisionLog.GetDecisions();
        }

        internal List<BackupDecision> GetMatchingDecisions(long? afterEtag, long upToEtag, long afterId, long upToId, BackupDecisionLogFilter filter, int take, out int totalMatches)
        {
            return _decisionLog.GetMatchingDecisions(afterEtag, upToEtag, afterId, upToId, filter, take, out totalMatches);
        }

        public bool TryGetLatestDecision(out BackupDecision decision)
        {
            return _decisionLog.TryGetLatest(out decision);
        }
    }
}
