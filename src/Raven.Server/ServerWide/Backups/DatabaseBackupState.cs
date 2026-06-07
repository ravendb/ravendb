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
    /// <summary>
    /// Holds all in-memory state that <see cref="ServerBackupRunner"/> tracks for a single periodic
    /// backup task on a specific database. One instance is created per (database, task-id) pair and
    /// lives for the lifetime of that task registration. It acts as the in-memory counterpart to the
    /// persisted <see cref="PeriodicBackupStatus"/> and drives every scheduling decision in the
    /// polling loop — next-backup time, running flag, operation id, and the decision log.
    /// </summary>
    public class DatabaseBackupState
    {
        public const int MaxDecisionLogSize = 1024; // was 32; raised for richer history during diagnostics

        internal readonly string DatabaseName;

        public readonly string OriginalDatabaseName;

        public long OperationId { get; internal set; }

        public PeriodicBackupConfiguration Configuration { get; set; }

        public PeriodicBackupStatus BackupStatus { get; set; }

        public PeriodicBackupStatus RunningBackupStatus { get; set; }

        public NextBackup NextBackup { get; set; }

        public DateTime CreatedAt { get; set; }

        public DateTime StartTimeInUtc { get; set; }

        public DateTime? DatabaseWakeUpTimeUtc { get; set; }

        public MultipleUseFlag Stale { get; } = new();

        private readonly List<(DateTime Time, string Reason)> _decisionLog = new();

        public MultipleUseFlag Running { get; } = new();

        public RunningBackupTask RunningTask { get; set; }

        /// <summary>
        /// The live cancellation handle for the in-flight backup. Set alongside <see cref="RunningTask"/>
        /// when a backup starts (<see cref="Documents.PeriodicBackup.BackupTask.Run"/>) and cleared in
        /// <see cref="ServerBackupRunner"/>.<c>FinishBackup</c> after <see cref="Running"/> is lowered and
        /// <see cref="RunningTask"/> is nulled. This is the same <see cref="OperationCancelToken"/> the
        /// backup polls at every expensive boundary (and which is already linked to the database shutdown
        /// token), so a trigger calling <see cref="CancelRunningBackup"/> stops the work in bounded time.
        /// </summary>
        public OperationCancelToken RunningCancel { get; internal set; }

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

        /// <summary>
        /// Computes the next scheduled backup time by reading the most up-to-date local status and
        /// delegating to <see cref="BackupUtils.GetNextBackupDetails"/>. Also returns the tag of the
        /// node currently responsible for this task, or null if the task is disabled.
        /// </summary>
        public NextBackup GetNextBackupDetails(out string responsibleNodeTag)
        {
            // Pass the in-memory BackupStatus so callers that just wrote in-memory fields
            // (DelayAsync DelayUntil; HandleDatabaseValueChanged M9 propagation) survive the
            // side-effect at line `BackupStatus = backupStatus` below when storage has no row
            // to merge (cluster-down delay, peer with no local row yet).
            var backupStatus = GetMostUpdatedLocalBackupStatusForOngoingTaskRead(Configuration.TaskId, inMemoryBackupStatus: BackupStatus, DatabaseName);
            var taskStatus = _serverStore.BackupRunner.GetTaskStatus(Configuration, DatabaseName, out responsibleNodeTag, disableLog: true);
            BackupStatus = backupStatus;
            return taskStatus == ServerBackupRunner.TaskStatus.Disabled ? null : GetNextBackupDetails(skipErrorLog: true);
        }

        /// <summary>Posts a warning notification when the backup schedule cannot be computed from the configuration.</summary>
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

        /// <summary>Posts an error notification when the cron expression in the backup configuration cannot be parsed.</summary>
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

        /// <summary>
        /// Delegates to <see cref="BackupUtils.GetNextBackupDetails"/> using this task's current
        /// configuration and cached status. Pass <paramref name="skipErrorLog"/> true when called
        /// from a read path that should not emit notifications on transient misses.
        /// </summary>
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

        /// <summary>
        /// Returns the most current <see cref="PeriodicBackupStatus"/> for this task by merging the
        /// on-disk local row with the cluster value store, preferring whichever has the later etag.
        /// Uses the gated read (<see cref="BackupStatusStorage.GetBackupStatus(string, long)"/>) so
        /// that a node which just took over responsibility from a peer does not see the previous
        /// node's status — important for W6 (IsFull-decision read) at <see cref="ServerBackupRunner.RunBackup"/>.
        /// </summary>
        public PeriodicBackupStatus GetMostUpdatedLocalBackupStatus(long taskId, PeriodicBackupStatus inMemoryBackupStatus, string databaseName)
        {
            var backupStatus = _serverStore.DatabaseInfoCache.BackupStatusStorage.GetBackupStatus(databaseName, taskId);
            return BackupUtils.ComparePeriodicBackupStatus(taskId, backupStatus, inMemoryBackupStatus);
        }

        /// <summary>
        /// Same as <see cref="GetMostUpdatedLocalBackupStatus"/> but uses the ungated read
        /// (<see cref="BackupStatusStorage.GetBackupStatusForOngoingTaskRead"/>) so a non-responsible
        /// peer can see the responsible node's cluster row when the node tags differ. Intended for
        /// the on-demand ongoing-task / Studio read path only — backup-decision callers must use
        /// the gated <see cref="GetMostUpdatedLocalBackupStatus"/>.
        /// </summary>
        public PeriodicBackupStatus GetMostUpdatedLocalBackupStatusForOngoingTaskRead(long taskId, PeriodicBackupStatus inMemoryBackupStatus, string databaseName)
        {
            var backupStatus = _serverStore.DatabaseInfoCache.BackupStatusStorage.GetBackupStatusForOngoingTaskRead(databaseName, taskId);
            return BackupUtils.ComparePeriodicBackupStatus(taskId, backupStatus, inMemoryBackupStatus);
        }

        public override string ToString()
        {
            return $"'{Configuration.Name} ({Configuration.TaskId})' for database '{DatabaseName}'";
        }

        /// <summary>
        /// Records a policy decision in the per-task decision log, trimming to <see cref="MaxDecisionLogSize"/>
        /// entries. The log is exposed through the debug timers endpoint so operators can see why a
        /// backup was skipped on a given tick.
        /// </summary>
        public void AddToDecisionLog(string reason, DateTime now)
        {
            lock (_decisionLog)
            {
                _decisionLog.Insert(0, (now, reason));

                if (_decisionLog.Count > MaxDecisionLogSize)
                    _decisionLog.RemoveAt(_decisionLog.Count - 1);
            }
        }

        /// <summary>
        /// Cooperatively cancels the in-flight backup (if any) by cancelling the stored
        /// <see cref="RunningCancel"/> token, and records a <c>[CANCELLED:&lt;reason&gt;]</c> entry in the
        /// decision log. Called by the disable and delete-task triggers after they raise
        /// <see cref="Stale"/>. (db-delete does not call this: that path is cancelled and awaited by
        /// <see cref="Documents.DocumentDatabase"/>.<c>Dispose</c> before <see cref="ServerBackupRunner"/>.
        /// <c>RemoveDatabase</c> runs, via the DatabaseShutdown linkage + Operations.Dispose.)
        /// When no backup is running (<see cref="RunningCancel"/> is null) this is a
        /// no-op and writes nothing, so the "stale only" case leaves no cancellation marker. Idempotent
        /// and safe to call repeatedly or after the token has been disposed: the continuation disposes the
        /// token before FinishBackup nulls it, so a racing trigger can hit a disposed CTS — the
        /// <see cref="ObjectDisposedException"/> that <see cref="OperationCancelToken.Cancel"/> then throws
        /// is swallowed (cancellation is moot once the backup is already finishing). Mirrors the v7.2
        /// <c>PeriodicBackup.CancelFutureTasks</c> behavior at the server level.
        /// </summary>
        /// <remarks>
        /// Known limitation (matches v7.2; tracked as future work): a cancelled remote upload may leave
        /// orphaned multipart parts on S3/Azure/GCS. Aborting them requires per-provider logic and is out
        /// of scope here.
        /// </remarks>
        internal void CancelRunningBackup(string reason)
        {
            // Snapshot once: FinishBackup nulls RunningCancel after lowering Running and clearing
            // RunningTask, mirroring the OnGoingBackup snapshot guard in ServerBackupRunner. Reading it
            // into a local means a concurrent FinishBackup that nulls the field cannot turn our null-check
            // into an NRE.
            var runningCancel = RunningCancel;
            if (runningCancel == null)
                return;

            AddToDecisionLog($"[CANCELLED:{reason}] Backup task {Configuration.TaskId}", DateTime.UtcNow);

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

        /// <summary>Returns a snapshot of the per-task decision log, safe to read outside the lock.</summary>
        public List<(DateTime Time, string Reason)> GetDecisionLog()
        {
            lock (_decisionLog)
            {
                return new List<(DateTime Time, string Reason)>(_decisionLog);
            }
        }
    }
}
