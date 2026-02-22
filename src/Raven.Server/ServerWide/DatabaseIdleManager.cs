using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Threading;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Exceptions.Database;
using Raven.Client.Extensions;
using Sparrow.Server;
using Raven.Server.Documents;
using Raven.Server.ServerWide.Commands;
using Raven.Server.ServerWide.Commands.PeriodicBackup;
using Raven.Server.Utils;
using Sparrow;
using Sparrow.Logging;
using Sparrow.Server.Threading;
using System.Linq;
using System.Threading.Tasks;
using Jint;
using NCrontab.Advanced;
using Raven.Client.Documents.Operations.Replication;
using Raven.Client.Util;
using Raven.Server.Documents.Replication;
using Raven.Server.ServerWide.Context;
using Raven.Server.Web.System;

namespace Raven.Server.ServerWide
{
    public record IdleDatabaseInfo(Dictionary<string, long> ReplicationInfo, string ChangeVector, string HubVoronDbId = null);

    public sealed class DatabaseIdleManager : IDisposable
    {
        private readonly ServerStore _serverStore;
        private readonly Logger _logger;
        private readonly AsyncGuard _disposing = new();
        private readonly Timer _timer;

        private readonly TimeSpan _frequencyToCheckForIdleDatabases;

        private readonly ConcurrentDictionary<string, IdleDatabaseInfo> _idleDatabases = new(StringComparer.OrdinalIgnoreCase);
        private readonly ConcurrentDictionary<string, Lazy<DatabaseWakeupTimer>> _wakeupTimers = new(StringComparer.OrdinalIgnoreCase);

        private readonly int _dueTimeOnRetry = (int)TimeSpan.FromSeconds(5).TotalMilliseconds;

        private bool _isDisposed;


        public DatabaseIdleManager(ServerStore serverStore)
        {
            _serverStore = serverStore ?? throw new ArgumentNullException(nameof(serverStore));
            _logger = LoggingSource.Instance.GetLogger<DatabaseIdleManager>(nameof(DatabaseIdleManager));
            _timer = new Timer(IdleOperationsCallback, state: null, dueTime: _frequencyToCheckForIdleDatabases, period: TimeSpan.FromDays(7));
            _frequencyToCheckForIdleDatabases = serverStore.Configuration.Databases.FrequencyToCheckForIdle.AsTimeSpan;
        }

        public enum DatabaseActivityState
        {
            /// <summary>
            /// Database is physically in memory (Loaded), currently waking up (Loading), or currently performing an unloading.
            /// Action: Traffic should wait for the task or proceed.
            /// </summary>
            Active,

            /// <summary>
            /// Database is strictly unloaded and marked as idle.
            /// Action: Traffic should be rejected (TCP) or trigger specific wakeup logic (REST).
            /// </summary>
            Idle,

            /// <summary>
            /// Database does not exist or is in an undefined state.
            /// </summary>
            Missing
        }

        /// <summary>
        /// The Single Source of Truth for database state.
        /// Resolves race conditions between physical loading status and logical idle status.
        /// </summary>
        public DatabaseActivityState GetActivityState(string databaseName)
        {
            if (_serverStore.DatabasesLandlord.DatabasesCache.TryGetValue(databaseName, out var task) &&
                task.IsFaulted == false &&
                task.IsCanceled == false)
                return DatabaseActivityState.Active;

            if (_idleDatabases.ContainsKey(databaseName))
                return DatabaseActivityState.Idle;

            return DatabaseActivityState.Missing;
        }

        /// <summary>
        /// Atomically attempts to move a database to Idle state.
        /// </summary>
        internal bool TryEnterIdleState(string databaseName, IdleDatabaseInfo idleInfo, IdleDatabaseActivity nextActivity)
        {
            // 1. Optimistic Lock (Logical Idle)
            // State Machine still returns 'Active' here because Landlord has priority (Task is still in cache).
            _idleDatabases[databaseName] = idleInfo;

            // 2. Physical Unload
            // This blocks until the database is disposed and removed from Landlord cache.
            // The moment this returns true, GetActivityState() stops seeing the Task
            // and starts seeing the _idleDatabases entry -> State flips to 'Idle'.
            if (_serverStore.DatabasesLandlord.UnloadDirectly(databaseName, nextActivity))
            {
                if (_logger.IsInfoEnabled)
                    _logger.Info($"Database '{databaseName}' successfully entered idle state. ChangeVector: '{idleInfo.ChangeVector}'.");

                return true;
            }

            if (_logger.IsOperationsEnabled)
                _logger.Operations($"Failed to unload database '{databaseName}' for idle state.");

            // Rollback
            _idleDatabases.TryRemove(databaseName, out _);
            
            if (_logger.IsInfoEnabled)
                _logger.Info($"Database '{databaseName}' failed to unload. Reverted idle status.");

            return false;
        }

        /// <summary>
        /// Called ONLY by DatabasesLandlord when a database creation task is started and on database deletion.
        /// Cleans up the idle state to reflect that the database is now officially waking up.
        /// </summary>
        public void RemoveFromIdleDatabases(string databaseName)
        {
            if (_idleDatabases.TryRemove(databaseName, out _) == false)
                return;

            if (_logger.IsInfoEnabled)
                _logger.Info($"Database '{databaseName}' marked as waking up (removed from idle state).");

            ForTestingPurposes?.AfterDatabaseRemovedFromIdle?.Invoke(databaseName);
        }

        /// <summary>
        /// Safe accessor for Idle Info.
        /// Returns false if the database is effectively Active (Loading/Loaded), 
        /// protecting against race conditions where the dictionary entry still exists during wakeup.
        /// </summary>
        public bool TryGetIdleInfo(string databaseName, out IdleDatabaseInfo info)
        {
            // We want to return Idle info only if the database is strictly Idle.
            if (GetActivityState(databaseName) is DatabaseActivityState.Idle)
                return _idleDatabases.TryGetValue(databaseName, out info);

            info = null;
            return false;
        }

        /// <summary>
        /// Prevents waking up an idle database for specific commands that are safe to execute without loading the database.
        /// </summary>
        /// <param name="databaseName">Database name to check</param>
        /// <param name="commandType">Command type to check</param>
        /// <returns>True if the command should prevent waking up the idle database, otherwise false</returns>
        internal bool ShouldPreventWakeUpIdleDatabase(string databaseName, string commandType)
        {
            if (GetActivityState(databaseName) is not DatabaseActivityState.Idle)
                return false;

            switch (commandType)
            {
                case nameof(PutServerWideBackupConfigurationCommand):
                case nameof(UpdatePeriodicBackupStatusCommand):
                case nameof(UpdateResponsibleNodeForTasksCommand):
                    return true;

                default:
                    return false;
            }
        }

        public void RescheduleNextIdleDatabaseActivity(string databaseName, IdleDatabaseActivity idleDatabaseActivity)
        {
            if (idleDatabaseActivity == null)
            {
                DisposeWakeupTimer(databaseName);
                return;
            }

            // in case the DueTime is negative or zero, the callback will be called immediately and the database will be loaded.
            _ = _wakeupTimers.AddOrUpdate(databaseName,
                _ => new Lazy<DatabaseWakeupTimer>(() => new DatabaseWakeupTimer(databaseName, idleDatabaseActivity, NextScheduledActivityCallback)),
                (_, timer) =>
                {
                    timer.Value.Update(idleDatabaseActivity);
                    return timer;
                }).Value;
        }

        internal void RescheduleTimerIfDatabaseIdleOnUpdatedResponsibleNode(string databaseName)
        {
            if (GetActivityState(databaseName) is not DatabaseIdleManager.DatabaseActivityState.Idle)
                return;

            var nextIdleDatabaseActivity = BackupUtils.GetEarliestIdleDatabaseActivity(new BackupUtils.EarliestIdleDatabaseActivityParameters()
            {
                DatabaseName = databaseName,
                NotificationCenter = _serverStore.NotificationCenter,
                Logger = _logger,
                ServerStore = _serverStore,
                IsIdle = true
            });

            RescheduleNextIdleDatabaseActivity(databaseName, nextIdleDatabaseActivity);
        }

        internal void RescheduleTimerIfDatabaseIdle(string db, object state)
        {
            if (GetActivityState(db) is not DatabaseActivityState.Idle)
                return;

            if (state is long taskId == false)
            {
                Debug.Assert(state == null,
                    $"This is probably a bug. This method should be called only for {nameof(PutServerWideBackupConfigurationCommand)} and the state should be the database periodic backup task id.");
                //The database is excluded from the server-wide backup.
                return;
            }

            PeriodicBackupConfiguration backupConfig;
            using (_serverStore.ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
            using (ctx.OpenReadTransaction())
            using (var rawRecord = _serverStore.Cluster.ReadRawDatabaseRecord(ctx, db))
            {
                backupConfig = rawRecord.GetPeriodicBackupConfiguration(taskId);

                if (backupConfig == null)
                {
                    //`indexPerDatabase` was collected from the previous transaction. The database can be excluded in the meantime.
                    if (_logger.IsInfoEnabled)
                        _logger.Info($"Could not reschedule the wakeup timer for idle database '{db}', because there is no backup task with id '{taskId}'.");
                    return;
                }
            }

            var tag = BackupUtils.GetResponsibleNodeTag(_serverStore, db, backupConfig.TaskId);
            if (_serverStore.Engine.Tag != tag)
            {
                if (_logger.IsOperationsEnabled && tag != null)
                    _logger.Operations($"Could not reschedule the wakeup timer for idle database '{db}', because backup task '{backupConfig.Name}' with id '{taskId}' belongs to node '{tag}' current node is '{_serverStore.Engine.Tag}'.");
                return;
            }

            if (backupConfig.Disabled || backupConfig.FullBackupFrequency == null && backupConfig.IncrementalBackupFrequency == null)
                return;

            var now = SystemTime.UtcNow;
            DateTime wakeup;
            if (backupConfig.FullBackupFrequency == null)
            {
                wakeup = CrontabSchedule.Parse(backupConfig.IncrementalBackupFrequency).GetNextOccurrence(now);
            }
            else
            {
                wakeup = CrontabSchedule.Parse(backupConfig.FullBackupFrequency).GetNextOccurrence(now);
                if (backupConfig.IncrementalBackupFrequency != null)
                {
                    var incremental = CrontabSchedule.Parse(backupConfig.IncrementalBackupFrequency).GetNextOccurrence(now);
                    wakeup = new DateTime(Math.Min(wakeup.Ticks, incremental.Ticks));
                }
            }

            wakeup = DateTime.SpecifyKind(wakeup, DateTimeKind.Utc);
            var nextIdleDatabaseActivity = new IdleDatabaseActivity(IdleDatabaseActivityType.WakeUpDatabase, wakeup);
            RescheduleNextIdleDatabaseActivity(db, nextIdleDatabaseActivity);

            if (_logger.IsOperationsEnabled)
                _logger.Operations($"Rescheduling the wakeup timer for idle database '{db}', because backup task '{backupConfig.Name}' with id '{taskId}' which belongs to node '{_serverStore.Engine.Tag}', new timer is set to: '{nextIdleDatabaseActivity.DateTime}', with dueTime: {nextIdleDatabaseActivity.DueTime} ms.");
        }

        public void DisposeWakeupTimer(string databaseName)
        {
            if (_wakeupTimers.TryRemove(databaseName, out Lazy<DatabaseWakeupTimer> oldTimer) && oldTimer.IsValueCreated)
                oldTimer.Value.Dispose();
        }

        private void NextScheduledActivityCallback(string databaseName, IdleDatabaseActivity nextIdleDatabaseActivity)
        {
            try
            {
                if (_disposing.TryEnter(out var idx) == false)
                    throw new ObjectDisposedException(nameof(DatabasesLandlord), $"The server is being disposed, cannot access to database '{databaseName}'.");

                try
                {
                    if (_serverStore.ServerShutdown.IsCancellationRequested)
                        return;

                    switch (nextIdleDatabaseActivity.Type)
                    {
                        case IdleDatabaseActivityType.UpdateBackupStatusOnly:

                            PeriodicBackupStatus backupStatus = _serverStore.DatabaseInfoCache.BackupStatusStorage.GetBackupStatus(databaseName, nextIdleDatabaseActivity.TaskId);

                            backupStatus.LastIncrementalBackup = backupStatus.LastIncrementalBackupInternal = nextIdleDatabaseActivity.DateTime;
                            backupStatus.LocalBackup.LastIncrementalBackup = nextIdleDatabaseActivity.DateTime;
                            backupStatus.LocalBackup.IncrementalBackupDurationInMs = 0;

                            var backupResult = new BackupResult();
                            backupResult.AddMessage($"Skipping incremental backup because no changes were made from last full backup on {backupStatus.LastFullBackup}.");
                            
                            BackupUtils.SaveBackupStatus(backupStatus, databaseName, _serverStore, _logger, backupResult);

                            // choose the next backup that will arrive the earliest
                            nextIdleDatabaseActivity = BackupUtils.GetEarliestIdleDatabaseActivity(new BackupUtils.EarliestIdleDatabaseActivityParameters
                            {
                                DatabaseName = databaseName,
                                LastEtag = nextIdleDatabaseActivity.LastEtag,
                                Logger = _logger,
                                ServerStore = _serverStore,
                                IsIdle = true
                            });

                            RescheduleNextIdleDatabaseActivity(databaseName, nextIdleDatabaseActivity);
                            break;

                        case IdleDatabaseActivityType.WakeUpDatabase:
                            if (_serverStore.ConcurrentBackupsCounter.CanRunBackup(ShardHelper.ToDatabaseName(databaseName)) == false)
                            {
                                // reached max concurrent backups
                                var delayInMs = RescheduleDatabaseWakeup();
                                if (_logger.IsInfoEnabled)
                                    _logger.Info($"Delaying the start of the database '{databaseName}' for running a backup because we reached " +
                                                 $"max concurrent backups ({_serverStore.ConcurrentBackupsCounter.MaxNumberOfConcurrentBackups}), will retry the wakeup in {delayInMs:#,#;;0}ms");
                                break;
                            }

                            if (BackupUtils.CanServerRunBackup(_serverStore) == false)
                            {
                                // the server cannot run the backup anyway (low memory, low cpu credits or high dirty memory state)
                                var delayInMs = RescheduleDatabaseWakeup();
                                if (_logger.IsInfoEnabled)
                                    _logger.Info($"Delaying the start of the database '{databaseName}' for running a backup because we are in a low memory state, " +
                                                 $"will retry the wakeup in {delayInMs:#,#;;0}ms");
                                break;
                            }

                            var startDatabaseForBackup = _serverStore.ConcurrentBackupsCounter.TryStartDatabaseForBackup();
                            if (startDatabaseForBackup == null)
                            {
                                // reached max concurrent loading of databases for backup
                                var delayInMs = RescheduleDatabaseWakeup();
                                if (_logger.IsInfoEnabled)
                                    _logger.Info($"Delaying the start of the database '{databaseName}' for running a backup because we reached max concurrent loading of databases " +
                                                 $"for backup ({_serverStore.ConcurrentBackupsCounter.MaxNumberOfConcurrentBackups}), will retry the wakeup in {delayInMs:#,#;;0}ms");
                            }
                            else
                            {
                                _ = _serverStore.DatabasesLandlord.TryGetOrCreateResourceStore(databaseName, nextIdleDatabaseActivity.DateTime).ContinueWith(t =>
                                {
                                    startDatabaseForBackup.Dispose();

                                    var ex = t.Exception.ExtractSingleInnerException();
                                    if (ex is DatabaseConcurrentLoadTimeoutException e)
                                    {
                                        // database failed to load
                                        var delayInMs = RescheduleDatabaseWakeup();
                                        if (_logger.IsInfoEnabled)
                                            _logger.Info($"Failed to start database '{databaseName}' for running a backup, will retry the wakeup in {delayInMs:#,#;;0}ms", e);
                                    }
                                });
                            }
                            break;

                            int RescheduleDatabaseWakeup()
                            {
                                ForTestingPurposes?.RescheduleDatabaseWakeupMre?.Set();

                                var delayInMs = _dueTimeOnRetry + Random.Shared.Next(0, _dueTimeOnRetry);
                                nextIdleDatabaseActivity.DateTime = DateTime.UtcNow.AddMilliseconds(delayInMs);
                                RescheduleNextIdleDatabaseActivity(databaseName, nextIdleDatabaseActivity);
                                return delayInMs;
                            }
                    }
                }
                finally
                {
                    _disposing.Exit(idx);
                }
            }
            catch (Exception e)
            {
                // we have to swallow any exception here.

                if (_logger.IsOperationsEnabled)
                    _logger.Operations($"Failed to schedule the next activity for the idle database '{databaseName}'.", e);

                ForTestingPurposes?.OnFailedRescheduleNextScheduledActivity?.Invoke(e, databaseName);
            }
        }

        internal bool ShouldContinueDispose(string name, IdleDatabaseActivity idleDatabaseActivity)
        {
            if (name == null)
                return true;

            if (_wakeupTimers.TryRemove(name, out var timer) && timer.IsValueCreated)
                timer.Value.Dispose();

            if (idleDatabaseActivity == null)
                return true;

            if (idleDatabaseActivity.DateTime.HasValue == false)
                return true;

            if (ForTestingPurposes?.SkipShouldContinueDisposeCheck == true)
                return true;

            // Unloading and then loading a database can use a lot of resources.
            // Therefore, we don't want to unload the database unless we're sure it will be loaded again soon.
            return idleDatabaseActivity.DueTime > TimeSpan.FromMinutes(5).TotalMilliseconds;
        }

        private void IdleOperationsCallback(object state) => IdleOperations();

        public void IdleOperations(Dictionary<StringSegment, DatabasesDebugHandler.IdleDatabaseStatistics> stats = null)
        {
            try
            {
                foreach (var db in _serverStore.DatabasesLandlord.DatabasesCache)
                {
                    try
                    {
                        if (db.Value.Status != TaskStatus.RanToCompletion)
                            continue;

                        var database = db.Value.Result;

                        if (DatabaseNeedsToRunIdleOperations(database, out var mode))
                            database.RunIdleOperations(mode);
                    }
                    catch (Exception e)
                    {
                        if (_logger.IsInfoEnabled)
                            _logger.Info("Error during idle operation run for " + db.Key, e);
                    }
                }

                try
                {
                    _serverStore.Server.Statistics.MaybePersist(_serverStore.ContextPool, _logger);

                    foreach (var databaseKvp in _serverStore.DatabasesLandlord.LastRecentlyUsed.ForceEnumerateInThreadSafeManner())
                    {
                        DatabasesDebugHandler.IdleDatabaseStatistics statistics = null;
                        if (stats != null)
                        {
                            if (stats.TryGetValue(databaseKvp.Key, out statistics) == false)
                                stats[databaseKvp.Key] = statistics = new DatabasesDebugHandler.IdleDatabaseStatistics();
                        }
                        
                        if (_serverStore.DatabasesLandlord.CanUnloadDatabase(databaseKvp.Key, databaseKvp.Value, statistics: statistics, out DocumentDatabase database) == false)
                            continue;

                        IdleDatabaseInfo idleInfo;
                        using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext documentsContext))
                        using (var tx = documentsContext.OpenReadTransaction())
                        {
                            var dbIdEtagDictionary = new Dictionary<string, long>();
                            foreach (var kvp in DocumentsStorage.GetAllReplicatedEtags(documentsContext))
                            {
                                if (Guid.TryParse(kvp.Key, out Guid parsedGuid))
                                    dbIdEtagDictionary[parsedGuid.ToBase64Unpadded()] = kvp.Value;
                                else
                                    dbIdEtagDictionary[kvp.Key] = kvp.Value;
                            }

                            var changeVector = DocumentsStorage.GetDatabaseChangeVector(tx.InnerTransaction);
                            idleInfo = new IdleDatabaseInfo(dbIdEtagDictionary, changeVector, database.DbBase64Id);
                        }

                        var repInfoStr = idleInfo.ReplicationInfo != null && idleInfo.ReplicationInfo.Count > 0
                            ? string.Join(", ", idleInfo.ReplicationInfo.Select(kvp => $"{kvp.Key}={kvp.Value}"))
                            : "(empty)";

                        if (_logger.IsOperationsEnabled)
                        {
                            _logger.Operations($"[IdleInfo] {database.Name}: CV='{idleInfo.ChangeVector}' ReplicationInfo={repInfoStr}");
                        }

                        var nextActivity = database.PeriodicBackupRunner.GetNextIdleDatabaseActivity(database.Name);

                        if (TryEnterIdleState(database.Name, idleInfo, nextActivity))
                        {
                            if (_logger.IsOperationsEnabled)
                                _logger.Operations($"{database.Name} was unloaded due to idleness with change-vector `{idleInfo.ChangeVector}`");
                        }
                        else
                        {
                            if (_logger.IsOperationsEnabled)
                                _logger.Operations($"{database.Name} could not be unloaded due to idleness with change-vector `{idleInfo.ChangeVector}`");
                        }
                    }
                }
                catch (Exception e)
                {
                    if (_logger.IsOperationsEnabled)
                        _logger.Operations("Error during idle operations for the server", e);
                }
            }
            catch (Exception e)
            {
                if (_logger.IsOperationsEnabled)
                    _logger.Operations("Unexpected error during idle operations for the server", e);
            }
            finally
            {
                try
                {
                    if (_isDisposed == false)
                        _timer.Change(dueTime: _frequencyToCheckForIdleDatabases, period: TimeSpan.FromDays(7));
                }
                catch (ObjectDisposedException)
                {
                }
            }
        }

        private bool DatabaseNeedsToRunIdleOperations(DocumentDatabase database, out DatabaseCleanupMode mode)
        {
            var now = DateTime.UtcNow;

            var envs = database.GetAllStoragesEnvironment();

            var maxLastWork = DateTime.MinValue;

            foreach (var env in envs)
            {
                if (env.Environment.LastWorkTime > maxLastWork)
                    maxLastWork = env.Environment.LastWorkTime;
            }

            if ((now - maxLastWork).CompareTo(database.Configuration.Databases.DeepCleanupThreshold.AsTimeSpan) > 0)
            {
                mode = DatabaseCleanupMode.Deep;
                return true;
            }

            if ((now - database.LastIdleTime).CompareTo(database.Configuration.Databases.RegularCleanupThreshold.AsTimeSpan) > 0)
            {
                mode = DatabaseCleanupMode.Regular;
                return true;
            }

            mode = DatabaseCleanupMode.None;
            return false;
        }

        internal void ThrowIfIdleAndUpToDate(string database, string sinkChangeVector, PullReplicationDefinition pullReplication, TransactionOperationContext context)
        {
            if (TryGetIdleInfo(database, out var idleDatabaseInfo) == false)
                return;

            if (idleDatabaseInfo.ChangeVector == null)
                return;

            if (_serverStore.DatabasesLandlord.IsDatabaseLoaded(database))
                return;

            var dbRecord = _serverStore.Cluster.ReadRawDatabaseRecord(context, database);
            var hubDbId = dbRecord?.Topology?.DatabaseTopologyIdBase64;

            FilterIrrelevantEntries(sinkChangeVector, idleDatabaseInfo, hubDbId, out var sinkCvForHubToSink, out var sinkCvForSinkToHub);

            if ((pullReplication.Mode & PullReplicationMode.HubToSink) != 0)
            {
                // Hub must wake up only if it has data the Sink doesn't have yet.
                // We compare Hub's full CV against only the Hub-origin entries from Sink's CV.
                var sinkStatus = ChangeVectorUtils.GetConflictStatus(remoteAsString: idleDatabaseInfo.ChangeVector, localAsString: sinkCvForHubToSink);
                if (sinkStatus is not ConflictStatus.AlreadyMerged)
                {
                    _serverStore.DatabasesLandlord.TryGetOrCreateDatabase(database);
                    return;
                }
            }

            if ((pullReplication.Mode & PullReplicationMode.SinkToHub) != 0)
            {
                // Hub must wake up only if Sink has new data Hub hasn't received yet.
                // We compare only the non-Hub, not-yet-replicated entries from Sink's CV against Hub's CV.
                var hubStatus = ChangeVectorUtils.GetConflictStatus(remoteAsString: sinkCvForSinkToHub, localAsString: idleDatabaseInfo.ChangeVector);
                if (hubStatus is not ConflictStatus.AlreadyMerged)
                {
                    _serverStore.DatabasesLandlord.TryGetOrCreateDatabase(database);
                    return;
                }
            }

            throw new DatabaseIdleException($"The database '{database}' is currently idle. " +
                                            $"The request was rejected to avoid waking up the database unnecessarily, " +
                                            $"as there are no new changes to replicate for the change vector '{sinkChangeVector}'.");
        }

        /// <summary>
        /// Keeping these two vectors separate prevents cross-contamination: Sink's own entries must not
        /// influence the HubToSink check, and Hub's echoed entries must not influence the SinkToHub check.
        /// </summary>
        /// <param name="sinkCvForHubToSink">contains only Hub-origin entries from Sink's CV (capped to Hub's local etag).
        /// Used to answer: "Does Sink already have everything Hub has?" (HubToSink direction)</param>
        /// <param name="sinkCvForSinkToHub">contains only non-Hub entries from Sink's CV that Hub hasn't replicated yet.
        /// Used to answer: "Does Sink have new data Hub hasn't received?" (SinkToHub direction)</param>
        internal static void FilterIrrelevantEntries(
            string sinkChangeVector,
            IdleDatabaseInfo idleInfo,
            string hubDbId,
            out string sinkCvForHubToSink,
            out string sinkCvForSinkToHub)
        {
            try
            {
                if (string.IsNullOrEmpty(sinkChangeVector))
                {
                    sinkCvForHubToSink = sinkChangeVector;
                    sinkCvForSinkToHub = sinkChangeVector;
                    return;
                }

                var sinkList = sinkChangeVector.ToChangeVectorList();

                var localEntries = new Dictionary<string, long>(StringComparer.OrdinalIgnoreCase);

                // Retrieve topology ID securely. We bypass conditional optimization.
                // hubDbId is passed as an argument.

                // For newly created databases, the local Change Vector might not yet contain the node's Topology ID,
                // but the Sink will report the Hub's real Storage Voron ID if it received echoing replication info.
                // It is CRITICAL to register `hubDbId` and `idleInfo.HubVoronDbId` so Hub correctly identifies echoed data.

                if (string.IsNullOrEmpty(hubDbId) == false)
                {
                    long etag = 0;
                    if (string.IsNullOrEmpty(idleInfo.ChangeVector) == false)
                        etag = ChangeVectorUtils.GetEtagById(idleInfo.ChangeVector, hubDbId);

                    localEntries[hubDbId] = etag;
                    if (TryBase64ToGuid(hubDbId, out var guid))
                        localEntries[guid] = etag;
                }

                if (string.IsNullOrEmpty(idleInfo.HubVoronDbId) == false)
                {
                    long etag = 0;
                    if (string.IsNullOrEmpty(idleInfo.ChangeVector) == false)
                        etag = ChangeVectorUtils.GetEtagById(idleInfo.ChangeVector, idleInfo.HubVoronDbId);

                    localEntries[idleInfo.HubVoronDbId] = etag;
                    if (TryBase64ToGuid(idleInfo.HubVoronDbId, out var guid))
                        localEntries[guid] = etag;
                }

                // ReplicationInfo keys may be GUID strings (from DbId.ToString()) while CV DbIds are Base64.
                // Build a normalized lookup that accepts both representations.
                Dictionary<string, long> replicationInfoLookup = null;
                if (idleInfo.ReplicationInfo != null && idleInfo.ReplicationInfo.Count > 0)
                {
                    replicationInfoLookup = new Dictionary<string, long>(idleInfo.ReplicationInfo.Count * 2, StringComparer.OrdinalIgnoreCase);
                    foreach (var kvp in idleInfo.ReplicationInfo)
                    {
                        replicationInfoLookup[kvp.Key] = kvp.Value;
                        // If the stored key is a GUID, also register its Base64 equivalent.
                        if (Guid.TryParse(kvp.Key, out var g))
                        {
                            var base64 = Convert.ToBase64String(g.ToByteArray()).TrimEnd('=');
                            replicationInfoLookup[base64] = kvp.Value;
                        }
                        // If the stored key is Base64, also register its GUID equivalent.
                        else if (TryBase64ToGuid(kvp.Key, out var g2))
                        {
                            replicationInfoLookup[g2] = kvp.Value;
                        }
                    }
                }

                var hubToSinkList = new List<ChangeVectorEntry>();
                var sinkToHubList = new List<ChangeVectorEntry>();

                foreach (var entry in sinkList)
                {
                    bool isHubEntry = localEntries.TryGetValue(entry.DbId, out long hubLocalEtag);

                    if (isHubEntry == false && TryBase64ToGuid(entry.DbId, out var entryGuid))
                        isHubEntry = localEntries.TryGetValue(entryGuid, out hubLocalEtag);

                    if (isHubEntry)
                    {
                        // Hub-origin entry: belongs in HubToSink vector only.
                        // Cap it to Hub's local etag — Sink may report a higher value if it received
                        // data from Hub that Hub itself has since rolled back or hasn't committed yet.
                        var cappedEtag = Math.Min(entry.Etag, hubLocalEtag);
                        hubToSinkList.Add(new ChangeVectorEntry { DbId = entry.DbId, Etag = cappedEtag, NodeTag = entry.NodeTag });
                        continue;
                    }

                    // Non-Hub entry: belongs in SinkToHub vector only, unless Hub already replicated it.
                    bool alreadyReplicated = replicationInfoLookup != null &&
                                            replicationInfoLookup.TryGetValue(entry.DbId, out long lastReplicated) &&
                                            entry.Etag <= lastReplicated;

                    if (alreadyReplicated == false)
                        sinkToHubList.Add(entry);
                }

                sinkCvForHubToSink = hubToSinkList.Count == 0 ? string.Empty : hubToSinkList.SerializeVector();
                sinkCvForSinkToHub = sinkToHubList.Count == 0 ? string.Empty : sinkToHubList.SerializeVector();
            }
            catch
            {
                sinkCvForHubToSink = sinkChangeVector;
                sinkCvForSinkToHub = sinkChangeVector;
            }
        }

        private static bool TryBase64ToGuid(string base64, out string guidString)
        {
            guidString = null;
            if (string.IsNullOrEmpty(base64) || base64.Length > 24)
                return false;
            try
            {
                // Base64 DbIds are 22 chars (16 bytes without padding); add padding if needed
                var padded = base64.Length % 4 == 0 ? base64 : base64.PadRight(base64.Length + (4 - base64.Length % 4), '=');
                var bytes = Convert.FromBase64String(padded);
                if (bytes.Length != 16)
                    return false;
                guidString = new Guid(bytes).ToString();
                return true;
            }
            catch
            {
                return false;
            }
        }

        public void Dispose()
        {
            if (_isDisposed)
                return;

            _disposing.CloseAndLock();

            try
            {
                var exceptionAggregator = new ExceptionAggregator(_logger, errorMsg: "Failure to dispose landlord");
                exceptionAggregator.Execute(() =>
                {
                    var handles = new List<WaitHandle>();
                    foreach (var timer in _wakeupTimers.Values)
                    {
                        if (timer.IsValueCreated == false)
                            continue;

                        var handle = new ManualResetEvent(false);
                        timer.Value.Dispose(handle);
                        handles.Add(handle);
                    }

                    if (handles.Count <= 0)
                        return;

                    var count = handles.Count;
                    var batchSize = Math.Min(64, count);

                    var numberOfBatches = count / batchSize;
                    if (count % batchSize != 0)
                    {
                        // if we have a reminder, we need another batch
                        numberOfBatches++;
                    }

                    var batch = new WaitHandle[batchSize];
                    for (var i = 0; i < numberOfBatches; i++)
                    {
                        var toCopy = Math.Min(64, count - i * batchSize);
                        handles.CopyTo(i * batchSize, batch, 0, toCopy);
                        WaitHandle.WaitAll(batch);
                    }
                });

                exceptionAggregator.Execute(() => _timer?.Dispose());
                exceptionAggregator.Execute(_disposing.Dispose);
                exceptionAggregator.ThrowIfNeeded();
            }
            finally
            {
                _isDisposed = true;
            }
        }

        private sealed class DatabaseWakeupTimer : IDisposable
        {
            private IdleDatabaseActivity _activity;

            private readonly string _databaseName;
            private readonly Action<string, IdleDatabaseActivity> _callback;
            private readonly Timer _timer;

            public DatabaseWakeupTimer([NotNull] string databaseName, [NotNull] IdleDatabaseActivity activity, [NotNull] Action<string, IdleDatabaseActivity> callback)
            {
                _databaseName = databaseName ?? throw new ArgumentNullException(nameof(databaseName));
                _activity = activity ?? throw new ArgumentNullException(nameof(activity));
                _callback = callback ?? throw new ArgumentNullException(nameof(callback));

                _timer = new Timer(TimerCallback, state: null, _activity.DueTime, period: Timeout.Infinite);
            }

            public void Update(IdleDatabaseActivity activity)
            {
                _activity = activity;
                _timer.Change(activity.DueTime, Timeout.Infinite);
            }

            private void TimerCallback(object state)
            {
                _callback(_databaseName, _activity);
            }

            public void Dispose()
            {
                _timer?.Dispose();
            }

            public void Dispose(WaitHandle notifyObject)
            {
                _timer?.Dispose(notifyObject);
            }
        }

        internal TestingStuff ForTestingPurposes;

        internal TestingStuff ForTestingPurposesOnly()
        {
            if (ForTestingPurposes != null)
                return ForTestingPurposes;

            return ForTestingPurposes = new TestingStuff();
        }

        internal sealed class TestingStuff
        {
            internal bool SkipShouldContinueDisposeCheck = false;
            internal ManualResetEventSlim RescheduleDatabaseWakeupMre = null;
            internal Action<Exception, string> OnFailedRescheduleNextScheduledActivity = null;
            internal Action<string> AfterDatabaseRemovedFromIdle = null;
        }
    }

    public sealed class IdleDatabaseActivity
    {
        public long LastEtag { get; }
        public IdleDatabaseActivityType Type { get; }
        public DateTime? DateTime { get; internal set; }
        public long TaskId { get; }
        public int DueTime => DateTime.HasValue
            ? (int)Math.Min(int.MaxValue, Math.Max(0, (DateTime.Value - System.DateTime.UtcNow).TotalMilliseconds))
            : 0;

        public IdleDatabaseActivity(IdleDatabaseActivityType type)
        {
            LastEtag = 0;
            Type = type;
            TaskId = 0;

            // DateTime should be only null in tests
            DateTime = null;
        }

        public IdleDatabaseActivity(IdleDatabaseActivityType type, DateTime timeOfActivity, long taskId = 0, long lastEtag = 0)
        {
            LastEtag = lastEtag;
            Type = type;
            TaskId = taskId;

            Debug.Assert(timeOfActivity.Kind != DateTimeKind.Unspecified);
            DateTime = timeOfActivity.ToUniversalTime();
        }
    }

    public enum IdleDatabaseActivityType
    {
        WakeUpDatabase,
        UpdateBackupStatusOnly
    }
}
