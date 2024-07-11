using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Exceptions.Database;
using Raven.Client.Extensions;
using Raven.Client.ServerWide;
using Raven.Client.Util;
using Raven.Server.Config;
using Raven.Server.Documents.Sharding;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.NotificationCenter.Notifications.Details;
using Raven.Server.NotificationCenter.Notifications.Server;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Commands;
using Raven.Server.ServerWide.Commands.PeriodicBackup;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Raven.Server.Web.System;
using Sparrow;
using Sparrow.Json;
using Sparrow.Logging;
using Sparrow.Server.Threading;
using Sparrow.Utils;
using Voron.Exceptions;
using Voron.Util.Settings;

namespace Raven.Server.Documents
{
    public sealed class DatabasesLandlord : IDisposable
    {
        public const string Init = "Init";
        public const string DoNotRemove = "DoNotRemove";

        private readonly AsyncGuard _disposing;

        public readonly ConcurrentDictionary<StringSegment, DateTime> LastRecentlyUsed =
            new ConcurrentDictionary<StringSegment, DateTime>(StringSegmentComparer.OrdinalIgnoreCase);

        private readonly ConcurrentDictionary<string, Timer> _wakeupTimers = new ConcurrentDictionary<string, Timer>();

        public readonly ResourceCache<DocumentDatabase> DatabasesCache = new ResourceCache<DocumentDatabase>();
        public readonly ResourceCache<ShardedDatabaseContext> ShardedDatabasesCache = new ResourceCache<ShardedDatabaseContext>();

        private readonly Logger _logger;
        private readonly ServerStore _serverStore;

        // used in ServerWideBackupStress
        internal bool SkipShouldContinueDisposeCheck = false;
        internal Action<(DocumentDatabase Database, string caller)> AfterDatabaseCreation;
        internal SemaphoreSlim _databaseSemaphore;
        internal TimeSpan _concurrentDatabaseLoadTimeout;
        internal int _dueTimeOnRetry = 60_000;

        public DatabasesLandlord(ServerStore serverStore)
        {
            _disposing = new AsyncGuard();
            
            _serverStore = serverStore;
            _databaseSemaphore = new SemaphoreSlim(_serverStore.Configuration.Databases.MaxConcurrentLoads);
            _concurrentDatabaseLoadTimeout = _serverStore.Configuration.Databases.ConcurrentLoadTimeout.AsTimeSpan;
            _logger = LoggingSource.Instance.GetLogger<DatabasesLandlord>("Server");
            CatastrophicFailureHandler = new CatastrophicFailureHandler(this, _serverStore);
        }

        public CatastrophicFailureHandler CatastrophicFailureHandler { get; }

        public Task ClusterOnDatabaseChanged(string databaseName, long index, string type, ClusterDatabaseChangeType changeType, object changeState)
        {
            return HandleClusterDatabaseChanged(databaseName, index, type, changeType, changeState);
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
            internal Action<ServerStore> BeforeHandleClusterDatabaseChanged;
            internal Action<string> InsideHandleClusterDatabaseChanged;
            internal Action<(DocumentDatabase Database, string caller)> AfterDatabaseCreation;
            internal Action<CancellationToken> DelayIncomingReplication;
            internal int? HoldDocumentDatabaseCreation = null;
            internal Action AfterDatabaseInitialize;
            internal bool PreventedRehabOfIdleDatabase = false;
            internal ManualResetEvent DeleteDatabaseWhileItBeingDeleted = null;
            internal Action BeforeActualDelete = null;
            internal Action<DocumentDatabase> OnBeforeDocumentDatabaseInitialization;
            internal ManualResetEventSlim RescheduleDatabaseWakeupMre = null;
            internal bool ShouldFetchIdleStateImmediately = false;
            internal Action<Exception, string> OnFailedRescheduleNextScheduledActivity;
            internal bool PreventNodePromotion = false;
            internal Func<ServerStore, Task> BeforeHandleClusterTransactionOnDatabaseChanged;
            internal Action DelayNotifyFeaturesAboutStateChange;
        }

        private async Task HandleClusterDatabaseChanged(string databaseName, long index, string type, ClusterDatabaseChangeType changeType, object changeState)
        {
            ForTestingPurposes?.BeforeHandleClusterDatabaseChanged?.Invoke(_serverStore);

            if (PreventWakeUpIdleDatabase(databaseName, type))
                return;

            if (_disposing.TryEnter(out var idx) == false)
                ThrowServerIsBeingDisposed(databaseName);
            try
            {
                try
                {
                    if (_serverStore.ServerShutdown.IsCancellationRequested)
                        return;

                    // response to changed database.
                    // if disabled, unload
                    using (_serverStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
                    using (context.OpenReadTransaction())
                    using (var rawRecord = _serverStore.Cluster.ReadRawDatabaseRecord(context, databaseName))
                    {
                        if (rawRecord == null)
                        {
                            // was removed, need to make sure that it isn't loaded
                            UnloadDatabase(databaseName, dbRecordIsNull: true);
                            return;
                        }

                        if (rawRecord.IsSharded)
                        {
                            foreach (var shardRawRecord in rawRecord.GetShardedDatabaseRecords())
                            {
                                await HandleSpecificClusterDatabaseChanged(
                                    shardRawRecord.DatabaseName, index, type, changeType, context, shardRawRecord, changeState);
                            }

                            var topology = rawRecord.Sharding.Orchestrator.Topology;
                            if (topology.RelevantFor(_serverStore.NodeTag))
                            {
                                if (rawRecord.IsDisabled)
                                {
                                    if (ShardedDatabasesCache.TryGetValue(databaseName, out var shardedDatabaseTask) == false)
                                        return; // sharded database was already unloaded

                                    UnloadDatabaseInternal(databaseName, shardedDatabaseTask);
                                    return;
                                }

                                // we need to update this upon any shard topology change
                                // and upon migration completion
                                var databaseContext = GetOrAddShardedDatabaseContext(databaseName, rawRecord);
                                await databaseContext.UpdateDatabaseRecordAsync(rawRecord, index);
                            }
                            else
                            {
                                using (ShardedDatabasesCache.RemoveLockAndReturn(databaseName, (databaseContext) => databaseContext.Dispose(), out _))
                                {
                                }

                                _serverStore.NotificationCenter.Storage.DeleteStorageFor(databaseName);
                            }
                        }
                        else
                        {
                            await HandleSpecificClusterDatabaseChanged(databaseName, index, type, changeType, context, rawRecord, changeState);
                        }
                    }

                    // if deleted, unload / deleted and then notify leader that we removed it
                }
                catch (AggregateException ae) when (nameof(DeleteDatabase).Equals(ae.InnerException.Data["Source"]))
                {
                    // in the process of being deleted
                }
                catch (AggregateException ae) when (ae.InnerException is DatabaseDisabledException)
                {
                    // the db is already disabled when we try to disable it
                }
                catch (DatabaseDisabledException)
                {
                    // the database was disabled while we were trying to execute an action (e.g. PendingClusterTransactions)
                }
                catch (ObjectDisposedException)
                {
                    // the server is disposed when we are trying to access the database
                }
                catch (OperationCanceledException)
                {
                    // the server is disposed when we are trying to access the database
                }
                catch (DatabaseConcurrentLoadTimeoutException e)
                {
                    var title = $"Concurrent load timeout of '{databaseName}' database";

                    var message =
                        $"Failed to load database '{databaseName}' concurrently with other databases within {_serverStore.Configuration.Databases.ConcurrentLoadTimeout.AsTimeSpan}. " +
                        "Database load will be attempted on next request accessing it. If you see this on regular basis you might consider adjusting the following configuration options: " +
                        $"{RavenConfiguration.GetKey(x => x.Databases.ConcurrentLoadTimeout)} and {RavenConfiguration.GetKey(x => x.Databases.MaxConcurrentLoads)}";

                    if (_logger.IsInfoEnabled)
                        _logger.Info(message, e);

                    _serverStore.NotificationCenter.Add(AlertRaised.Create(databaseName, title, message, AlertType.ConcurrentDatabaseLoadTimeout,
                        NotificationSeverity.Warning,
                        details: new ExceptionDetails(e)));

                    throw;
                }
                catch (Exception e)
                {
                    var title = $"Failed to digest change of type '{changeType}' for database '{databaseName}' at index {index}";
                    if (_logger.IsInfoEnabled)
                        _logger.Info(title, e);
                    _serverStore.NotificationCenter.Add(AlertRaised.Create(databaseName, title, e.Message, AlertType.DeletionError, NotificationSeverity.Error,
                        details: new ExceptionDetails(e)));
                    throw;
                }
            }
            finally
            {
                _disposing.Exit(idx);
            }
        }

        private async Task HandleSpecificClusterDatabaseChanged(string databaseName, long index, string type, ClusterDatabaseChangeType changeType,
            TransactionOperationContext context,
            RawDatabaseRecord rawRecord, object changeState)
        {
            ForTestingPurposes?.InsideHandleClusterDatabaseChanged?.Invoke(type);

            if (ShouldDeleteDatabase(context, databaseName, rawRecord))
                return;

            var topology = rawRecord.Topology;
            if (topology.RelevantFor(_serverStore.NodeTag) == false)
                return;

            if (rawRecord.IsDisabled || rawRecord.DatabaseState == DatabaseStateStatus.RestoreInProgress)
            {
                UnloadDatabase(databaseName);
                return;
            }

            if (changeType == ClusterDatabaseChangeType.RecordRestored)
            {
                // - a successful restore operation ends when we successfully restored
                // the database files and saved the updated the database record
                // - this is the first time that the database was loaded so there is no need to call
                // StateChanged after the database was restored
                // - the database will be started on demand
                return;
            }

            if (DatabasesCache.TryGetValue(databaseName, out var task) == false)
            {
                // if the database isn't loaded, but it is relevant for this node, we need to create
                // it. This is important so things like replication will start pumping, and that 
                // configuration changes such as running periodic backup will get a chance to run, which
                // they wouldn't unless the database is loaded / will have a request on it.          
                task = TryGetOrCreateResourceStore(databaseName, ignoreBeenDeleted: true, caller: type);
            }

            var database = await task;

            switch (changeType)
            {
                case ClusterDatabaseChangeType.RecordChanged:
                    await database.StateChangedAsync(index);
                    if (type == ClusterStateMachine.SnapshotInstalled)
                    {
                        database.NotifyOnPendingClusterTransaction(index, changeType);
                    }
                    break;

                case ClusterDatabaseChangeType.ValueChanged:
                    await database.ValueChangedAsync(index, type, changeState);
                    break;

                case ClusterDatabaseChangeType.PendingClusterTransactions:
                case ClusterDatabaseChangeType.ClusterTransactionCompleted:

                    if (ForTestingPurposes?.BeforeHandleClusterTransactionOnDatabaseChanged != null)
                        await ForTestingPurposes.BeforeHandleClusterTransactionOnDatabaseChanged.Invoke(_serverStore);

                    database.SetIds(rawRecord);
                    database.NotifyOnPendingClusterTransaction(index, changeType);
                    break;
                default:
                    ThrowUnknownClusterDatabaseChangeType(changeType);
                    break;
            }
        }

        private bool PreventWakeUpIdleDatabase(string databaseName, string type)
        {
            if (_serverStore.IdleDatabases.ContainsKey(databaseName) == false)
                return false;

            switch (type)
            {
                case nameof(PutServerWideBackupConfigurationCommand):
                case nameof(UpdatePeriodicBackupStatusCommand):
                    return true;

                default:
                    return false;
            }
        }

        private void UnloadDatabase(string databaseName, bool dbRecordIsNull = false)
        {
            if (dbRecordIsNull)
            {
                UnloadDatabaseInternal(databaseName);
                return;
            }

            if (DatabasesCache.TryGetValue(databaseName, out var databaseTask) == false)
            {
                // database was already unloaded or deleted
                return;
            }

            UnloadDatabaseInternal(databaseName, databaseTask);
        }

        public static bool IsDatabaseDisabled(BlittableJsonReaderObject databaseRecord)
        {
            var noDisabled = databaseRecord.TryGet(nameof(DatabaseRecord.Disabled), out bool disabled) == false;
            var noDatabaseState = databaseRecord.TryGet(nameof(DatabaseRecord.DatabaseState), out DatabaseStateStatus dbState) == false;

            if (noDisabled && noDatabaseState)
                return false;

            if (noDatabaseState)
                return disabled;

            var isRestoring = dbState == DatabaseStateStatus.RestoreInProgress;
            if (noDisabled)
                return isRestoring;

            return disabled || isRestoring;
        }

        private void UnloadDatabaseInternal(string databaseName, [CallerMemberName] string caller = null)
        {
            using (DatabasesCache.RemoveLockAndReturn(databaseName, CompleteDatabaseUnloading, out _, caller))
            {
                if (ShardedDatabasesCache.TryGetAndRemove(databaseName, out var databaseContextTask))
                    databaseContextTask.Result.Dispose();
            }
        }

        private void UnloadDatabaseInternal(string databaseName, Task databaseTask)
        {
            if (databaseTask.IsCompletedSuccessfully)
            {
                UnloadDatabaseInternal(databaseName);
            }
            else if (databaseTask.IsCompleted == false)
            {
                // This case is when an unload was issued prior to the actual loading of a database.
                databaseTask.ContinueWith(t =>
                {
                    if (databaseTask.IsCompletedSuccessfully)
                    {
                        using (_serverStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
                        using (context.OpenReadTransaction())
                        using (var databaseRecord = _serverStore.Cluster.ReadRawDatabaseRecord(context, databaseName))
                        {
                            // unload only if DB is still disabled
                            if (IsDatabaseDisabled(databaseRecord.Raw))
                                UnloadDatabaseInternal(databaseName);
                        }
                    }
                });
            }
        }


        public bool ShouldDeleteDatabase(TransactionOperationContext context, string dbName, RawDatabaseRecord rawRecord, bool fromReplication = false)
        {
            var shard = ShardHelper.TryGetShardNumberFromDatabaseName(dbName, out var shardNumber);
            var tag = shard ? DatabaseRecord.GetKeyForDeletionInProgress(_serverStore.NodeTag, shardNumber) : _serverStore.NodeTag;

            var deletionInProgress = DeletionInProgressStatus.No;
            var directDelete = rawRecord.DeletionInProgress?.TryGetValue(tag, out deletionInProgress) == true &&
                               deletionInProgress != DeletionInProgressStatus.No;

            if (directDelete == false)
                return false;

            if (rawRecord.Topology.Rehabs.Contains(_serverStore.NodeTag) && fromReplication == false)
                // If the deletion was issued from the cluster observer to maintain the replication factor we need to make sure
                // that all the documents were replicated from this node, therefore the deletion will be called from the replication code.
                return false;

            // We materialize the values here because we close the read transaction
            var record = rawRecord.MaterializedRecord;

            context.Transaction.InnerTransaction.LowLevelTransaction.OnDispose += _ =>
                DeleteDatabase(dbName, deletionInProgress, record);

            return true;
        }

        public void DeleteDatabase(string dbName, DeletionInProgressStatus deletionInProgress, DatabaseRecord record)
        {
            IDisposable removeLockAndReturn = null;
            string databaseId;
            try
            {
                try
                {
                    removeLockAndReturn = DatabasesCache.RemoveLockAndReturn(dbName, CompleteDatabaseUnloading, out var database);
                    databaseId = database?.DbBase64Id;
                }
                catch (AggregateException ae) when (nameof(DeleteDatabase).Equals(ae.InnerException?.Data["Source"]))
                {
                    // this is already in the process of being deleted, we can just exit and let another thread handle it
                    ForTestingPurposes?.DeleteDatabaseWhileItBeingDeleted?.Set();
                    return;
                }
                catch (DatabaseConcurrentLoadTimeoutException e)
                {
                    if (e.Data.Contains(nameof(DeleteDatabase)))
                    {
                        // This case is when a deletion request was issued during the loading of the database.
                        // The DB will be deleted after actually finishing the loading process
                        return;
                    }

                    throw;
                }

                ForTestingPurposes?.BeforeActualDelete?.Invoke();

                if (deletionInProgress == DeletionInProgressStatus.HardDelete)
                {
                    RavenConfiguration configuration;
                    try
                    {
                        configuration = CreateDatabaseConfiguration(dbName, ignoreDisabledDatabase: true, ignoreBeenDeleted: true, ignoreNotRelevant: true,
                            databaseRecord: record);
                    }
                    catch (Exception ex)
                    {
                        configuration = null;
                        if (_logger.IsInfoEnabled)
                            _logger.Info("Could not create database configuration", ex);
                    }

                    // this can happen if the database record was already deleted
                    if (configuration != null)
                    {
                        CheckDatabasePathsIntersection(dbName, configuration);
                        DatabaseHelper.DeleteDatabaseFiles(configuration);
                    }
                }

                // At this point the db record still exists but the db was effectively deleted
                // from this node so we can also remove its secret key from this node.
                if (record.Encrypted)
                {
                    using (_serverStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
                    using (var tx = context.OpenWriteTransaction())
                    {
                        _serverStore.DeleteSecretKey(context, dbName);
                        tx.Commit();
                    }
                }

                DeleteDatabaseNotifications(dbName, throwOnError: true);

                // delete the cache info
                DeleteDatabaseCachedInfo(dbName, throwOnError: true);
            }
            finally
            {
                removeLockAndReturn?.Dispose();
            }
            NotifyLeaderAboutRemoval(dbName, databaseId);
        }

        [DoesNotReturn]
        private static void ThrowUnknownClusterDatabaseChangeType(ClusterDatabaseChangeType type)
        {
            throw new InvalidOperationException($"Unknown cluster database change type: {type}");
        }

        private void NotifyLeaderAboutRemoval(string dbName, string databaseId, string requestId = null)
        {
            requestId ??= RaftIdGenerator.NewId();
            var cmd = new RemoveNodeFromDatabaseCommand(dbName, databaseId, requestId)
            {
                NodeTag = _serverStore.NodeTag
            };
            _serverStore.SendToLeaderAsync(cmd)
                .ContinueWith(async t =>
                {
                    if (t.IsFaulted)
                    {
                        var ex = t.Exception.ExtractSingleInnerException();
                        if (ex is DatabaseDoesNotExistException)
                            return;
                    }

                    if (_logger.IsInfoEnabled)
                    {
                        try
                        {
                            await t; // throw immediately
                        }
                        catch (Exception e)
                        {
                            _logger.Info($"Failed to notify leader about removal of node {_serverStore.NodeTag} from database '{dbName}', will retry again in 15 seconds.", e);
                        }
                    }

                    try
                    {
                        await Task.Delay(TimeSpan.FromSeconds(15), _serverStore.ServerShutdown);
                    }
                    catch (OperationCanceledException)
                    {
                        return;
                    }

                    try
                    {
                        NotifyLeaderAboutRemoval(dbName, databaseId, requestId);
                    }
                    catch (Exception e)
                    {
                        if (_logger.IsOperationsEnabled)
                        {
                            _logger.Operations($"Failed to notify leader about removal of node {_serverStore.NodeTag} from database '{dbName}'", e);
                        }
                    }

                }, TaskContinuationOptions.NotOnRanToCompletion);
        }

        public TimeSpan DatabaseLoadTimeout => _serverStore.Configuration.Server.MaxTimeForTaskToWaitForDatabaseToLoad.AsTimeSpan;

        public void Dispose()
        {
            _disposing.CloseAndLock();
            var exceptionAggregator = new ExceptionAggregator(_logger, "Failure to dispose landlord");
            try
            {
                // prevent creating new databases
                _databaseSemaphore.Dispose();
            }
            catch (Exception e)
            {
                if (_logger.IsInfoEnabled)
                    _logger.Info("Failed to dispose resource semaphore", e);
            }

            // we don't want to wake up database during dispose.
            var handles = new List<WaitHandle>();
            foreach (var timer in _wakeupTimers.Values)
            {
                var handle = new ManualResetEvent(false);
                timer.Dispose(handle);
                handles.Add(handle);
            }

            if (handles.Count > 0)
            {
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
            }

            // shut down all databases in parallel, avoid having to wait for each one
            Parallel.ForEach(DatabasesCache.Values, new ParallelOptions
            {
                // we limit the number of resources we dispose concurrently to avoid
                // putting too much pressure on the I/O system if a disposing db need
                // to flush data to disk
                MaxDegreeOfParallelism = Math.Max(1, ProcessorInfo.ProcessorCount / 2)
            }, dbTask =>
            {
                if (dbTask.IsCompleted == false)
                    dbTask.ContinueWith(task =>
                    {
                        if (task.Status != TaskStatus.RanToCompletion)
                            return;

                        try
                        {
                            task.Result.Dispose();
                        }
                        catch (Exception e)
                        {
                            if (_logger.IsInfoEnabled)
                                _logger.Info("Failure in deferred disposal of a database", e);
                        }
                    });
                else if (dbTask.Status == TaskStatus.RanToCompletion && dbTask.Result != null)
                    exceptionAggregator.Execute(dbTask.Result.Dispose);
                // there is no else, the db is probably faulted
            });
            DatabasesCache.Clear();

            Parallel.ForEach(ShardedDatabasesCache.Values, new ParallelOptions
            {
                // we limit the number of resources we dispose concurrently to avoid
                // putting too much pressure on the I/O system if a disposing db need
                // to flush data to disk
                MaxDegreeOfParallelism = Math.Max(1, ProcessorInfo.ProcessorCount / 2)
            }, dbTask =>
            {
                // this is not really a task
                exceptionAggregator.Execute(dbTask.Result.Dispose);
            });
            exceptionAggregator.Execute(ShardedDatabasesCache.Clear);
            
            exceptionAggregator.Execute(_disposing.Dispose);

            exceptionAggregator.ThrowIfNeeded();
        }

        public event Action<string> OnDatabaseLoaded = delegate { };

        public bool IsDatabaseLoaded(StringSegment databaseName) => TryGetDatabaseIfLoaded(databaseName, out _);

        public bool TryGetDatabaseIfLoaded(StringSegment databaseName, out DocumentDatabase database)
        {
            database = default;
            if (DatabasesCache.TryGetValue(databaseName, out var task) == false)
                return false;

            if (task.IsCompletedSuccessfully == false)
                return false;

            database = task.Result;
            return task.Result.DatabaseShutdown.IsCancellationRequested == false;
        }

        public readonly struct DatabaseSearchResult(DatabaseSearchResult.Status databaseStatus, Task<DocumentDatabase> databaseTask, ShardedDatabaseContext databaseContext)
        {
            public readonly Task<DocumentDatabase> DatabaseTask = databaseTask;
            public readonly ShardedDatabaseContext DatabaseContext = databaseContext;
            public readonly Status DatabaseStatus = databaseStatus;

            public enum Status
            {
                None,
                Missing,
                Database,
                Sharded
            }
        }

        public DatabaseSearchResult TryGetOrCreateDatabase(StringSegment databaseName)
        {
            if (_disposing.TryEnter(out var idx) == false)
                ThrowServerIsBeingDisposed(databaseName);
            try
            {
                if (TryGetResourceStore(databaseName, out var databaseTask))
                {
                    return new DatabaseSearchResult(DatabaseSearchResult.Status.Database, databaseTask, databaseContext: null);
                }

                if (ShardedDatabasesCache.TryGetValue(databaseName, out var database))
                {
                    return new DatabaseSearchResult(DatabaseSearchResult.Status.Sharded, databaseTask: null, database.Result);
                }

                using (_serverStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
                using (context.OpenReadTransaction())
                {
                    var databaseRecord = _serverStore.Cluster.ReadRawDatabaseRecord(context, databaseName.Value);
                    if (databaseRecord == null)
                    {
                        return new DatabaseSearchResult(DatabaseSearchResult.Status.Missing, Task.FromResult<DocumentDatabase>(null), databaseContext: null);
                    }

                    if (databaseRecord.IsSharded)
                    {
                        var databaseContext = GetOrAddShardedDatabaseContext(databaseName, databaseRecord);

                        return new DatabaseSearchResult(DatabaseSearchResult.Status.Sharded, databaseTask: null, databaseContext);
                    }
                }

                return new DatabaseSearchResult(DatabaseSearchResult.Status.Database, TryGetOrCreateResourceStore(databaseName), databaseContext: null);
            }
            finally
            {
                _disposing.Exit(idx);
            }
        }

        private ShardedDatabaseContext GetOrAddShardedDatabaseContext(StringSegment databaseName, RawDatabaseRecord databaseRecord)
        {
            if (databaseRecord.IsDisabled)
                throw new DatabaseDisabledException(databaseName + " has been disabled");

            if (databaseRecord.Sharding.Orchestrator.Topology.RelevantFor(_serverStore.NodeTag) == false)
                throw new DatabaseNotRelevantException($"Can't get or add orchestrator for database {databaseName} because it is not relevant on this node {_serverStore.NodeTag}");

            var newTask = new Task<ShardedDatabaseContext>(() => new ShardedDatabaseContext(_serverStore, databaseRecord));
            var currentTask = ShardedDatabasesCache.GetOrAdd(databaseName, newTask);

            ShardedDatabaseContext databaseContext;
            try
            {
                if (newTask == currentTask)
                    currentTask.Start();

                databaseContext = currentTask.Result;
            }
            catch (Exception)
            {
                ShardedDatabasesCache.TryRemove(databaseName, newTask);

                throw;
            }

            return databaseContext;
        }

        public IEnumerable<Task<ShardedDocumentDatabase>> TryGetOrCreateShardedResourcesStore(StringSegment databaseName, DateTime? wakeup = null, bool ignoreDisabledDatabase = false, bool ignoreBeenDeleted = false, bool ignoreNotRelevant = false)
        {
            // create all database shards on this node
            List<string> relevantDatabases = new List<string>();
            using (_serverStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (context.OpenReadTransaction())
            using (var databaseRecord = _serverStore.Cluster.ReadRawDatabaseRecord(context, databaseName.Value))
            {
                if (databaseRecord == null)
                    yield break;

                foreach (var shard in databaseRecord.Topologies)
                {
                    if (shard.Topology.RelevantFor(_serverStore.NodeTag))
                        relevantDatabases.Add(shard.Name);
                }
            }

            foreach (var shardedDatabase in relevantDatabases)
            {
                yield return TryGetOrCreateShardedResourceStore(shardedDatabase, wakeup, ignoreDisabledDatabase, ignoreBeenDeleted, ignoreNotRelevant);
            }
        }

        public Task<ShardedDocumentDatabase> TryGetOrCreateShardedResourceStore(StringSegment databaseName, DateTime? wakeup = null, bool ignoreDisabledDatabase = false, bool ignoreBeenDeleted = false, bool ignoreNotRelevant = false, Action<string> addToInitLog = null, [CallerMemberName] string caller = null)
        {
            var t = TryGetOrCreateResourceStore(databaseName, wakeup, ignoreDisabledDatabase, ignoreBeenDeleted, ignoreNotRelevant, addToInitLog, caller);
            return t.ContinueWith(database => ShardedDocumentDatabase.CastToShardedDocumentDatabase(database.Result), TaskContinuationOptions.OnlyOnRanToCompletion);
        }

        public Task<DocumentDatabase> TryGetOrCreateResourceStore(StringSegment databaseName, DateTime? wakeup = null, bool ignoreDisabledDatabase = false, bool ignoreBeenDeleted = false, bool ignoreNotRelevant = false, Action<string> addToInitLog = null, [CallerMemberName] string caller = null)
        {
            if (_wakeupTimers.TryRemove(databaseName.Value, out var timer))
            {
                timer.Dispose();
            }
            if (_disposing.TryEnter(out var idx) == false)
                ThrowServerIsBeingDisposed(databaseName);
            try
            {
                if (TryGetResourceStore(databaseName, out var database))
                    return database;

                return CreateDatabase(databaseName, wakeup, ignoreDisabledDatabase, ignoreBeenDeleted, ignoreNotRelevant, caller);
            }
            finally
            {
                _disposing.Exit(idx);
            }
        }

        private bool TryGetResourceStore(StringSegment databaseName, out Task<DocumentDatabase> database)
        {
            if (DatabasesCache.TryGetValue(databaseName, out database))
            {
                if (database.IsFaulted)
                {
                    // If a database was unloaded, this is what we get from DatabasesCache.
                    // We want to keep the exception there until UnloadAndLockDatabase is disposed.
                    if (IsLockedDatabase(database.Exception))
                        return true;
                }

                if (database.IsFaulted || database.IsCanceled)
                {
                    DatabasesCache.TryRemove(databaseName, database);
                    LastRecentlyUsed.TryRemove(databaseName, out var _);
                    // and now we will try creating it again
                }
                else
                {
                    if (database.IsCompletedSuccessfully)
                        database.Result.LastAccessTime = database.Result.Time.GetUtcNow();

                    return true;
                }
            }

            return false;
        }

        public async Task RestartDatabaseAsync(string databaseName)
        {
            if (_disposing.TryEnter(out var idx) == false)
                ThrowServerIsBeingDisposed(databaseName);
            try
            {
                UnloadDatabaseInternal(databaseName);
            }
            finally
            {
                _disposing.Exit(idx);
            }

            var result = TryGetOrCreateDatabase(databaseName);
            switch (result.DatabaseStatus)
            {
                case DatabaseSearchResult.Status.Database:
                    await result.DatabaseTask;
                    break;
            }
        }

        internal static bool IsLockedDatabase(AggregateException exception)
        {
            if (exception == null)
                return false;
            var extractSingleInnerException = exception.ExtractSingleInnerException();
            return Equals(extractSingleInnerException.Data[DoNotRemove], true);
        }

        [DoesNotReturn]
        private static void ThrowServerIsBeingDisposed(StringSegment databaseName)
        {
            throw new ObjectDisposedException("The server is being disposed, cannot load database " + databaseName);
        }

        private Task<DocumentDatabase> CreateDatabase(StringSegment databaseName, DateTime? wakeup, bool ignoreDisabledDatabase, bool ignoreBeenDeleted, bool ignoreNotRelevant, string caller, Action<string> addToInitLog = null)
        {
            var config = CreateDatabaseConfiguration(databaseName, ignoreDisabledDatabase, ignoreBeenDeleted, ignoreNotRelevant);
            if (config == null)
                return Task.FromResult<DocumentDatabase>(null);

            if (!_databaseSemaphore.Wait(0))
                return UnlikelyCreateDatabaseUnderContention(databaseName, config, wakeup, caller);

            return CreateDatabaseUnderResourceSemaphore(databaseName, config, wakeup, addToInitLog, caller);
        }

        private async Task<DocumentDatabase> UnlikelyCreateDatabaseUnderContention(StringSegment databaseName, RavenConfiguration config, DateTime? wakeup = null, string caller = null)
        {
            var timeToWait = caller == Init ? Timeout.InfiniteTimeSpan : _concurrentDatabaseLoadTimeout;
            if (await _databaseSemaphore.WaitAsync(timeToWait) == false)
                throw new DatabaseConcurrentLoadTimeoutException("Too many databases loading concurrently, timed out waiting for them to load.");

            return await CreateDatabaseUnderResourceSemaphore(databaseName, config, wakeup);
        }

        private Task<DocumentDatabase> CreateDatabaseUnderResourceSemaphore(StringSegment databaseName, RavenConfiguration config, DateTime? wakeup = null, Action<string> addToInitLog = null, string caller = null)
        {
            try
            {
                var task = new Task<DocumentDatabase>(() => ActuallyCreateDatabase(databaseName, config, wakeup, addToInitLog), TaskCreationOptions.RunContinuationsAsynchronously);
                var database = DatabasesCache.GetOrAdd(databaseName, task);
                if (database == task)
                {
                    // This is in case when an deletion request was issued prior to the actual loading of the database.
                    task.ContinueWith(t =>
                    {
                        DeleteIfNeeded(databaseName);
                    });
                    task.Start(); // the semaphore will be released here at the end of the task
                    task.ContinueWith(t =>
                    {
                        ForTestingPurposes?.AfterDatabaseCreation?.Invoke((t.GetAwaiter().GetResult(), caller));

                        _serverStore.IdleDatabases.TryRemove(databaseName.Value, out _);
                    }, TaskContinuationOptions.OnlyOnRanToCompletion | TaskContinuationOptions.ExecuteSynchronously);
                }
                else
                    _databaseSemaphore.Release();

                return database;
            }
            catch (Exception)
            {
                _databaseSemaphore.Release();
                throw;
            }
        }

        private DocumentDatabase ActuallyCreateDatabase(StringSegment databaseName, RavenConfiguration config, DateTime? wakeup = null, Action<string> addToInitLog = null)
        {
            try
            {
                if (_serverStore.Disposed)
                    ThrowServerIsBeingDisposed(databaseName);

                if (_disposing.TryEnter(out var idx) == false)
                    ThrowServerIsBeingDisposed(databaseName);
                try
                {
                    var db = CreateDocumentsStorage(databaseName, config, wakeup, addToInitLog);
                    _serverStore.NotificationCenter.Add(
                        DatabaseChanged.Create(databaseName.Value, DatabaseChangeType.Load));

                    return db;
                }
                finally
                {
                    _disposing.Exit(idx);
                }
            }
            catch (Exception e)
            {
                if (_logger.IsInfoEnabled && e.InnerException is UnauthorizedAccessException)
                {
                    _logger.Info("Failed to load database because couldn't access certain file. Please check permissions, and make sure that nothing locks that file (an antivirus is a good example of something that can lock the file)", e);
                }

                throw;
            }
            finally
            {
                try
                {
                    _databaseSemaphore.Release();
                }
                catch (ObjectDisposedException)
                {
                    // nothing to do
                }
            }
        }

        public void DeleteIfNeeded(StringSegment databaseName, bool fromReplication = false)
        {
            try
            {
                if (_disposing.TryEnter(out var idx) == false)
                    ThrowServerIsBeingDisposed(databaseName);
                try
                {
                    if (_serverStore.ServerShutdown.IsCancellationRequested)
                        return;

                    using (_serverStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
                    using (context.OpenReadTransaction())
                    using (var rawRecord = _serverStore.Cluster.ReadRawDatabaseRecord(context, databaseName.Value))
                    {
                        if (rawRecord == null)
                            return;

                        ShouldDeleteDatabase(context, databaseName.Value, rawRecord, fromReplication);
                    }
                }
                finally
                {
                    _disposing.Exit(idx);
                }
            }
            catch
            {
                // nothing we can do here
            }
        }

        public ConcurrentDictionary<string, ConcurrentQueue<string>> InitLog =
            new ConcurrentDictionary<string, ConcurrentQueue<string>>(StringComparer.OrdinalIgnoreCase);

        public static DocumentDatabase CreateDocumentDatabase(string name, RavenConfiguration configuration, ServerStore serverStore, Action<string> addToInitLog)
        {
            return ShardHelper.IsShardName(name) ?
                new ShardedDocumentDatabase(name, configuration, serverStore, addToInitLog) :
                new DocumentDatabase(name, configuration, serverStore, addToInitLog);
        }

        private DocumentDatabase CreateDocumentsStorage(StringSegment databaseName, RavenConfiguration config, DateTime? wakeup, Action<string> addToInitLog)
        {
            void AddToInitLog(string txt)
            {
                addToInitLog?.Invoke(txt);
                string msg = txt;
                msg = $"[Load Database] {DateTime.UtcNow} :: Database '{databaseName}' : {msg}";
                if (InitLog.TryGetValue(databaseName.Value, out var q))
                    q.Enqueue(msg);
                if (_logger.IsInfoEnabled)
                    _logger.Info(msg);
            }

            DocumentDatabase documentDatabase = null;

            try
            {
                // force this to have a new value if one already exists
                InitLog.AddOrUpdate(databaseName.Value,
                    s => new ConcurrentQueue<string>(),
                    (s, existing) => new ConcurrentQueue<string>());

                AddToInitLog("Starting database initialization");

                var sp = Stopwatch.StartNew();

                documentDatabase = CreateDocumentDatabase(config.ResourceName, config, _serverStore, AddToInitLog);

                if (ForTestingPurposes?.HoldDocumentDatabaseCreation != null)
                    Thread.Sleep(ForTestingPurposes.HoldDocumentDatabaseCreation.Value);

                ForTestingPurposes?.OnBeforeDocumentDatabaseInitialization?.Invoke(documentDatabase);

                documentDatabase.Initialize(InitializeOptions.None, wakeup);

                ForTestingPurposes?.AfterDatabaseInitialize?.Invoke();

                AddToInitLog("Finish database initialization");
                DeleteDatabaseCachedInfo(documentDatabase.Name, throwOnError: false);
                if (_logger.IsInfoEnabled)
                    _logger.Info($"Started database {config.ResourceName} in {sp.ElapsedMilliseconds:#,#;;0}ms");

                OnDatabaseLoaded(config.ResourceName);

                // if we have a very long init process, make sure that we reset the last idle time for this db.
                LastRecentlyUsed.AddOrUpdate(databaseName, SystemTime.UtcNow, (_, time) => SystemTime.UtcNow);

                return documentDatabase;
            }
            catch (Exception e)
            {
                documentDatabase?.Dispose();

                if (_logger.IsInfoEnabled)
                    _logger.Info($"Failed to start database {config.ResourceName}", e);

                if (e is SchemaErrorException)
                {
                    throw new DatabaseSchemaErrorException($"Failed to start database {config.ResourceName}" + Environment.NewLine +
                                                           $"At {config.Core.DataDirectory}", e);
                }
                throw new DatabaseLoadFailureException($"Failed to start database {config.ResourceName}" + Environment.NewLine +
                                                       $"At {config.Core.DataDirectory}", e);
            }
            finally
            {
                InitLog.TryRemove(databaseName.Value, out var _);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void DeleteDatabaseNotifications(string databaseName, bool throwOnError)
        {
            try
            {
                _serverStore.NotificationCenter.Storage.DeleteStorageFor(databaseName);
            }
            catch (Exception e)
            {
                if (throwOnError)
                    throw;

                if (_logger.IsInfoEnabled)
                    _logger.Info($"Failed to delete database notifications for '{databaseName}' database.", e);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void DeleteDatabaseCachedInfo(string databaseName, bool throwOnError)
        {
            try
            {
                _serverStore.DatabaseInfoCache.Delete(databaseName);
            }
            catch (Exception e)
            {
                if (throwOnError)
                    throw;

                if (_logger.IsInfoEnabled)
                    _logger.Info($"Failed to delete database info for '{databaseName}' database.", e);
            }
        }

        public RavenConfiguration CreateDatabaseConfiguration(StringSegment databaseName, bool ignoreDisabledDatabase = false, bool ignoreBeenDeleted = false, bool ignoreNotRelevant = false)
        {
            if (databaseName.Trim().Length == 0)
                throw new ArgumentNullException(nameof(databaseName), "Database name cannot be empty");
            if (databaseName.Equals("<system>")) // This is here to guard against old ravendb tests
                throw new ArgumentNullException(nameof(databaseName),
                    "Database name cannot be <system>. Using of <system> database indicates outdated code that was targeted RavenDB 3.5.");

            Debug.Assert(_serverStore.Disposed == false);

            using (_serverStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (context.OpenReadTransaction())
            using (var databaseRecord = _serverStore.Cluster.ReadRawDatabaseRecord(context, databaseName.Value))
            {
                if (databaseRecord == null)
                    return null;

                var record = databaseRecord.MaterializedRecord;
                if (record.Encrypted && _serverStore.Server.AllowEncryptedDatabasesOverHttp == false)
                {
                    if (_serverStore.Server.WebUrl?.StartsWith("https:", StringComparison.OrdinalIgnoreCase) == false)
                    {
                        throw new DatabaseDisabledException(
                            $"The database {databaseName.Value} is encrypted, and must be accessed only via HTTPS, but the web url used is {_serverStore.Server.WebUrl}");
                    }
                }

                if (record.IsSharded)
                    throw new InvalidOperationException($"The database '{databaseName}' is sharded, can't call this method directly");

                return CreateDatabaseConfiguration(databaseRecord.DatabaseName, ignoreDisabledDatabase, ignoreBeenDeleted, ignoreNotRelevant, record);
            }
        }

        public RavenConfiguration CreateDatabaseConfiguration(StringSegment databaseName, bool ignoreDisabledDatabase, bool ignoreBeenDeleted, bool ignoreNotRelevant, DatabaseRecord databaseRecord)
        {
            if (databaseRecord.DatabaseState == DatabaseStateStatus.RestoreInProgress)
                throw new DatabaseRestoringException(databaseName + " is currently being restored");

            if (databaseRecord.Disabled && ignoreDisabledDatabase == false)
                throw new DatabaseDisabledException(databaseName + " has been disabled");

            var databaseIsBeenDeleted = databaseRecord.DeletionInProgress != null &&
                                        TryGetDeletionInProgress(databaseRecord.DeletionInProgress, databaseName.ToString(), _serverStore.NodeTag, out var deletionInProgress) &&
                                        deletionInProgress != DeletionInProgressStatus.No;
            if (ignoreBeenDeleted == false && databaseIsBeenDeleted)
                throw new DatabaseDisabledException(databaseName + " is currently being deleted on " + _serverStore.NodeTag);

            if (ignoreNotRelevant == false && databaseRecord.Topology.RelevantFor(_serverStore.NodeTag) == false &&
                databaseIsBeenDeleted == false)
                throw new DatabaseNotRelevantException(databaseName + " is not relevant for " + _serverStore.NodeTag);

            return CreateDatabaseConfiguration(_serverStore, databaseName.Value, databaseRecord.Settings);
        }

        private static bool TryGetDeletionInProgress(Dictionary<string, DeletionInProgressStatus> deletionInProgress, string databaseName, string nodeTag, out DeletionInProgressStatus status)
        {
            if (ShardHelper.TryGetShardNumberAndDatabaseName(databaseName, out _, out int shardNumber))
            {
                return deletionInProgress.TryGetValue(DatabaseRecord.GetKeyForDeletionInProgress(nodeTag, shardNumber), out status);
            }

            return deletionInProgress.TryGetValue(DatabaseRecord.GetKeyForDeletionInProgress(nodeTag, shardNumber: null), out status);
        }

        public static RavenConfiguration CreateDatabaseConfiguration(ServerStore serverStore, string databaseName, Dictionary<string, string> settings)
        {
            Debug.Assert(serverStore.Disposed == false, "_serverStore.Disposed == false");

            var config = RavenConfiguration.CreateForDatabase(serverStore.Configuration, databaseName);

            foreach (var setting in settings)
                config.SetSetting(setting.Key, setting.Value);

            config.Initialize();

            return config;
        }

        public DateTime LastWork(DocumentDatabase resource)
        {
            if (ForTestingPurposes is { ShouldFetchIdleStateImmediately: true })
                return resource.LastAccessTime;
            
            // This allows us to increase the time large databases will be held in memory
            // Using this method, we'll add 0.5 ms per KB, or roughly half a second of idle time per MB.

            var envs = resource.GetAllStoragesEnvironment();

            long dbSize = 0;
            var maxLastWork = resource.LastAccessTime;

            foreach (var env in envs)
            {
                dbSize += env.Environment.Stats().AllocatedDataFileSizeInBytes;

                if (env.Environment.LastWorkTime > maxLastWork)
                    maxLastWork = env.Environment.LastWorkTime;
            }

            return maxLastWork.AddMilliseconds(dbSize / 1024L);
        }

        public Task<IDisposable> UnloadAndLockDatabase(string dbName, string reason)
        {
            return UnloadAndLockDatabaseImpl(DatabasesCache, dbName, CompleteDatabaseUnloading, reason);
        }

        internal static async Task<IDisposable> UnloadAndLockDatabaseImpl<T>(ResourceCache<T> resourceCache, string dbName, Action<T> unloadDatabaseOnSuccess, string reason)
        {
            while (true)
            {
                try
                {
                    return resourceCache.RemoveLockAndReturn(dbName, unloadDatabaseOnSuccess, out _, caller: null, reason: reason);
                }
                catch (DatabaseConcurrentLoadTimeoutException)
                {
                    // database is still being loaded

                    await Task.Delay(100);
                }
                catch (AggregateException ea)
                {
                    var inner = ea.ExtractSingleInnerException();

                    if (inner is DatabaseDisabledException)
                        throw inner;

                    throw;
                }
            }
        }

        public enum ClusterDatabaseChangeType
        {
            RecordChanged,
            RecordRestored,
            ValueChanged,
            PendingClusterTransactions,
            ClusterTransactionCompleted
        }

        public bool UnloadDirectly(StringSegment databaseName, DateTime? wakeup = null, [CallerMemberName] string caller = null)
        {
            var nextScheduledAction = new IdleDatabaseActivity(IdleDatabaseActivityType.WakeUpDatabase);
            return UnloadDirectly(databaseName, nextScheduledAction, caller);
        }

        public bool UnloadDirectly(StringSegment databaseName, IdleDatabaseActivity idleDatabaseActivity, [CallerMemberName] string caller = null)
        {
            if (ShouldContinueDispose(databaseName.Value, idleDatabaseActivity) == false)
            {
                LogUnloadFailureReason(databaseName, $"{nameof(IdleDatabaseActivity.DueTime)} is {idleDatabaseActivity?.DueTime} ms which is less than {TimeSpan.FromMinutes(5).TotalMilliseconds} ms.");
                return false;
            }

            if (DatabasesCache.TryGetValue(databaseName, out var databaseTask) == false)
            {
                LogUnloadFailureReason(databaseName, "database was already unloaded or deleted.");
                return false;
            }

            if (databaseTask.IsCompleted == false)
            {
                LogUnloadFailureReason(databaseName, "database is loading.");
                return false;
            }

            if (databaseTask.IsCompletedSuccessfully == false)
            {
                LogUnloadFailureReason(databaseName, "database task is faulted or canceled.");
                return false;
            }

            try
            {
                UnloadDatabaseInternal(databaseName.Value, caller);
                LastRecentlyUsed.TryRemove(databaseName, out _);

                // DateTime should be only null in tests
                if (idleDatabaseActivity is { DateTime: not null })
                    _wakeupTimers.TryAdd(databaseName.Value, new Timer(
                        callback: _ => NextScheduledActivityCallback(databaseName.Value, idleDatabaseActivity),
                        state: null,
                        // in case the DueTime is negative or zero, the callback will be called immediately and database will be loaded.
                        dueTime: idleDatabaseActivity.DueTime > 0 ? idleDatabaseActivity.DueTime : 0,
                        period: Timeout.Infinite));

                if (_logger.IsOperationsEnabled)
                {
                    var msg = idleDatabaseActivity == null ? "without setting a wakeup timer." : $"wakeup timer set to: '{idleDatabaseActivity.DateTime.GetValueOrDefault()}', which will happen in '{idleDatabaseActivity.DueTime}' ms.";
                    _logger.Operations($"Unloading directly database '{databaseName}', {msg}");
                }

                return true;
            }
            catch (AggregateException ae) when (nameof(DeleteDatabase).Equals(ae.InnerException.Data["Source"]))
            {
                LogUnloadFailureReason(databaseName, "database is in the process of being deleted.");
                return false;
            }
            catch (AggregateException ae) when (ae.InnerException is DatabaseDisabledException)
            {
                LogUnloadFailureReason(databaseName, "database is already disabled when we try to unload it.");
                return false;
            }
            catch (DatabaseDisabledException)
            {
                LogUnloadFailureReason(databaseName, "database is already disabled when we try to unload it.");
                return false;
            }
            catch (ObjectDisposedException)
            {
                LogUnloadFailureReason(databaseName, "the server is disposed when we are trying to access to database.");
                return false;
            }
        }

        private void LogUnloadFailureReason(StringSegment databaseName, string reason)
        {
            if (_logger.IsOperationsEnabled)
                _logger.Operations($"Could not unload database '{databaseName}', reason: {reason}");
        }

        public void RescheduleNextIdleDatabaseActivity(string databaseName, IdleDatabaseActivity idleDatabaseActivity)
        {
            if (_wakeupTimers.TryGetValue(databaseName, out var oldTimer))
            {
                oldTimer.Dispose();
            }

            var newTimer = new Timer(_ => NextScheduledActivityCallback(databaseName, idleDatabaseActivity), null, idleDatabaseActivity.DueTime, Timeout.Infinite);
            _wakeupTimers.AddOrUpdate(databaseName, _ => newTimer, (_, __) => newTimer);
        }

        private void NextScheduledActivityCallback(string databaseName, IdleDatabaseActivity nextIdleDatabaseActivity)
        {
            try
            {
                if (_disposing.TryEnter(out var idx) == false)
                    ThrowServerIsBeingDisposed(databaseName);
                try
                {
                    if (_serverStore.ServerShutdown.IsCancellationRequested)
                        return;

                    switch (nextIdleDatabaseActivity.Type)
                    {
                        case IdleDatabaseActivityType.UpdateBackupStatusOnly:
                            PeriodicBackupStatus backupStatus;

                            using (_serverStore.Engine.ContextPool.AllocateOperationContext(out ClusterOperationContext context))
                            using (context.OpenReadTransaction())
                                backupStatus = BackupUtils.GetBackupStatusFromCluster(_serverStore, context, databaseName, nextIdleDatabaseActivity.TaskId);

                            backupStatus.LastIncrementalBackup = backupStatus.LastIncrementalBackupInternal = nextIdleDatabaseActivity.DateTime;
                            backupStatus.LocalBackup.LastIncrementalBackup = nextIdleDatabaseActivity.DateTime;
                            backupStatus.LocalBackup.IncrementalBackupDurationInMs = 0;

                            var backupResult = new BackupResult();
                            backupResult.AddMessage($"Skipping incremental backup because no changes were made from last full backup on {backupStatus.LastFullBackup}.");

                            BackupUtils.SaveBackupStatus(backupStatus, databaseName, _serverStore, _logger, backupResult);

                            nextIdleDatabaseActivity = BackupUtils.GetEarliestIdleDatabaseActivity(new BackupUtils.EarliestIdleDatabaseActivityParameters
                            {
                                DatabaseName = databaseName, LastEtag = nextIdleDatabaseActivity.LastEtag, Logger = _logger, ServerStore = _serverStore
                            });

                            RescheduleNextIdleDatabaseActivity(databaseName, nextIdleDatabaseActivity);
                            break;

                        case IdleDatabaseActivityType.WakeUpDatabase:
                            _ = TryGetOrCreateResourceStore(databaseName, nextIdleDatabaseActivity.DateTime).ContinueWith(t =>
                            {
                                var ex = t.Exception.ExtractSingleInnerException();
                                if (ex is DatabaseConcurrentLoadTimeoutException e)
                                {
                                    // database failed to load, retry after 1 min

                                    if (_logger.IsInfoEnabled)
                                        _logger.Info($"Failed to start database '{databaseName}' on timer, will retry the wakeup in '{_dueTimeOnRetry}' ms", e);

                                    nextIdleDatabaseActivity.DateTime = DateTime.UtcNow.AddMilliseconds(_dueTimeOnRetry);
                                    ForTestingPurposes?.RescheduleDatabaseWakeupMre?.Set();

                                    RescheduleNextIdleDatabaseActivity(databaseName, nextIdleDatabaseActivity);
                                }
                            }, TaskContinuationOptions.OnlyOnFaulted);
                            break;
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

        private bool ShouldContinueDispose(string name, IdleDatabaseActivity idleDatabaseActivity)
        {
            if (name == null)
                return true;

            if (_wakeupTimers.TryRemove(name, out var timer))
                timer.Dispose();

            if (idleDatabaseActivity == null)
                return true;

            if (idleDatabaseActivity.DateTime.HasValue == false)
                return true;

            if (SkipShouldContinueDisposeCheck)
                return true;

            // if we have a small value or even a negative one, simply don't dispose the database.
            return idleDatabaseActivity.DueTime > TimeSpan.FromMinutes(5).TotalMilliseconds;
        }

        private void CompleteDatabaseUnloading(DocumentDatabase database)
        {
            if (database == null)
                return;

            try
            {
                database.Dispose();
            }
            catch (Exception e)
            {
                if (_logger.IsInfoEnabled)
                    _logger.Info("Could not dispose database: " + database.Name, e);
            }

            database.DatabaseShutdownCompleted.Set();
        }

        private void CheckDatabasePathsIntersection(string databaseName, RavenConfiguration configuration)
        {
            using (_serverStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (context.OpenReadTransaction())
            {
                var allDatabasesRecords = _serverStore.Cluster.GetAllRawDatabases(context).SelectMany(r => r.AsShardsOrNormal());

                foreach (var currRecord in allDatabasesRecords)
                {
                    if (currRecord.DatabaseName.Equals(databaseName))
                        continue;

                    RavenConfiguration currentConfiguration;
                    try
                    {
                        currentConfiguration = CreateDatabaseConfiguration(currRecord.DatabaseName, ignoreDisabledDatabase: true,
                            ignoreBeenDeleted: true, ignoreNotRelevant: true, databaseRecord: currRecord);
                    }
                    catch (Exception e)
                    {
                        if (_logger.IsInfoEnabled)
                            _logger.Info("Could not create database configuration", e);
                        continue;
                    }

                    CheckConfigurationPaths(configuration, currentConfiguration, databaseName, currRecord.DatabaseName);
                }
            }
        }

        private void CheckConfigurationPaths(RavenConfiguration parentConfiguration, RavenConfiguration currentConfiguration, string databaseName, string currentDatabaseName)
        {
            if (PathUtil.IsSubDirectory(currentConfiguration.Core.DataDirectory.FullPath, parentConfiguration.Core.DataDirectory.FullPath))
            {
                throw new InvalidOperationException($"Cannot delete database {databaseName} from {parentConfiguration.Core.DataDirectory.FullPath}. " +
                                                    $"There is an intersection with database {currentDatabaseName} located in {currentConfiguration.Core.DataDirectory.FullPath}.");
            }
            if (currentConfiguration.Storage.TempPath != null && parentConfiguration.Storage.TempPath != null
                                                           && PathUtil.IsSubDirectory(currentConfiguration.Storage.TempPath.FullPath, parentConfiguration.Storage.TempPath.FullPath))
            {
                throw new InvalidOperationException($"Cannot delete database {databaseName} from {parentConfiguration.Storage.TempPath.FullPath}. " +
                                                    $"There is an intersection with database {currentDatabaseName} Temp file located in {currentConfiguration.Storage.TempPath.FullPath}.");
            }
            if (currentConfiguration.Indexing.StoragePath != null && parentConfiguration.Indexing.StoragePath != null
                                                               && PathUtil.IsSubDirectory(currentConfiguration.Indexing.StoragePath.FullPath, parentConfiguration.Indexing.StoragePath.FullPath))
            {
                throw new InvalidOperationException($"Cannot delete database {databaseName} from {parentConfiguration.Indexing.StoragePath.FullPath}. " +
                                                    $"There is an intersection with database {currentDatabaseName} located in {currentConfiguration.Indexing.StoragePath.FullPath}.");
            }
            if (currentConfiguration.Indexing.TempPath != null && parentConfiguration.Indexing.TempPath != null
                                                            && PathUtil.IsSubDirectory(currentConfiguration.Indexing.TempPath.FullPath, parentConfiguration.Indexing.TempPath.FullPath))
            {
                throw new InvalidOperationException($"Cannot delete database {databaseName} from {parentConfiguration.Indexing.TempPath.FullPath}. " +
                                                    $"There is an intersection with database {currentDatabaseName} Temp file located in {currentConfiguration.Indexing.TempPath.FullPath}.");
            }
        }

        public static async ValueTask NotifyFeaturesAboutStateChangeAsync(DatabaseRecord record, long index, StateChange state)
        {
            if (CanSkipDatabaseRecordChange())
                return;

            var taken = false;
            Stopwatch sp = default;

            while (taken == false)
            {
                taken = await state.Locker.WaitAsync(TimeSpan.FromSeconds(5), state.Token);

                try
                {
                    if (CanSkipDatabaseRecordChange())
                        return;

                    state.Token.ThrowIfCancellationRequested();
                    
                    if (taken == false)
                        continue;

                    sp = Stopwatch.StartNew();

                    Debug.Assert(string.Equals(state.Name, record.DatabaseName, StringComparison.OrdinalIgnoreCase),
                        $"{state.Name} != {record.DatabaseName}");

                    if (state.Logger.IsInfoEnabled)
                        state.Logger.Info($"Starting to process record {index} (current {state.LastIndexChange}) for {record.DatabaseName}.");

                    try
                    {
                        await state.OnChange(record, index);
                        state.LastIndexChange = index;

                        if (state.Logger.IsInfoEnabled)
                            state.Logger.Info($"Finish to process record {index} for {record.DatabaseName}.");
                    }
                    catch (Exception e)
                    {
                        if (state.Logger.IsInfoEnabled)
                            state.Logger.Info($"Encounter an error while processing record {index} for {record.DatabaseName}.", e);
                        throw;
                    }
                }
                finally
                {
                    if (taken)
                    {
                        state.Locker.Release();

                        sp?.Stop();

                        if (sp?.Elapsed > TimeSpan.FromSeconds(10) && state.Logger.IsOperationsEnabled)
                        {
                            try
                            {
                                using (state.ServerStore.Engine.ContextPool.AllocateOperationContext(out ClusterOperationContext ctx))
                                using (ctx.OpenReadTransaction())
                                {
                                    var logs = state.ServerStore.Engine.LogHistory.GetLogByIndex(ctx, index).Select(djv => ctx.ReadObject(djv, "djv").ToString());
                                    var msg =
                                        $"Lock held for a very long time {sp.Elapsed} in database {state.Name} for index {index} ({string.Join(", ", logs)})";
                                    state.Logger.Operations(msg);

#if !RELEASE
                                    Console.WriteLine(msg);
#endif
                                }
                            }
                            catch (OperationCanceledException)
                            {
                            }
                            catch (Exception e)
                            {
                                state.Logger.Operations($"Failed to log long held cluster lock: {sp.Elapsed} in database {state.Name}", e);
                            }
                        }
                    }
                }
            }

            bool CanSkipDatabaseRecordChange()
            {
                if (state.LastIndexChange > index)
                {
                    // index and LastDatabaseRecordIndex could have equal values when we transit from/to passive and want to update the tasks.
                    if (state.Logger.IsInfoEnabled)
                        state.Logger.Info($"Skipping record {index} (current {state.LastIndexChange}) for {record.DatabaseName} because it was already precessed.");
                    return true;
                }

                return false;
            }
        }

        public sealed class StateChange
        {
            public readonly SemaphoreSlim Locker;
            public readonly ServerStore ServerStore;
            public readonly string Name;
            public readonly CancellationToken Token;
            public readonly Logger Logger;
            public readonly Func<DatabaseRecord, long, Task> OnChange;

            public long LastIndexChange;

            public StateChange(ServerStore serverStore, string name, Logger logger, Func<DatabaseRecord, long, Task> onChange, long lastIndexChange, CancellationToken token)
            {
                ServerStore = serverStore;
                Name = name;
                Logger = logger;
                OnChange = onChange;
                LastIndexChange = lastIndexChange;
                Token = token;
                Locker = new SemaphoreSlim(1, 1);
            }
        }
    }

    public sealed class IdleDatabaseActivity
    {
        public long LastEtag { get; }
        public IdleDatabaseActivityType Type { get; }
        public DateTime? DateTime { get; internal set; }
        public long TaskId { get; }
        public int DueTime => DateTime.HasValue
            ? (int)Math.Min(int.MaxValue, (DateTime.Value - System.DateTime.UtcNow).TotalMilliseconds)
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
