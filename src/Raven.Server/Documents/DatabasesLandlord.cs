using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Nito.AsyncEx;
using Raven.Client.Exceptions;
using Raven.Client.Exceptions.Database;
using Raven.Client.Extensions;
using Raven.Client.ServerWide;
using Raven.Client.Util;
using Raven.Server.Config;
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
using Sparrow.Utils;
using Voron.Exceptions;
using Voron.Util.Settings;

namespace Raven.Server.Documents
{
    public class DatabasesLandlord : IDisposable
    {
        public const string DoNotRemove = "DoNotRemove";
        private readonly AsyncReaderWriterLock _disposing = new AsyncReaderWriterLock();

        public readonly ConcurrentDictionary<StringSegment, DateTime> LastRecentlyUsed =
            new ConcurrentDictionary<StringSegment, DateTime>(StringSegmentComparer.OrdinalIgnoreCase);

        private readonly ConcurrentDictionary<string, Timer> _wakeupTimers = new ConcurrentDictionary<string, Timer>();

        public readonly ResourceCache<DocumentDatabase> DatabasesCache = new ResourceCache<DocumentDatabase>();
        private readonly TimeSpan _concurrentDatabaseLoadTimeout;
        private readonly Logger _logger;
        private readonly SemaphoreSlim _databaseSemaphore;
        private readonly ServerStore _serverStore;

        // used in ServerWideBackupStress
        internal bool SkipShouldContinueDisposeCheck = false;

        public DatabasesLandlord(ServerStore serverStore)
        {
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

        internal class TestingStuff
        {
            internal Action<ServerStore> BeforeHandleClusterDatabaseChanged;
            internal int? HoldDocumentDatabaseCreation = null;
            internal bool PreventedRehabOfIdleDatabase = false;
        }

        private async Task HandleClusterDatabaseChanged(string databaseName, long index, string type, ClusterDatabaseChangeType changeType, object _)
        {
            ForTestingPurposes?.BeforeHandleClusterDatabaseChanged?.Invoke(_serverStore);

            if (PreventWakeUpIdleDatabase(databaseName, type))
                return;

            using (await _disposing.ReaderLockAsync(_serverStore.ServerShutdown))
            {
                try
                {
                    if (_serverStore.ServerShutdown.IsCancellationRequested)
                        return;

                    // response to changed database.
                    // if disabled, unload
                    DatabaseTopology topology;
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


                        if (ShouldDeleteDatabase(context, databaseName, rawRecord))
                            return;

                        topology = rawRecord.Topology;
                        if (topology.RelevantFor(_serverStore.NodeTag) == false)
                            return;

                        if (rawRecord.IsDisabled || rawRecord.DatabaseState == DatabaseStateStatus.RestoreInProgress)
                        {
                            UnloadDatabase(databaseName);
                            return;
                        }
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
                        task = TryGetOrCreateResourceStore(databaseName, ignoreBeenDeleted: true);
                    }

                    var database = await task;

                    switch (changeType)
                    {
                        case ClusterDatabaseChangeType.RecordChanged:
                            database.StateChanged(index);
                            if (type == ClusterStateMachine.SnapshotInstalled)
                            {
                                database.NotifyOnPendingClusterTransaction(index, changeType);
                            }
                            break;

                        case ClusterDatabaseChangeType.ValueChanged:
                            database.ValueChanged(index);
                            break;

                        case ClusterDatabaseChangeType.PendingClusterTransactions:
                        case ClusterDatabaseChangeType.ClusterTransactionCompleted:
                            database.DatabaseGroupId = topology.DatabaseTopologyIdBase64;
                            database.NotifyOnPendingClusterTransaction(index, changeType);
                            break;

                        default:
                            ThrowUnknownClusterDatabaseChangeType(changeType);
                            break;
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
                    // the server is disposed when we are trying to access to database
                }
                catch (DatabaseConcurrentLoadTimeoutException e)
                {
                    var title = $"Concurrent load timeout of '{databaseName}' database";

                    var message = $"Failed to load database '{databaseName}' concurrently with other databases within {_serverStore.Configuration.Databases.ConcurrentLoadTimeout.AsTimeSpan}. " +
                                "Database load will be attempted on next request accessing it. If you see this on regular basis you might consider adjusting the following configuration options: " +
                                $"{RavenConfiguration.GetKey(x => x.Databases.ConcurrentLoadTimeout)} and {RavenConfiguration.GetKey(x => x.Databases.MaxConcurrentLoads)}";

                    if (_logger.IsInfoEnabled)
                        _logger.Info(message, e);

                    _serverStore.NotificationCenter.Add(AlertRaised.Create(databaseName, title, message, AlertType.ConcurrentDatabaseLoadTimeout, NotificationSeverity.Warning,
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

        private void UnloadDatabaseInternal(string databaseName, string caller = null)
        {
            DatabasesCache.RemoveLockAndReturn(databaseName, CompleteDatabaseUnloading, out _, caller).Dispose();
        }

        public bool ShouldDeleteDatabase(TransactionOperationContext context, string dbName, RawDatabaseRecord rawRecord)
        {
            var deletionInProgress = DeletionInProgressStatus.No;
            var directDelete = rawRecord.DeletionInProgress?.TryGetValue(_serverStore.NodeTag, out deletionInProgress) == true &&
                               deletionInProgress != DeletionInProgressStatus.No;

            if (directDelete == false)
                return false;

            if (rawRecord.Topology.Rehabs.Contains(_serverStore.NodeTag))
                // If the deletion was issued form the cluster observer to maintain the replication factor we need to make sure
                // that all the documents were replicated from this node, therefor the deletion will be called from the replication code.
                return false;

            var record = rawRecord.MaterializedRecord;
            context.CloseTransaction();

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
                catch (AggregateException ae) when (nameof(DeleteDatabase).Equals(ae.InnerException.Data["Source"]))
                {
                    // this is already in the process of being deleted, we can just exit and let another thread handle it
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

                    CheckDatabasePathsIntersection(dbName, configuration);

                    // this can happen if the database record was already deleted
                    if (configuration != null)
                    {
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

                // delete the cache info
                DeleteDatabaseCachedInfo(dbName, _serverStore);
            }
            finally
            {
                removeLockAndReturn?.Dispose();
            }
            NotifyLeaderAboutRemoval(dbName, databaseId);
        }

        private static void ThrowUnknownClusterDatabaseChangeType(ClusterDatabaseChangeType type)
        {
            throw new InvalidOperationException($"Unknown cluster database change type: {type}");
        }

        private void NotifyLeaderAboutRemoval(string dbName, string databaseId)
        {
            var cmd = new RemoveNodeFromDatabaseCommand(dbName, databaseId, RaftIdGenerator.NewId())
            {
                NodeTag = _serverStore.NodeTag
            };
            _serverStore.SendToLeaderAsync(cmd)
                .ContinueWith(async t =>
                {
                    var message = $"Failed to notify leader about removal of node {_serverStore.NodeTag} from database '{dbName}', will retry again in 15 seconds.";
                    if (t.IsFaulted)
                    {
                        var ex = t.Exception.ExtractSingleInnerException();
                        if (ex is DatabaseDoesNotExistException)
                            return;

                        if (_logger.IsInfoEnabled)
                        {
                            _logger.Info(message, t.Exception);
                        }
                    }

                    if (t.IsCanceled)
                    {
                        if (_logger.IsInfoEnabled)
                        {
                            _logger.Info(message, t.Exception);
                        }
                    }

                    await Task.Delay(TimeSpan.FromSeconds(15));

                    NotifyLeaderAboutRemoval(dbName, databaseId);
                }, TaskContinuationOptions.NotOnRanToCompletion);
        }

        public TimeSpan DatabaseLoadTimeout => _serverStore.Configuration.Server.MaxTimeForTaskToWaitForDatabaseToLoad.AsTimeSpan;

        public void Dispose()
        {
            var release = _disposing.WriterLock();
            try
            {
                var exceptionAggregator = new ExceptionAggregator(_logger, "Failure to dispose landlord");

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

                try
                {
                    _databaseSemaphore.Dispose();
                }
                catch (Exception e)
                {
                    if (_logger.IsInfoEnabled)
                        _logger.Info("Failed to dispose resource semaphore", e);
                }

                exceptionAggregator.ThrowIfNeeded();
            }
            finally
            {
                release.Dispose();
            }
        }

        public event Action<string> OnDatabaseLoaded = delegate { };

        public bool IsDatabaseLoaded(StringSegment databaseName)
        {
            if (DatabasesCache.TryGetValue(databaseName, out var task))
                return task != null && task.IsCompletedSuccessfully;

            return false;
        }

        public Task<DocumentDatabase> TryGetOrCreateResourceStore(StringSegment databaseName, DateTime? wakeup = null, bool ignoreDisabledDatabase = false, bool ignoreBeenDeleted = false, bool ignoreNotRelevant = false)
        {
            IDisposable release = null;
            try
            {
                if (_wakeupTimers.TryRemove(databaseName.Value, out var timer))
                {
                    timer.Dispose();
                }
                release = EnterReadLockImmediately(databaseName);

                if (DatabasesCache.TryGetValue(databaseName, out var database))
                {
                    if (database.IsFaulted)
                    {
                        // If a database was unloaded, this is what we get from DatabasesCache.
                        // We want to keep the exception there until UnloadAndLockDatabase is disposed.
                        var extractSingleInnerException = database.Exception.ExtractSingleInnerException();
                        if (Equals(extractSingleInnerException.Data[DoNotRemove], true))
                            return database;
                    }

                    if (database.IsFaulted || database.IsCanceled)
                    {
                        DatabasesCache.TryRemove(databaseName, out database);
                        LastRecentlyUsed.TryRemove(databaseName, out var _);
                        // and now we will try creating it again
                    }
                    else
                    {
                        if (database.IsCompletedSuccessfully)
                            database.Result.LastAccessTime = database.Result.Time.GetUtcNow();

                        return database;
                    }
                }
                return CreateDatabase(databaseName, wakeup, ignoreDisabledDatabase, ignoreBeenDeleted, ignoreNotRelevant);
            }
            finally
            {
                release?.Dispose();
            }
        }

        private IDisposable EnterReadLockImmediately(StringSegment databaseName)
        {
            using (var cts = new CancellationTokenSource())
            {
                var awaiter = _disposing.ReaderLockAsync(cts.Token).GetAwaiter();
                if (awaiter.IsCompleted == false)
                {
                    cts.Cancel();
                    try
                    {
                        ThrowServerIsBeingDisposed(databaseName);
                    }
                    finally
                    {
                        try
                        {
                            awaiter.GetResult()?.Dispose();
                        }
                        catch
                        {
                            // nothing to do here
                        }
                    }
                }

                return awaiter.GetResult();
            }
        }

        private static void ThrowServerIsBeingDisposed(StringSegment databaseName)
        {
            throw new ObjectDisposedException("The server is being disposed, cannot load database " + databaseName);
        }

        private Task<DocumentDatabase> CreateDatabase(StringSegment databaseName, DateTime? wakeup, bool ignoreDisabledDatabase, bool ignoreBeenDeleted, bool ignoreNotRelevant)
        {
            var config = CreateDatabaseConfiguration(databaseName, ignoreDisabledDatabase, ignoreBeenDeleted, ignoreNotRelevant);
            if (config == null)
                return Task.FromResult<DocumentDatabase>(null);

            if (!_databaseSemaphore.Wait(0))
                return UnlikelyCreateDatabaseUnderContention(databaseName, config, wakeup);

            return CreateDatabaseUnderResourceSemaphore(databaseName, config, wakeup);
        }

        private async Task<DocumentDatabase> UnlikelyCreateDatabaseUnderContention(StringSegment databaseName, RavenConfiguration config, DateTime? wakeup = null)
        {
            if (await _databaseSemaphore.WaitAsync(_concurrentDatabaseLoadTimeout) == false)
                throw new DatabaseConcurrentLoadTimeoutException("Too many databases loading concurrently, timed out waiting for them to load.");

            return await CreateDatabaseUnderResourceSemaphore(databaseName, config, wakeup);
        }

        private Task<DocumentDatabase> CreateDatabaseUnderResourceSemaphore(StringSegment databaseName, RavenConfiguration config, DateTime? wakeup = null)
        {
            try
            {
                var task = new Task<DocumentDatabase>(() => ActuallyCreateDatabase(databaseName, config, wakeup), TaskCreationOptions.RunContinuationsAsynchronously);
                var database = DatabasesCache.GetOrAdd(databaseName, task);
                if (database == task)
                {
                    DeleteIfNeeded(databaseName, task);
                    task.Start(); // the semaphore will be released here at the end of the task
                    task.ContinueWith(__ => _serverStore.IdleDatabases.TryRemove(databaseName.Value, out _), TaskContinuationOptions.OnlyOnRanToCompletion | TaskContinuationOptions.ExecuteSynchronously);
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

        private DocumentDatabase ActuallyCreateDatabase(StringSegment databaseName, RavenConfiguration config, DateTime? wakeup = null)
        {
            IDisposable release = null;
            try
            {
                if (_serverStore.Disposed)
                    ThrowServerIsBeingDisposed(databaseName);

                //if false, this means we have started disposing, so we shouldn't create a database now
                release = EnterReadLockImmediately(databaseName);

                var db = CreateDocumentsStorage(databaseName, config, wakeup);
                _serverStore.NotificationCenter.Add(
                    DatabaseChanged.Create(databaseName.Value, DatabaseChangeType.Load));

                return db;
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

                release?.Dispose();
            }
        }

        private void DeleteIfNeeded(StringSegment databaseName, Task<DocumentDatabase> database)
        {
            database.ContinueWith(t =>
            {
                // This is in case when an deletion request was issued prior to the actual loading of the database.
                try
                {
                    using (_disposing.ReaderLock(_serverStore.ServerShutdown))
                    {
                        if (_serverStore.ServerShutdown.IsCancellationRequested)
                            return;

                        using (_serverStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
                        using (context.OpenReadTransaction())
                        using (var rawRecord = _serverStore.Cluster.ReadRawDatabaseRecord(context, databaseName.Value))
                        {
                            if (rawRecord == null)
                                return;

                            ShouldDeleteDatabase(context, databaseName.Value, rawRecord);
                        }
                    }
                }
                catch
                {
                    // nothing we can do here
                }
            });
        }

        public ConcurrentDictionary<string, ConcurrentQueue<string>> InitLog =
            new ConcurrentDictionary<string, ConcurrentQueue<string>>(StringComparer.OrdinalIgnoreCase);

        private DocumentDatabase CreateDocumentsStorage(StringSegment databaseName, RavenConfiguration config, DateTime? wakeup = null)
        {
            void AddToInitLog(string txt)
            {
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
                documentDatabase = new DocumentDatabase(config.ResourceName, config, _serverStore, AddToInitLog);

                if (ForTestingPurposes?.HoldDocumentDatabaseCreation != null)
                    Thread.Sleep(ForTestingPurposes.HoldDocumentDatabaseCreation.Value);

                documentDatabase.Initialize(InitializeOptions.None, wakeup);

                AddToInitLog("Finish database initialization");
                DeleteDatabaseCachedInfo(documentDatabase.Name, _serverStore);
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
        private static void DeleteDatabaseCachedInfo(string databaseName, ServerStore serverStore)
        {
            serverStore.DatabaseInfoCache.Delete(databaseName);
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
                if (record.Encrypted)
                {
                    if (_serverStore.Server.WebUrl?.StartsWith("https:", StringComparison.OrdinalIgnoreCase) == false)
                    {
                        throw new DatabaseDisabledException(
                            $"The database {databaseName.Value} is encrypted, and must be accessed only via HTTPS, but the web url used is {_serverStore.Server.WebUrl}");
                    }
                }

                return CreateDatabaseConfiguration(databaseName, ignoreDisabledDatabase, ignoreBeenDeleted, ignoreNotRelevant, record);
            }
        }

        public RavenConfiguration CreateDatabaseConfiguration(StringSegment databaseName, bool ignoreDisabledDatabase, bool ignoreBeenDeleted, bool ignoreNotRelevant, DatabaseRecord databaseRecord)
        {
            if (databaseRecord.DatabaseState == DatabaseStateStatus.RestoreInProgress)
                throw new DatabaseRestoringException(databaseName + " is currently being restored");

            if (databaseRecord.Disabled && ignoreDisabledDatabase == false)
                throw new DatabaseDisabledException(databaseName + " has been disabled");

            var databaseIsBeenDeleted = databaseRecord.DeletionInProgress != null &&
                            databaseRecord.DeletionInProgress.TryGetValue(_serverStore.NodeTag, out DeletionInProgressStatus deletionInProgress) &&
                            deletionInProgress != DeletionInProgressStatus.No;
            if (ignoreBeenDeleted == false && databaseIsBeenDeleted)
                throw new DatabaseDisabledException(databaseName + " is currently being deleted on " + _serverStore.NodeTag);

            if (ignoreNotRelevant == false && databaseRecord.Topology.RelevantFor(_serverStore.NodeTag) == false &&
                databaseIsBeenDeleted == false)
                throw new DatabaseNotRelevantException(databaseName + " is not relevant for " + _serverStore.NodeTag);
            return CreateConfiguration(databaseRecord);
        }

        protected RavenConfiguration CreateConfiguration(DatabaseRecord record)
        {
            Debug.Assert(_serverStore.Disposed == false, "_serverStore.Disposed == false");
            var config = RavenConfiguration.CreateForDatabase(_serverStore.Configuration, record.DatabaseName);

            foreach (var setting in record.Settings)
                config.SetSetting(setting.Key, setting.Value);

            config.Initialize();

            return config;
        }

        public DateTime LastWork(DocumentDatabase resource)
        {
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

        public async Task<IDisposable> UnloadAndLockDatabase(string dbName, string reason)
        {
            var tcs = Task.FromException<DocumentDatabase>(new DatabaseDisabledException($"The database {dbName} is currently locked because {reason}")
            {
                Data =
                {
                    [DoNotRemove] = true
                }
            });

            var t = tcs.IgnoreUnobservedExceptions();

            try
            {
                var existing = DatabasesCache.Replace(dbName, tcs);
                if (existing != null)
                    (await existing)?.Dispose();

                return new DisposableAction(() =>
                {
                    DatabasesCache.TryRemove(dbName, out var _);
                });
            }
            catch (Exception)
            {
                DatabasesCache.TryRemove(dbName, out var _);
                throw;
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
            if (ShouldContinueDispose(databaseName.Value, wakeup, out var dueTime) == false)
            {
                LogUnloadFailureReason(databaseName, $"{nameof(dueTime)} is {dueTime} ms which is less than {TimeSpan.FromMinutes(5).TotalMilliseconds} ms.");
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

                if (dueTime > 0)
                    _wakeupTimers.TryAdd(databaseName.Value, new Timer(_ => StartDatabaseOnTimer(databaseName.Value, wakeup), null, dueTime, Timeout.Infinite));

                if (_logger.IsOperationsEnabled)
                {
                    var msg = dueTime > 0 ? $"wakeup timer set to: {wakeup}, which will happen in {dueTime} ms." : "without setting a wakeup timer.";
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

        public void RescheduleDatabaseWakeup(string name, long milliseconds, DateTime? wakeup = null)
        {
            if (_wakeupTimers.TryGetValue(name, out var oldTimer))
            {
                oldTimer.Dispose();
            }

            var newTimer = new Timer(_ => StartDatabaseOnTimer(name, wakeup), null, milliseconds, Timeout.Infinite);
            _wakeupTimers.AddOrUpdate(name, _ => newTimer, (_, __) => newTimer);
        }

        private void StartDatabaseOnTimer(string name, DateTime? wakeup = null)
        {
            try
            {
                using (_disposing.ReaderLock(_serverStore.ServerShutdown))
                {
                    if (_serverStore.ServerShutdown.IsCancellationRequested)
                        return;

                    TryGetOrCreateResourceStore(name, wakeup);
                }
            }
            catch
            {
                // we have to swallow any exception here.
            }
        }

        private bool ShouldContinueDispose(string name, DateTime? wakeupUtc, out int dueTime)
        {
            dueTime = 0;

            if (name == null)
                return true;

            if (_wakeupTimers.TryRemove(name, out var timer))
            {
                timer.Dispose();
            }

            if (wakeupUtc.HasValue == false)
                return true;

            // if we have a small value or even a negative one, simply don't dispose the database.
            dueTime = (int)(wakeupUtc - DateTime.UtcNow).Value.TotalMilliseconds;

            if (SkipShouldContinueDisposeCheck)
                return true;

            return dueTime > TimeSpan.FromMinutes(5).TotalMilliseconds;
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
                var allDatabasesRecords = _serverStore.Cluster.GetAllDatabases(context);
                var parentDir = configuration.Core.DataDirectory.FullPath;

                foreach (var currRecord in allDatabasesRecords)
                {
                    if (currRecord.DatabaseName.Equals(databaseName))
                        continue;

                    RavenConfiguration currConfiguration;
                    try
                    {
                        currConfiguration = CreateDatabaseConfiguration(currRecord.DatabaseName, ignoreDisabledDatabase: true, 
                            ignoreBeenDeleted: true, ignoreNotRelevant: true, databaseRecord: currRecord);
                    }
                    catch (Exception e)
                    {
                        currConfiguration = null;
                        if (_logger.IsInfoEnabled)
                            _logger.Info("Could not create database configuration", e);
                    }

                    var currDir = currConfiguration?.Core.DataDirectory.FullPath;
                    if (PathUtil.IsSubDirectory(currDir, parentDir))
                    {
                        throw new InvalidOperationException($"Cannot delete database {databaseName} from {parentDir}. " +
                                                            $"There is an intersection with database {currRecord.DatabaseName} located in {currDir}.");
                    }
                }
            }
        }
    }
}
