using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Raven.Client;
using Raven.Client.Documents;
using Raven.Client.Exceptions.Database;
using Raven.Client.Extensions;
using Raven.Client.Json.Converters;
using Raven.Client.Server;
using Raven.Client.Util;
using Raven.Server.Config;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.NotificationCenter.Notifications.Details;
using Raven.Server.NotificationCenter.Notifications.Server;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Commands;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Raven.Server.Web.System;
using Sparrow;
using Sparrow.Collections;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Logging;
using Sparrow.Utils;

namespace Raven.Server.Documents
{
    public class DatabasesLandlord : IDisposable
    {
        private readonly ReaderWriterLockSlim _disposing = new ReaderWriterLockSlim(LockRecursionPolicy.SupportsRecursion);
        public readonly ConcurrentDictionary<StringSegment, DateTime> LastRecentlyUsed =
            new ConcurrentDictionary<StringSegment, DateTime>(CaseInsensitiveStringSegmentEqualityComparer.Instance);

        public readonly ResourceCache<DocumentDatabase> DatabasesCache = new ResourceCache<DocumentDatabase>();
        private readonly TimeSpan _concurrentResourceLoadTimeout;
        private readonly Logger _logger;
        private readonly SemaphoreSlim _resourceSemaphore;
        private readonly ServerStore _serverStore;

        public DatabasesLandlord(ServerStore serverStore)
        {
            _serverStore = serverStore;
            _resourceSemaphore = new SemaphoreSlim(_serverStore.Configuration.Databases.MaxConcurrentResourceLoads);
            _concurrentResourceLoadTimeout = _serverStore.Configuration.Databases.ConcurrentResourceLoadTimeout.AsTimeSpan;
            _logger = LoggingSource.Instance.GetLogger<DatabasesLandlord>("Raven/Server");
        }

        public void ClusterOnDatabaseChanged(object sender, (string dbName, long index, string type) t)
        {
            _disposing.EnterReadLock();
            try
            {
                if (_serverStore.Disposed)
                    return;

                // response to changed database.
                // if disabled, unload

                using (_serverStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
                {
                    context.OpenReadTransaction();
                    var record = _serverStore.Cluster.ReadDatabase(context, t.dbName);
                    if (record == null)
                    {
                        // was removed, need to make sure that it isn't loaded 
                        UnloadDatabase(t.dbName);
                        return;
                    }

                    if (record.DeletionInProgress != null &&
                        record.DeletionInProgress.TryGetValue(_serverStore.NodeTag, out DeletionInProgressStatus deletionInProgress) &&
                        deletionInProgress != DeletionInProgressStatus.No)
                    {
                        UnloadDatabase(t.dbName);

                        if (deletionInProgress == DeletionInProgressStatus.HardDelete)
                        {
                            RavenConfiguration configuration;
                            try
                            {
                                configuration = CreateDatabaseConfiguration(t.dbName, ignoreDisabledDatabase: true, ignoreBeenDeleted: true, databaseRecord: record);
                            }
                            catch (Exception ex)
                            {
                                configuration = null;
                                if (_logger.IsInfoEnabled)
                                    _logger.Info("Could not create database configuration", ex);
                            }
                            //this can happen if the database record was already deleted
                            if (configuration != null)
                            {
                                DatabaseHelper.DeleteDatabaseFiles(configuration);
                            }
                        }

                        NotifyLeaderAboutRemoval(t.dbName);

                        return;
                    }

                    if (record.Topology.RelevantFor(_serverStore.NodeTag) == false)
                        return;

                    if (record.Disabled)
                    {
                        UnloadDatabase(t.dbName);
                        return;
                    }

                    if (DatabasesCache.TryGetValue(t.dbName, out var task) == false)
                    {
                        // if the database isn't loaded, but it is relevant for this node, we need to create
                        // it. This is important so things like replication will start pumping, and that 
                        // configuration changes such as running periodic backup will get a chance to run, which
                        // they wouldn't unless the database is loaded / will have a request on it.          
                        task = TryGetOrCreateResourceStore(t.dbName);
                    }

                    if (task.IsCanceled || task.IsFaulted)
                        return;

                    if (task.IsCompleted)
                    {
                        NotifyDatabaseAboutStateChange(t.dbName, task, t.index);
                        return;
                    }
                    task.ContinueWith(done =>
                    {
                        NotifyDatabaseAboutStateChange(t.dbName, done, t.index);
                    });
                }

                // if deleted, unload / deleted and then notify leader that we removed it
            }
            catch (Exception e)
            {
                if (_logger.IsInfoEnabled)
                    _logger.Info("Could not react to a cluster database change", e);
            }
            finally
            {
                _disposing.ExitReadLock();
            }
        }

        private void NotifyLeaderAboutRemoval(string dbName)
        {
            var cmd = new RemoveNodeFromDatabaseCommand(dbName)
            {
                NodeTag = _serverStore.NodeTag
            };
            _serverStore.SendToLeaderAsync(cmd)
                .ContinueWith(t =>
                {
                    if (t.Exception != null)
                    {
                        if (_logger.IsInfoEnabled)
                        {
                            _logger.Info($"Failed to notify leader about removal of node {_serverStore.NodeTag} from database {dbName}", t.Exception);
                        }
                    }
                });
        }

        private void NotifyDatabaseAboutStateChange(string changedDatabase, Task<DocumentDatabase> done, long index)
        {
            try
            {
                done.Result.StateChanged(index);
            }
            catch (Exception e)
            {
                if (_logger.IsInfoEnabled)
                {
                    _logger.Info($"Failed to update database {changedDatabase} about new state", e);
                }
                // nothing to do here
            }
        }

        public TimeSpan DatabaseLoadTimeout => _serverStore.Configuration.Server.MaxTimeForTaskToWaitForDatabaseToLoad.AsTimeSpan;

        public void Dispose()
        {
            _disposing.EnterWriteLock();
            try
            {
                var exceptionAggregator = new ExceptionAggregator(_logger, "Failure to dispose landlord");

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
                    _resourceSemaphore.Dispose();
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
                _disposing.ExitWriteLock();
            }
        }

        public event Action<string> OnDatabaseLoaded = delegate { };

        public bool IsDatabaseLoaded(StringSegment databaseName)
        {
            if (DatabasesCache.TryGetValue(databaseName, out Task<DocumentDatabase> databaseTask))
            {
                return databaseTask.IsCanceled == false && databaseTask.IsFaulted == false;
            }

            return false;
        }

        public Task<DocumentDatabase> TryGetOrCreateResourceStore(StringSegment databaseName, bool ignoreDisabledDatabase = false)
        {
            try
            {
                if (_disposing.TryEnterReadLock(0) == false)
                    ThrowServerIsBeingDisposed(databaseName);

                if (DatabasesCache.TryGetValue(databaseName, out var database))
                {
                    if (database.IsFaulted || database.IsCanceled)
                    {
                        DatabasesCache.TryRemove(databaseName, out database);
                        LastRecentlyUsed.TryRemove(databaseName, out var _);
                        // and now we will try creating it again
                    }
                    else
                    {
                        return database;
                    }
                }
                return CreateDatabase(databaseName, ignoreDisabledDatabase);
            }
            finally
            {
                if (_disposing.IsReadLockHeld)
                    _disposing.ExitReadLock();
            }
        }

        private static void ThrowServerIsBeingDisposed(StringSegment databaseName)
        {
            throw new ObjectDisposedException("The server is being disposed, cannot load database " + databaseName);
        }

        private Task<DocumentDatabase> CreateDatabase(StringSegment databaseName, bool ignoreDisabledDatabase = false)
        {
            var config = CreateDatabaseConfiguration(databaseName, ignoreDisabledDatabase);
            if (config == null)
                return null;

            if (!_resourceSemaphore.Wait(0))
                return UnlikelyCreateDatabaseUnderContention(databaseName, config);

            return CreateDatabaseUnderResourceSemaphore(databaseName, config);
        }

        private async Task<DocumentDatabase> UnlikelyCreateDatabaseUnderContention(StringSegment databaseName, RavenConfiguration config)
        {
            if(await _resourceSemaphore.WaitAsync(_concurrentResourceLoadTimeout) == false)
                throw new DatabaseConcurrentLoadTimeoutException("Too many databases loading concurrently, timed out waiting for them to load.");

            return await CreateDatabaseUnderResourceSemaphore(databaseName, config);
        }

        private Task<DocumentDatabase> CreateDatabaseUnderResourceSemaphore(StringSegment databaseName, RavenConfiguration config)
        {
            try
            {
                var task = new Task<DocumentDatabase>(() => ActuallyCreateDatabase(databaseName, config), TaskCreationOptions.RunContinuationsAsynchronously);
                var database = DatabasesCache.GetOrAdd(databaseName, task);
                if (database == task)
                    task.Start(); // the semaphore will be released here at the end of the task
                else
                    _resourceSemaphore.Release();

                return database;
            }
            catch (Exception)
            {
                _resourceSemaphore.Release();
                throw;
            }
        }

        private DocumentDatabase ActuallyCreateDatabase(StringSegment databaseName, RavenConfiguration config)
        {
            try
            {
                if (_serverStore.Disposed)
                    return null;

                //if false, this means we have started disposing, so we shouldn't create a database now
                if (_disposing.TryEnterReadLock(0) == false)
                    return null;

                var db = CreateDocumentsStorage(databaseName, config);
                _serverStore.NotificationCenter.Add(
                    DatabaseChanged.Create(databaseName, DatabaseChangeType.Load));

                return db;
            }
            catch (Exception e)
            {
                // if we are here, there is an error, and if there is an error, we need to clear it from the 
                // resource store cache so we can try to reload it.
                // Note that we return the faulted task anyway, because we need the user to look at the error
                if (e.Data.Contains("Raven/KeepInResourceStore") == false)
                {
                    Task<DocumentDatabase> val;
                    DatabasesCache.TryRemove(databaseName, out val);
                }

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
                    _resourceSemaphore.Release();
                }
                catch (ObjectDisposedException)
                {
                }

                if(_disposing.IsReadLockHeld)
                    _disposing.ExitReadLock();
              
            }
        }

        private DocumentDatabase CreateDocumentsStorage(StringSegment databaseName, RavenConfiguration config)
        {
            try
            {
                var sp = Stopwatch.StartNew();
                var documentDatabase = new DocumentDatabase(config.ResourceName, config, _serverStore);
                documentDatabase.Initialize();
                DeleteDatabaseCachedInfo(documentDatabase, _serverStore);
                if (_logger.IsInfoEnabled)
                    _logger.Info($"Started database {config.ResourceName} in {sp.ElapsedMilliseconds:#,#;;0}ms");

                OnDatabaseLoaded(config.ResourceName);

                // if we have a very long init process, make sure that we reset the last idle time for this db.
                LastRecentlyUsed.AddOrUpdate(databaseName, SystemTime.UtcNow, (_, time) => SystemTime.UtcNow);
                return documentDatabase;
            }
            catch (Exception e)
            {
                if (_logger.IsInfoEnabled)
                    _logger.Info($"Failed to start database {config.ResourceName}", e);
                throw new DatabaseLoadFailureException($"Failed to start database {config.ResourceName}" + Environment.NewLine +
                                                       $"At {config.Core.DataDirectory}", e);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void DeleteDatabaseCachedInfo(DocumentDatabase database, ServerStore serverStore)
        {
            serverStore.DatabaseInfoCache.Delete(database.Name);
        }

        public RavenConfiguration CreateDatabaseConfiguration(StringSegment databaseName, bool ignoreDisabledDatabase = false, bool ignoreBeenDeleted = false)
        {
            if (databaseName.IsNullOrWhiteSpace())
                throw new ArgumentNullException(nameof(databaseName), "Database name cannot be empty");
            if (databaseName.Equals("<system>")) // This is here to guard against old ravendb tests
                throw new ArgumentNullException(nameof(databaseName),
                    "Database name cannot be <system>. Using of <system> database indicates outdated code that was targeted RavenDB 3.5.");

            Debug.Assert(_serverStore.Disposed == false);

            TransactionOperationContext context;
            using (_serverStore.ContextPool.AllocateOperationContext(out context))
            {
                context.OpenReadTransaction();

                var doc = _serverStore.Cluster.Read(context, "db/" + databaseName.Value.ToLowerInvariant());
                if (doc == null)
                    return null;

                var databaseRecord = JsonDeserializationCluster.DatabaseRecord(doc);
                return CreateDatabaseConfiguration(databaseName, ignoreDisabledDatabase, ignoreBeenDeleted, databaseRecord);

            }

        }

        public RavenConfiguration CreateDatabaseConfiguration(StringSegment databaseName, bool ignoreDisabledDatabase, bool ignoreBeenDeleted, DatabaseRecord databaseRecord)
        {
            if (databaseRecord.Disabled && ignoreDisabledDatabase == false)
                throw new DatabaseDisabledException(databaseName + " has been disabled");

            var databaseIsBeenDeleted = databaseRecord.DeletionInProgress != null &&
                            databaseRecord.DeletionInProgress.TryGetValue(_serverStore.NodeTag, out DeletionInProgressStatus deletionInProgress) &&
                            deletionInProgress != DeletionInProgressStatus.No;
            if (ignoreBeenDeleted == false && databaseIsBeenDeleted)
                throw new DatabaseDisabledException(databaseName + " is currently being deleted on " + _serverStore.NodeTag);

            if (databaseRecord.Topology.RelevantFor(_serverStore.NodeTag) == false &&
                databaseIsBeenDeleted == false)
                // TODO: need to handle this properly, need to redirect to somewhere it is on
                throw new InvalidOperationException(databaseName + " is not relevant for " + _serverStore.NodeTag);
            return CreateConfiguration(databaseName, databaseRecord);
        }

        protected RavenConfiguration CreateConfiguration(StringSegment databaseName, DatabaseRecord record)
        {
            Debug.Assert(_serverStore.Disposed == false);
            var config = RavenConfiguration.CreateFrom(_serverStore.Configuration, databaseName, ResourceType.Database);

            foreach (var setting in record.Settings)
                config.SetSetting(setting.Key, setting.Value);

            config.Initialize();

            config.CopyParentSettings(_serverStore.Configuration);


            return config;
        }

        public DateTime LastWork(DocumentDatabase resource)
        {
            // This allows us to increase the time large databases will be held in memory
            // Using this method, we'll add 0.5 ms per KB, or roughly half a second of idle time per MB.

            var envs = resource.GetAllStoragesEnvironment();

            long dbSize = 0;
            var maxLastWork = DateTime.MinValue;

            foreach (var env in envs)
            {
                dbSize += env.Environment.Stats().AllocatedDataFileSizeInBytes;

                if (env.Environment.LastWorkTime > maxLastWork)
                    maxLastWork = env.Environment.LastWorkTime;
            }

            return maxLastWork.AddMilliseconds(dbSize / 1024L);
        }

        public void UnloadResourceOnCatastrophicFailue(string databaseName, Exception e)
        {
            Task.Run(async () =>
            {
                var title = $"Critical error in '{databaseName}'";
                var message = "Database is about to be unloaded due to an encountered error";

                try
                {
                    _serverStore.NotificationCenter.Add(AlertRaised.Create(
                        title,
                        message,
                        AlertType.CatastrophicDatabaseFailure,
                        NotificationSeverity.Error,
                        key: databaseName,
                        details: new ExceptionDetails(e)));
                }
                catch (Exception)
                {
                    // exception in raising an alert can't prevent us from unloading a database
                }

                if (_logger.IsOperationsEnabled)
                    _logger.Operations($"{title}. {message}", e);

                await Task.Delay(2000); // let it propagate the exception to the client first

                UnloadDatabase(databaseName, null);
            });
        }

        public void UnloadDatabase(string dbName, TimeSpan? skipIfActiveInDuration = null, Func<DocumentDatabase, bool> shouldSkip = null)
        {
            if (DatabasesCache.TryGetValue(dbName, out Task<DocumentDatabase> dbTask) == false)
            {
                LastRecentlyUsed.TryRemove(dbName, out _);
                return;
            }
            var dbTaskStatus = dbTask.Status;
            if (dbTaskStatus == TaskStatus.Faulted || dbTaskStatus == TaskStatus.Canceled)
            {
                LastRecentlyUsed.TryRemove(dbName, out _);
                DatabasesCache.TryRemove(dbName, out dbTask);
                return;
            }
            if (dbTaskStatus != TaskStatus.RanToCompletion)
                throw new InvalidOperationException($"Couldn't modify '{dbName}' while it is loading, current status {dbTaskStatus}");

            // will never wait, we checked that we already run to completion here
            var database = dbTask.Result;

            if (skipIfActiveInDuration != null && SystemTime.UtcNow - LastWork(database) < skipIfActiveInDuration ||
                shouldSkip != null && shouldSkip(database))
                return;

            try
            {
                database.Dispose();
            }
            catch (Exception e)
            {
                if (_logger.IsInfoEnabled)
                    _logger.Info("Could not dispose database: " + dbName, e);
            }

            LastRecentlyUsed.TryRemove(dbName, out _);
            DatabasesCache.TryRemove(dbName, out dbTask);

            database.DatabaseShutdownCompleted.Set();
        }
    }
}