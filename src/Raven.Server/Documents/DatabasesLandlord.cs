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
using Raven.Client.Exceptions.Database;
using Raven.Client.Json.Converters;
using Raven.Client.Server;
using Raven.Client.Util;
using Raven.Server.Config;
using Raven.Server.NotificationCenter.Notifications.Server;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow;
using Sparrow.Collections;
using Sparrow.Logging;

namespace Raven.Server.Documents
{
    public class DatabasesLandlord : IDisposable
    {
        private ReaderWriterLockSlim _disposing = new ReaderWriterLockSlim();
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
            _serverStore.Cluster.DatabaseChanged += ClusterOnDatabaseChanged;
        }

        private void ClusterOnDatabaseChanged(object sender, string changedDatabase)
        {
            // response to changed database.
            // if disabled, unload

            TransactionOperationContext context;
            using (_serverStore.ContextPool.AllocateOperationContext(out context))
            {
                context.OpenReadTransaction();
                var doc = _serverStore.Cluster.Read(context, "db/" + changedDatabase.ToLowerInvariant());
                if (doc == null)
                {
                    // was removed, need to make sure that it isn't loaded 
                    UnloadDatabase(changedDatabase, null);
                    return;
                }

                var record = JsonDeserializationCluster.DatabaseRecord(doc);
                if (record.Disabled)
                {
                    UnloadDatabase(changedDatabase, null);
                    return;
                }

                Task<DocumentDatabase> task;
                if (DatabasesCache.TryGetValue(changedDatabase, out task) == false)
                    return;

                if (task.IsCanceled || task.IsFaulted)
                    return;

                if (task.IsCompleted)
                {
                    NotifyDatabaseAboutStateChange(changedDatabase, task);
                    return;
                }
                task.ContinueWith(done =>
                {
                    NotifyDatabaseAboutStateChange(changedDatabase, done);
                });
            }

            // if deleted, unload / deleted and then notify leader that we removed it
        }

        private void NotifyDatabaseAboutStateChange(string changedDatabase, Task<DocumentDatabase> done)
        {
            try
            {
                done.Result.StateChanged();
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
                    MaxDegreeOfParallelism = Math.Max(1, Environment.ProcessorCount / 2)
                }, dbTask =>
                {
                    if (dbTask.IsCompleted == false)
                        dbTask.ContinueWith(task =>
                        {
                            if (task.Status != TaskStatus.RanToCompletion)
                                return;

                            try
                            {
                                ((IDisposable)task.Result).Dispose();
                            }
                            catch (Exception e)
                            {
                                if (_logger.IsInfoEnabled)
                                    _logger.Info("Failure in deferred disposal of a database", e);
                            }
                        });
                    else if (dbTask.Status == TaskStatus.RanToCompletion)
                        exceptionAggregator.Execute(((IDisposable)dbTask.Result).Dispose);
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


        public Task<DocumentDatabase> TryGetOrCreateResourceStore(StringSegment databaseName, bool ignoreDisabledDatabase = false)
        {
            try
            {
                if (_disposing.TryEnterReadLock(0) == false)
                    ThrowServerIsBeingDisposed(databaseName);

                Task<DocumentDatabase> database;
                if (DatabasesCache.TryGetValue(databaseName, out database))
                    if (database.IsFaulted || database.IsCanceled)
                    {
                        DatabasesCache.TryRemove(databaseName, out database);
                        DateTime time;
                        LastRecentlyUsed.TryRemove(databaseName, out time);
                        // and now we will try creating it again
                    }
                    else
                    {
                        return database;
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

            if (!_resourceSemaphore.Wait(_concurrentResourceLoadTimeout))
                throw new DatabaseConcurrentLoadTimeoutException(
                    "Too much databases loading concurrently, timed out waiting for them to load.");
            try
            {
                var task = new Task<DocumentDatabase>(() => ActuallyCreateDatabase(databaseName, config));

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

        public RavenConfiguration CreateDatabaseConfiguration(StringSegment databaseName, bool ignoreDisabledDatabase = false)
        {
            if (databaseName.IsNullOrWhiteSpace())
                throw new ArgumentNullException(nameof(databaseName), "Database name cannot be empty");
            if (databaseName.Equals("<system>")) // This is here to guard against old ravendb tests
                throw new ArgumentNullException(nameof(databaseName),
                    "Database name cannot be <system>. Using of <system> database indicates outdated code that was targeted RavenDB 3.5.");

            var document = GetDatabaseDocument(databaseName, ignoreDisabledDatabase);
            if (document == null)
                return null;

            return CreateConfiguration(databaseName, document, RavenConfiguration.GetKey(x => x.Core.DataDirectory));
        }

        protected RavenConfiguration CreateConfiguration(StringSegment databaseName, DatabaseDocument document, string folderPropName)
        {
            var config = RavenConfiguration.CreateFrom(_serverStore.Configuration, databaseName, ResourceType.Database);

            foreach (var setting in document.Settings)
                config.SetSetting(setting.Key, setting.Value);
            Unprotect(document);

            foreach (var securedSetting in document.SecuredSettings)
                config.SetSetting(securedSetting.Key, securedSetting.Value);

            config.Initialize();
            config.CopyParentSettings(_serverStore.Configuration);
            return config;
        }

        public void Unprotect(DatabaseDocument databaseDocument)
        {
            if (databaseDocument.SecuredSettings == null)
            {
                databaseDocument.SecuredSettings = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
                return;
            }

            foreach (var prop in databaseDocument.SecuredSettings.ToList())
            {
                if (prop.Value == null)
                    continue;
                var bytes = Convert.FromBase64String(prop.Value);
                var entrophy = Encoding.UTF8.GetBytes(prop.Key);
                try
                {
                    /*var unprotectedValue = ProtectedData.Unprotect(bytes, entrophy, DataProtectionScope.CurrentUser);
                    databaseDocument.SecuredSettings[prop.Key] = Encoding.UTF8.GetString(unprotectedValue);*/
                }
                catch (Exception e)
                {
                    if (_logger.IsInfoEnabled)
                        _logger.Info("Could not unprotect secured db data " + prop.Key + " setting the value to '<data could not be decrypted>'", e);
                    databaseDocument.SecuredSettings[prop.Key] = Constants.Documents.Encryption.DataCouldNotBeDecrypted;
                }
            }
        }

        private DatabaseDocument GetDatabaseDocument(StringSegment databaseName, bool ignoreDisabledDatabase = false)
        {
            // We allocate the context here because it should be relatively rare operation
            TransactionOperationContext context;
            using (_serverStore.ContextPool.AllocateOperationContext(out context))
            {
                context.OpenReadTransaction();

                var dbId = Constants.Documents.Prefix + databaseName;
                var jsonReaderObject = _serverStore.Cluster.Read(context, dbId);
                if (jsonReaderObject == null)
                    return null;

                var document = JsonDeserializationClient.DatabaseDocument(jsonReaderObject);

                if (document.Disabled && !ignoreDisabledDatabase)
                    throw new DatabaseDisabledException("The database has been disabled.");

                return document;
            }
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

            return maxLastWork + TimeSpan.FromMilliseconds(dbSize / 1024L);
        }

        public void UnloadDatabase(string dbName, TimeSpan? skipIfActiveInDuration, Func<DocumentDatabase, bool> shouldSkip = null)
        {
            DateTime time;
            Task<DocumentDatabase> dbTask;
            if (DatabasesCache.TryGetValue(dbName, out dbTask) == false)
            {
                LastRecentlyUsed.TryRemove(dbName, out time);
                return;
            }
            var dbTaskStatus = dbTask.Status;
            if (dbTaskStatus == TaskStatus.Faulted || dbTaskStatus == TaskStatus.Canceled)
            {
                LastRecentlyUsed.TryRemove(dbName, out time);
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
                ((IDisposable)database).Dispose();
            }
            catch (Exception e)
            {
                if (_logger.IsInfoEnabled)
                    _logger.Info("Could not dispose database: " + dbName, e);
            }

            LastRecentlyUsed.TryRemove(dbName, out time);
            DatabasesCache.TryRemove(dbName, out dbTask);
        }
    }
}