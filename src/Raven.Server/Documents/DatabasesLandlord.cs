using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client;
using Raven.Client.Exceptions.Database;
using Raven.Client.Json.Converters;
using Raven.Client.Server;
using Raven.Client.Util;
using Raven.Server.Config;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.NotificationCenter.Notifications.Details;
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
        public static readonly string DisposingLock = Guid.NewGuid().ToString();

        public readonly ConcurrentDictionary<StringSegment, DateTime> LastRecentlyUsed =
            new ConcurrentDictionary<StringSegment, DateTime>(CaseInsensitiveStringSegmentEqualityComparer.Instance);

        private readonly ConcurrentSet<string> _locks =
            new ConcurrentSet<string>(StringComparer.OrdinalIgnoreCase);

        public readonly ResourceCache<DocumentDatabase> ResourcesStoresCache = new ResourceCache<DocumentDatabase>();
        private readonly TimeSpan _concurrentResourceLoadTimeout;
        private int _hasLocks;
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

        public TimeSpan DatabaseLoadTimeout => _serverStore.Configuration.Server.MaxTimeForTaskToWaitForDatabaseToLoad.AsTimeSpan;

        public void Dispose()
        {
            _locks.TryAdd(DisposingLock);

            var exceptionAggregator = new ExceptionAggregator(_logger, "Failure to dispose landlord");

            // shut down all databases in parallel, avoid having to wait for each one
            Parallel.ForEach(ResourcesStoresCache.Values, new ParallelOptions
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
            ResourcesStoresCache.Clear();

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

        public event Action<string> OnDatabaseLoaded = delegate { };


        public Task<DocumentDatabase> TryGetOrCreateResourceStore(StringSegment databaseName, bool ignoreDisabledDatabase = false)
        {
            if (_hasLocks != 0)
                AssertLocks(databaseName);
            Task<DocumentDatabase> database;
            if (ResourcesStoresCache.TryGetValue(databaseName, out database))
                if (database.IsFaulted || database.IsCanceled)
                {
                    ResourcesStoresCache.TryRemove(databaseName, out database);
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
                var task = new Task<DocumentDatabase>(() => ActuallyCreateDatabase(databaseName, config));

                var database = ResourcesStoresCache.GetOrAdd(databaseName, task);
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
                    ResourcesStoresCache.TryRemove(databaseName, out val);
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

        private void AssertLocks(string databaseName)
        {
            if (_locks.Contains(DisposingLock))
                throw new ObjectDisposedException("DatabaseLandlord", "Server is shutting down, can't access any databases");

            if (_locks.Contains(databaseName))
                throw new InvalidOperationException($"Database '{databaseName}' is currently locked and cannot be accessed.");
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
                var jsonReaderObject = _serverStore.Read(context, dbId);
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

            return maxLastWork.AddMilliseconds(dbSize / 1024L);
        }

        public void UnloadResourceOnCatastrophicFailure(string databaseName, Exception e)
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

                UnloadResource(databaseName, null);
            });
        }

        public void UnloadResource(string resourceName, TimeSpan? skipIfActiveInDuration, Func<DocumentDatabase, bool> shouldSkip = null)
        {
            DateTime time;
            Task<DocumentDatabase> resourceTask;
            if (ResourcesStoresCache.TryGetValue(resourceName, out resourceTask) == false)
            {
                LastRecentlyUsed.TryRemove(resourceName, out time);
                return;
            }
            var resourceTaskStatus = resourceTask.Status;
            if (resourceTaskStatus == TaskStatus.Faulted || resourceTaskStatus == TaskStatus.Canceled)
            {
                LastRecentlyUsed.TryRemove(resourceName, out time);
                ResourcesStoresCache.TryRemove(resourceName, out resourceTask);
                return;
            }
            if (resourceTaskStatus != TaskStatus.RanToCompletion)
                throw new InvalidOperationException($"Couldn't modify '{resourceName}' while it is loading, current status {resourceTaskStatus}");

            // will never wait, we checked that we already run to completion here
            var database = resourceTask.Result;

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
                    _logger.Info("Could not dispose database: " + resourceName, e);
            }

            LastRecentlyUsed.TryRemove(resourceName, out time);
            ResourcesStoresCache.TryRemove(resourceName, out resourceTask);
        }

        public void UnloadAndLock(string resourceName, Action actionToTake)
        {
            if (_locks.TryAdd(resourceName) == false)
                throw new InvalidOperationException(resourceName + "' is currently locked and cannot be accessed");
            Interlocked.Increment(ref _hasLocks);
            try
            {
                UnloadResource(resourceName, null);
                actionToTake();
            }
            finally
            {
                _locks.TryRemove(resourceName);
                Interlocked.Decrement(ref _hasLocks);
            }
        }
    }
}