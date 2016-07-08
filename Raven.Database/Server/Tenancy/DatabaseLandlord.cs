using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Logging;
using Raven.Database.Commercial;
using Raven.Database.Config;
using Raven.Database.Extensions;
using Raven.Database.Server.Connections;
using Raven.Json.Linq;
using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

using Raven.Database.Server.Security;
using Raven.Abstractions.Exceptions;
using Raven.Database.Raft.Dto;

namespace Raven.Database.Server.Tenancy
{
    public class DatabasesLandlord : AbstractLandlord<DocumentDatabase>
    {
        public event Action<InMemoryRavenConfiguration> SetupTenantConfiguration = delegate { };

        public event Action<string> OnDatabaseLoaded = delegate { };

        private bool initialized;
        private const string DATABASES_PREFIX = "Raven/Databases/";
        public override string ResourcePrefix { get { return DATABASES_PREFIX; } }

        public int MaxIdleTimeForTenantDatabaseInSec { get; private set; }

        public int FrequencyToCheckForIdleDatabasesInSec { get; private set; }

        public DatabasesLandlord(DocumentDatabase systemDatabase) : base(systemDatabase)
        {
            int val;
            if (int.TryParse(SystemConfiguration.Settings["Raven/Tenants/MaxIdleTimeForTenantDatabase"], out val) == false)
                val = 900;

            MaxIdleTimeForTenantDatabaseInSec = val;

            if (int.TryParse(SystemConfiguration.Settings["Raven/Tenants/FrequencyToCheckForIdleDatabases"], out val) == false)
                val = 60;

            FrequencyToCheckForIdleDatabasesInSec = val;

            string tempPath = SystemConfiguration.TempPath;
            var fullTempPath = tempPath + Constants.TempUploadsDirectoryName;
            if (File.Exists(fullTempPath))
            {
                try
                {
                    File.Delete(fullTempPath);
                }
                catch (Exception)
                {
                    // we ignore this issue, nothing to do now, and we'll only see
                    // this as an error if there are actually uploads
                }
            }
            if (Directory.Exists(fullTempPath))
            {
                try
                {
                    Directory.Delete(fullTempPath, true);
                }
                catch (Exception)
                {
                    // there is nothing that we can do here, and it is possible that we have
                    // another database doing uploads for the same user, so we'll just 
                    // not any cleanup. Worst case, we'll waste some memory.
                }
            }

            Init();
        }

        public DocumentDatabase SystemDatabase
        {
            get { return systemDatabase; }
        }

        public InMemoryRavenConfiguration SystemConfiguration
        {
            get { return systemConfiguration; }
        }

        public InMemoryRavenConfiguration CreateTenantConfiguration(string tenantId, bool ignoreDisabledDatabase = false)
        {
            if (string.IsNullOrWhiteSpace(tenantId) || tenantId.Equals("<system>", StringComparison.OrdinalIgnoreCase))
                return systemConfiguration;
            var document = GetTenantDatabaseDocument(tenantId, ignoreDisabledDatabase);
            if (document == null)
                return null;

            Dictionary<string, string> clusterWideSettings = null;
            if (systemDatabase.ClusterManager?.Value?.HasNonEmptyTopology == true)
            {
                clusterWideSettings = GetClusterWideSettings();
            }

            return CreateConfiguration(tenantId, document, Constants.RavenDataDir, systemConfiguration, clusterWideSettings);
        }

        private Dictionary<string, string> GetClusterWideSettings()
        {
            var configurationJson = systemDatabase.Documents.Get(Constants.Cluster.ClusterConfigurationDocumentKey, null);
            if (configurationJson == null)
                return null;

            var configuration = configurationJson.DataAsJson.JsonDeserialization<ClusterConfiguration>();
            return configuration.DatabaseSettings;
        }

        private DatabaseDocument GetTenantDatabaseDocument(string tenantId, bool ignoreDisabledDatabase = false)
        {
            JsonDocument jsonDocument;
            using (systemDatabase.DisableAllTriggersForCurrentThread())
                jsonDocument = systemDatabase.Documents.Get(Constants.Database.Prefix + tenantId, null);
            if (jsonDocument == null ||
                jsonDocument.Metadata == null ||
                jsonDocument.Metadata.Value<bool>(Constants.RavenDocumentDoesNotExists) ||
                jsonDocument.Metadata.Value<bool>(Constants.RavenDeleteMarker))
                return null;

            var document = jsonDocument.DataAsJson.JsonDeserialization<DatabaseDocument>();
            if (document.Settings[Constants.RavenDataDir] == null)
                throw new InvalidOperationException("Could not find " + Constants.RavenDataDir);

            if (document.Disabled && !ignoreDisabledDatabase)
                throw new InvalidOperationException("The database has been disabled.");

            return document;
        }

        public override async Task<DocumentDatabase> GetResourceInternal(string resourceName)
        {
            if (string.Equals("<system>", resourceName, StringComparison.OrdinalIgnoreCase) || string.IsNullOrWhiteSpace(resourceName))
                return systemDatabase;
            Task<DocumentDatabase> db;
            if (TryGetOrCreateResourceStore(resourceName, out db))
                return await db.ConfigureAwait(false);
            return null;
        }

        public override bool TryGetOrCreateResourceStore(string tenantId, out Task<DocumentDatabase> database)
        {
            if (Locks.Contains(DisposingLock))
                throw new ObjectDisposedException("DatabaseLandlord", "Server is shutting down, can't access any databases");

            if (Locks.Contains(tenantId))
                throw new InvalidOperationException("Database '" + tenantId + "' is currently locked and cannot be accessed.");

            ManualResetEvent cleanupLock;
            if (Cleanups.TryGetValue(tenantId, out cleanupLock) && cleanupLock.WaitOne(MaxSecondsForTaskToWaitForDatabaseToLoad * 1000) == false)
                throw new InvalidOperationException(string.Format("Database '{0}' is currently being restarted and cannot be accessed. We already waited {1} seconds.", tenantId, MaxSecondsForTaskToWaitForDatabaseToLoad));

            if (ResourcesStoresCache.TryGetValue(tenantId, out database))
            {
                if (database.IsFaulted || database.IsCanceled)
                {
                    ResourcesStoresCache.TryRemove(tenantId, out database);
                    DateTime time;
                    LastRecentlyUsed.TryRemove(tenantId, out time);
                    // and now we will try creating it again
                }
                else
                {
                    return true;
                }
            }

            var config = CreateTenantConfiguration(tenantId);
            if (config == null)
                return false;

            var hasAcquired = false;
            try
            {
                if (!ResourceSemaphore.Wait(ConcurrentResourceLoadTimeout))
                    throw new ConcurrentLoadTimeoutException("Too much databases loading concurrently, timed out waiting for them to load.");

                hasAcquired = true;
                database = ResourcesStoresCache.GetOrAdd(tenantId, __ => Task.Factory.StartNew(() =>
                {
                    var transportState = ResourseTransportStates.GetOrAdd(tenantId, s => new TransportState());

                    AssertLicenseParameters(config);
                    var documentDatabase = new DocumentDatabase(config, systemDatabase, transportState);

                    documentDatabase.SpinBackgroundWorkers(false);
                    documentDatabase.Disposing += DocumentDatabaseDisposingStarted;
                    documentDatabase.DisposingEnded += DocumentDatabaseDisposingEnded;
                    documentDatabase.StorageInaccessible += UnloadDatabaseOnStorageInaccessible;
                    // register only DB that has incremental backup set.
                    documentDatabase.OnBackupComplete += OnDatabaseBackupCompleted;

                    // if we have a very long init process, make sure that we reset the last idle time for this db.
                    LastRecentlyUsed.AddOrUpdate(tenantId, SystemTime.UtcNow, (_, time) => SystemTime.UtcNow);
                    documentDatabase.RequestManager = SystemDatabase.RequestManager;
                    documentDatabase.ClusterManager = SystemDatabase.ClusterManager;
                    return documentDatabase;
                }).ContinueWith(task =>
                {
                    if (task.Status == TaskStatus.RanToCompletion)
                        OnDatabaseLoaded(tenantId);

                    if (task.Status == TaskStatus.Faulted) // this observes the task exception
                    {
                        Logger.WarnException("Failed to create database " + tenantId, task.Exception);
                    }
                    return task;
                }).Unwrap());
            }
            finally
            {
                if (hasAcquired)
                    ResourceSemaphore.Release();
            }

            if (database.IsFaulted && database.Exception != null)
            {
                // if we are here, there is an error, and if there is an error, we need to clear it from the 
                // resource store cache so we can try to reload it.
                // Note that we return the faulted task anyway, because we need the user to look at the error
                if (database.Exception.Data.Contains("Raven/KeepInResourceStore") == false)
                {
                    Task<DocumentDatabase> val;
                    ResourcesStoresCache.TryRemove(tenantId, out val);
                }
            }

            return true;
        }

        protected InMemoryRavenConfiguration CreateConfiguration(
                        string tenantId,
                        DatabaseDocument document,
                        string folderPropName,
                        InMemoryRavenConfiguration parentConfiguration, 
                        Dictionary<string, string> clusterSettings)
        {
            var effectiveParentSettings = new NameValueCollection(parentConfiguration.Settings);
            if (clusterSettings != null)
            {
                foreach (var keyValuePair in clusterSettings)
                {
                    effectiveParentSettings[keyValuePair.Key] = keyValuePair.Value;
                }
            }

            var config = new InMemoryRavenConfiguration
            {
                Settings = new NameValueCollection(effectiveParentSettings),
            };

            if (config.Settings["Raven/CompiledIndexCacheDirectory"] == null)
            {
                var compiledIndexCacheDirectory = parentConfiguration.CompiledIndexCacheDirectory;
                config.Settings["Raven/CompiledIndexCacheDirectory"] = compiledIndexCacheDirectory;
            }

            if (config.Settings[Constants.TempPath] == null)
                config.Settings[Constants.TempPath] = parentConfiguration.TempPath;

            SetupTenantConfiguration(config);

            config.CustomizeValuesForDatabaseTenant(tenantId);

            config.Settings["Raven/StorageEngine"] = parentConfiguration.DefaultStorageTypeName;

            foreach (var setting in document.Settings)
            {
                config.Settings[setting.Key] = setting.Value;
            }
            Unprotect(document);

            foreach (var securedSetting in document.SecuredSettings)
            {
                config.Settings[securedSetting.Key] = securedSetting.Value;
            }

            config.Settings[folderPropName] = config.Settings[folderPropName].ToFullPath(parentConfiguration.DataDirectory);

            config.Settings["Raven/Esent/LogsPath"] = config.Settings["Raven/Esent/LogsPath"].ToFullPath(parentConfiguration.DataDirectory);
            config.Settings[Constants.RavenTxJournalPath] = config.Settings[Constants.RavenTxJournalPath].ToFullPath(parentConfiguration.DataDirectory);

            config.Settings["Raven/VirtualDir"] = config.Settings["Raven/VirtualDir"] + "/" + tenantId;

            config.DatabaseName = tenantId;
            config.IsTenantDatabase = true;

            config.Initialize();
            config.CopyParentSettings(parentConfiguration);
            return config;
        }

        public void Protect(DatabaseDocument databaseDocument)
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
                var bytes = Encoding.UTF8.GetBytes(prop.Value);
                var entrophy = Encoding.UTF8.GetBytes(prop.Key);
                var protectedValue = ProtectedData.Protect(bytes, entrophy, DataProtectionScope.CurrentUser);
                databaseDocument.SecuredSettings[prop.Key] = Convert.ToBase64String(protectedValue);
            }
        }

        private void OnDatabaseBackupCompleted(DocumentDatabase db)
        {
            var dbStatusKey = "Raven/BackupStatus/" + db.Name;
            var statusDocument = db.Documents.Get(dbStatusKey, null);
            DatabaseOperationsStatus status;
            if (statusDocument == null)
            {
                status = new DatabaseOperationsStatus();
            }
            else
            {
                status = statusDocument.DataAsJson.JsonDeserialization<DatabaseOperationsStatus>();
            }
            status.LastBackup = SystemTime.UtcNow;
            var json = RavenJObject.FromObject(status);
            json.Remove("Id");
            systemDatabase.Documents.Put(dbStatusKey, null, json, new RavenJObject(), null);
        }

        private void AssertLicenseParameters(InMemoryRavenConfiguration config)
        {
            string maxDatabases;
            if (ValidateLicense.CurrentLicense.Attributes.TryGetValue("numberOfDatabases", out maxDatabases))
            {
                if (string.Equals(maxDatabases, "unlimited", StringComparison.OrdinalIgnoreCase) == false)
                {
                    var numberOfAllowedDbs = int.Parse(maxDatabases);

                    int nextPageStart = 0;
                    var databases = systemDatabase.Documents.GetDocumentsWithIdStartingWith("Raven/Databases/", null, null, 0, numberOfAllowedDbs, CancellationToken.None, ref nextPageStart).ToList();
                    if (databases.Count > numberOfAllowedDbs)
                        throw new InvalidOperationException(
                            "You have reached the maximum number of databases that you can have according to your license: " + numberOfAllowedDbs + Environment.NewLine +
                            "But we detect: " + databases.Count + " databases" + Environment.NewLine +
                            "You can either upgrade your RavenDB license or delete a database from the server");
                }
            }

            Authentication.AssertLicensedBundles(config.ActiveBundles);
                }

        public void ForAllDatabases(Action<DocumentDatabase> action, bool excludeSystemDatabase = false)
        {
            if (!excludeSystemDatabase) action(systemDatabase);
            foreach (var value in ResourcesStoresCache
                .Select(db => db.Value)
                .Where(value => value.Status == TaskStatus.RanToCompletion))
            {
                action(value.Result);
            }
        }

        protected override DateTime LastWork(DocumentDatabase resource)
        {
            var databaseSizeInformation = resource.TransactionalStorage.GetDatabaseSize();
            return resource.WorkContext.LastWorkTime +
                   // this allow us to increase the time large databases will be held in memory
                   // because they are more expensive to unload & reload. Using this method, we'll
                   // add 0.5 ms per each KB, or roughly half a second of idle time per MB.
                   // A DB with 1GB will remain live another 16 minutes after being item. Given the default idle time
                   // that means that we'll keep it alive for about 30 minutes without shutting down.
                   // A database with 50GB will take roughly 8 hours of idle time to shut down. 
                   TimeSpan.FromMilliseconds(databaseSizeInformation.AllocatedSizeInBytes / 1024L / 2);
        }

        public void Init()
        {
            if (initialized)
                return;
            initialized = true;
            SystemDatabase.Notifications.OnDocumentChange += (database, notification, doc) =>
            {
                if (notification.Id == null)
                    return;
                const string ravenDbPrefix = "Raven/Databases/";
                if (notification.Id.StartsWith(ravenDbPrefix, StringComparison.InvariantCultureIgnoreCase) == false)
                    return;
                var dbName = notification.Id.Substring(ravenDbPrefix.Length);
                Logger.Info("Shutting down database {0} because the tenant database has been updated or removed", dbName);
                Cleanup(dbName, skipIfActiveInDuration: null, notificationType: notification.Type);
            };
        }

        public bool IsDatabaseLoaded(string tenantName)
        {
            if (tenantName == Constants.SystemDatabase)
                return true;

            Task<DocumentDatabase> dbTask;
            if (ResourcesStoresCache.TryGetValue(tenantName, out dbTask) == false)
                return false;

            return dbTask != null && dbTask.Status == TaskStatus.RanToCompletion;
        }

        private void DocumentDatabaseDisposingStarted(object documentDatabase, EventArgs args)
        {
            try
            {
                var database = documentDatabase as DocumentDatabase;
                if (database == null)
                {
                    return;
                }

                ResourcesStoresCache.Set(database.Name, (dbName) =>
                {
                    var tcs = new TaskCompletionSource<DocumentDatabase>();
                    tcs.SetException(new ObjectDisposedException(database.Name, "Database named " + database.Name + " is being disposed right now and cannot be accessed.\r\n" +
                                                                 "Access will be available when the dispose process will end")
                    {
                        Data =
                        {
                            {"Raven/KeepInResourceStore", "true"}
                        }
                    });
                    // we need to observe this task exception in case no one is actually looking at it during disposal
                    GC.KeepAlive(tcs.Task.Exception);
                    return tcs.Task;
                });
            }
            catch (Exception ex)
            {
                Logger.WarnException("Failed to substitute database task with temporary place holder. This should not happen", ex);
            }
        }

        private void DocumentDatabaseDisposingEnded(object documentDatabase, EventArgs args)
        {
            try
            {
                var database = documentDatabase as DocumentDatabase;
                if (database == null)
                {
                    return;
                }

                ResourcesStoresCache.Remove(database.Name);
            }
            catch (Exception ex)
            {
                Logger.ErrorException("Failed to remove database at the end of the disposal. This should not happen", ex);
            }
        }

        private void UnloadDatabaseOnStorageInaccessible(object documentDatabase, EventArgs eventArgs)
        {
            try
            {
                var database = documentDatabase as DocumentDatabase;
                if (database == null)
                {
                    return;
                }

                Task.Run(() =>
                {
                    Thread.Sleep(2000); // let the exception thrown by the storage to be propagated into the client

                    Logger.Warn("Shutting down database {0} because its storage has become inaccessible", database.Name);

                    Cleanup(database.Name, skipIfActiveInDuration: null, shouldSkip: x => false);
                });

            }
            catch (Exception ex)
            {
                Logger.ErrorException("Failed to cleanup database that storage is inaccessible. This should not happen", ex);
            }

        }
    }
}
