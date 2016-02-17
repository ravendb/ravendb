using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;
using System.Linq;
using System.Threading.Tasks;

using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Exceptions;
using Raven.Abstractions.Logging;
using Raven.Server.Config;
using Raven.Server.Json;
using Raven.Server.Routing;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;

namespace Raven.Server.Documents
{
    public class DatabasesLandlord : AbstractLandlord<DocumentDatabase>
    {
        public event Action<string> OnDatabaseLoaded = delegate { };

        public override async Task<DocumentDatabase> GetResourceInternal(StringSegment resourceName)
        {
            Task<DocumentDatabase> db;
            if (TryGetOrCreateResourceStore(resourceName, out db))
                return await db.ConfigureAwait(false);
            return null;
        }

        public override bool TryGetOrCreateResourceStore(StringSegment databaseId, out Task<DocumentDatabase> database)
        {
            //TODO: Restore those
            // if (Locks.Contains(DisposingLock))
            //     throw new ObjectDisposedException("DatabaseLandlord", "Server is shutting down, can't access any databases");
            // 
            // if (Locks.Contains(tenantId))
            //     throw new InvalidOperationException("Database '" + tenantId + "' is currently locked and cannot be accessed.");
            // 
            // ManualResetEvent cleanupLock;
            // if (Cleanups.TryGetValue(tenantId, out cleanupLock) && cleanupLock.WaitOne(MaxTimeForTaskToWaitForDatabaseToLoad) == false)
            //     throw new InvalidOperationException($"Database '{tenantId}' is currently being restarted and cannot be accessed. We already waited {MaxTimeForTaskToWaitForDatabaseToLoad.TotalSeconds} seconds.");

            if (ResourcesStoresCache.TryGetValue(databaseId, out database))
            {
                if (database.IsFaulted || database.IsCanceled)
                {
                    ResourcesStoresCache.TryRemove(databaseId, out database);
                    DateTime time;
                    LastRecentlyUsed.TryRemove(databaseId, out time);
                    // and now we will try creating it again
                }
                else
                {
                    return true;
                }
            }

            var config = CreateDatabaseConfiguration(databaseId);
            if (config == null)
                return false;

            var hasAcquired = false;
            try
            {
                if (!ResourceSemaphore.Wait(ConcurrentResourceLoadTimeout))
                    throw new ConcurrentLoadTimeoutException("Too much databases loading concurrently, timed out waiting for them to load.");

                hasAcquired = true;

                var task = new Task<DocumentDatabase>(() => CreateDocumentsStorage(databaseId, config));
                database = ResourcesStoresCache.GetOrAdd(databaseId, task);
                if (database == task)
                    task.Start();

                if (database.IsFaulted && database.Exception != null)
                {
                    // if we are here, there is an error, and if there is an error, we need to clear it from the 
                    // resource store cache so we can try to reload it.
                    // Note that we return the faulted task anyway, because we need the user to look at the error
                    if (database.Exception.Data.Contains("Raven/KeepInResourceStore") == false)
                    {
                        Task<DocumentDatabase> val;
                        ResourcesStoresCache.TryRemove(databaseId, out val);
                    }
                }

                return true;
            }
            finally
            {
                if (hasAcquired)
                    ResourceSemaphore.Release();
            }
        }

        private DocumentDatabase CreateDocumentsStorage(StringSegment databaseId, RavenConfiguration config)
        {
            try
            {
                var sp = Stopwatch.StartNew();
                var documentDatabase = new DocumentDatabase(config.DatabaseName, config);

                documentDatabase.Initialize();

                if (Log.IsInfoEnabled)
                {
                    Log.Info($"Started database {config.DatabaseName} in {sp.ElapsedMilliseconds:#,#;;0}ms");
                }

                OnDatabaseLoaded(config.DatabaseName);

                // if we have a very long init process, make sure that we reset the last idle time for this db.
                LastRecentlyUsed.AddOrUpdate(databaseId, SystemTime.UtcNow, (_, time) => SystemTime.UtcNow);
                return documentDatabase;
            }
            catch(Exception e)
            {
                if (Log.IsWarnEnabled)
                    Log.WarnException($"Failed to start database {config.DatabaseName}", e);
                throw;
            }
        }

        public RavenConfiguration CreateDatabaseConfiguration(StringSegment tenantId, bool ignoreDisabledDatabase = false)
        {
            if (tenantId.IsNullOrWhiteSpace())
                throw new ArgumentNullException(nameof(tenantId), "Tenant ID cannot be empty");
            if (tenantId.Equals("<system>"))
                throw new ArgumentNullException(nameof(tenantId), "Tenant ID cannot be <system>");

            var document = GetDatabaseDocument(tenantId, ignoreDisabledDatabase);
            if (document == null)
                return null;

            return CreateConfiguration(tenantId, document, RavenConfiguration.GetKey(x => x.Core.DataDirectory));
        }

        protected RavenConfiguration CreateConfiguration(StringSegment databaseName, DatabaseDocument document, string folderPropName)
        {
            var config = RavenConfiguration.CreateFrom(ServerStore.Configuration);

            foreach (var setting in document.Settings)
            {
                config.SetSetting(setting.Key, setting.Value);
            }
            Unprotect(document);

            foreach (var securedSetting in document.SecuredSettings)
            {
                config.SetSetting(securedSetting.Key, securedSetting.Value);
            }

            config.SetSetting(folderPropName, config.GetSetting(folderPropName).ToFullPath(ServerStore.Configuration.Core.DataDirectory));
            config.SetSetting(RavenConfiguration.GetKey(x => x.Storage.JournalsStoragePath), config.GetSetting(RavenConfiguration.GetKey(x => x.Storage.JournalsStoragePath)).ToFullPath(ServerStore.Configuration.Core.DataDirectory));

            config.DatabaseName = databaseName.ToString();

            config.Initialize();
            config.CopyParentSettings(ServerStore.Configuration);
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
                    Log.WarnException("Could not unprotect secured db data " + prop.Key + " setting the value to '<data could not be decrypted>'", e);
                    databaseDocument.SecuredSettings[prop.Key] = Constants.DataCouldNotBeDecrypted;
                }
            }
        }

        private DatabaseDocument GetDatabaseDocument(StringSegment tenantId, bool ignoreDisabledDatabase = false)
        {
            // We allocate the context here because it should be relatively rare operation
            TransactionOperationContext context;
            using (ServerStore.ContextPool.AllocateOperationContext(out context))
            {
                context.OpenReadTransaction();

                var id = Constants.Database.Prefix + tenantId;
                var jsonReaderObject = ServerStore.Read(context, id);
                if (jsonReaderObject == null)
                    return null;

                var document = JsonDeserialization.DatabaseDocument(jsonReaderObject);

                if (document.Settings[RavenConfiguration.GetKey(x => x.Core.DataDirectory)] == null)
                    throw new InvalidOperationException("Could not find " + RavenConfiguration.GetKey(x => x.Core.DataDirectory));

                if (document.Disabled && !ignoreDisabledDatabase)
                    throw new InvalidOperationException("The database has been disabled.");

                return document;
            }
        }

        public DatabasesLandlord(ServerStore serverStore) : base(serverStore)
        {

        }
    }
}