using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Text;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Exceptions;
using Raven.Abstractions.Logging;
using Raven.Abstractions.Util;
using Raven.Server.Config;
using Raven.Server.Json;
using Raven.Server.ServerWide;
using Raven.Server.Utils;

namespace Raven.Server.Documents
{
    public class DatabasesLandlord : AbstractLandlord<DocumentDatabase>
    {
        public event Action<string> OnDatabaseLoaded = delegate { };

        public override async Task<DocumentDatabase> GetResourceInternal(string resourceName, RavenOperationContext context)
        {
            Task<DocumentDatabase> db;
            if (TryGetOrCreateResourceStore(resourceName, context, out db))
                return await db.ConfigureAwait(false);
            return null;
        }

        public override bool TryGetOrCreateResourceStore(string databaseId, RavenOperationContext context, out Task<DocumentDatabase> database)
        {
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

            var config = CreateDatabaseConfiguration(databaseId, context);
            if (config == null)
                return false;

            var hasAcquired = false;
            try
            {
                if (!ResourceSemaphore.Wait(ConcurrentResourceLoadTimeout))
                    throw new ConcurrentLoadTimeoutException("Too much databases loading concurrently, timed out waiting for them to load.");

                hasAcquired = true;
                database = ResourcesStoresCache.GetOrAdd(databaseId, __ => Task.Factory.StartNew(() =>
                {
                    var documentDatabase = new DocumentDatabase(databaseId, config);

                    // if we have a very long init process, make sure that we reset the last idle time for this db.
                    LastRecentlyUsed.AddOrUpdate(databaseId, SystemTime.UtcNow, (_, time) => SystemTime.UtcNow);
                    return documentDatabase;
                }).ContinueWith(task =>
                {
                    if (task.Status == TaskStatus.RanToCompletion)
                        OnDatabaseLoaded(databaseId);

                    if (task.Status == TaskStatus.Faulted) // this observes the task exception
                    {
                        Log.WarnException("Failed to create database " + databaseId, task.Exception);
                    }
                    return task;
                }).Unwrap());

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

        public RavenConfiguration CreateDatabaseConfiguration(string tenantId, RavenOperationContext context, bool ignoreDisabledDatabase = false)
        {
            if (string.IsNullOrWhiteSpace(tenantId))
                throw new ArgumentNullException(nameof(tenantId), "Tenant ID cannot be empty");
            if (tenantId.Equals("<system>", StringComparison.OrdinalIgnoreCase))
                throw new ArgumentNullException(nameof(tenantId), "Tenant ID cannot be <system>");

            var document = GetTenantDatabaseDocument(tenantId, context, ignoreDisabledDatabase);
            if (document == null)
                return null;

            return CreateConfiguration(tenantId, document, RavenConfiguration.GetKey(x => x.Core.DataDirectory));
        }

        protected RavenConfiguration CreateConfiguration(string databaseName, DatabaseDocument document, string folderPropName)
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

            config.DatabaseName = databaseName;

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

        private DatabaseDocument GetTenantDatabaseDocument(string tenantId, RavenOperationContext context, bool ignoreDisabledDatabase = false)
        {
            var id = Constants.Database.Prefix + tenantId;
            var jsonReaderObject = ServerStore.Read(context, id);
            if (jsonReaderObject == null)
                return null;

            var document = jsonReaderObject.Deserialize<DatabaseDocument>();

            if (document.Settings[RavenConfiguration.GetKey(x => x.Core.DataDirectory)] == null)
                throw new InvalidOperationException("Could not find " + RavenConfiguration.GetKey(x => x.Core.DataDirectory));

            if (document.Disabled && !ignoreDisabledDatabase)
                throw new InvalidOperationException("The database has been disabled.");

            return document;
        }

        public DatabasesLandlord(ServerStore serverStore) : base(serverStore)
        {

        }
    }
}