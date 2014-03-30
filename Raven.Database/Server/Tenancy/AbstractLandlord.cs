using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;
using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Logging;
using Raven.Abstractions.Util;
using Raven.Database.Config;
using Raven.Database.Extensions;
using Raven.Database.Impl;
using Raven.Database.Util;

namespace Raven.Database.Server.Tenancy
{
    public abstract class AbstractLandlord<TResource> : IDisposable
        where TResource : IDisposable
    {
        protected static readonly ILog Logger = LogManager.GetCurrentClassLogger();
        public event Action<InMemoryRavenConfiguration> SetupTenantConfiguration = delegate { };
        protected readonly ConcurrentSet<string> Locks = new ConcurrentSet<string>(StringComparer.OrdinalIgnoreCase);
        public readonly AtomicDictionary<Task<TResource>> ResourcesStoresCache =
                new AtomicDictionary<Task<TResource>>(StringComparer.OrdinalIgnoreCase);

        public readonly ConcurrentDictionary<string, DateTime> LastRecentlyUsed = new ConcurrentDictionary<string, DateTime>(StringComparer.OrdinalIgnoreCase);
        public event Action<string> CleanupOccured;
		
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
                    var unprotectedValue = ProtectedData.Unprotect(bytes, entrophy, DataProtectionScope.CurrentUser);
                    databaseDocument.SecuredSettings[prop.Key] = Encoding.UTF8.GetString(unprotectedValue);
                }
                catch (Exception e)
                {
                    Logger.WarnException("Could not unprotect secured db data " + prop.Key + " setting the value to '<data could not be decrypted>'", e);
                    databaseDocument.SecuredSettings[prop.Key] = "<data could not be decrypted>";
                }
            }
        }

        public void Cleanup(string db, bool skipIfActive)
        {
            using (ResourcesStoresCache.WithAllLocks())
            {
                DateTime time;
                Task<TResource> databaseTask;
                if (ResourcesStoresCache.TryGetValue(db, out databaseTask) == false)
                {
                    LastRecentlyUsed.TryRemove(db, out time);
                    return;
                }
                if (databaseTask.Status == TaskStatus.Faulted || databaseTask.Status == TaskStatus.Canceled)
                {
                    LastRecentlyUsed.TryRemove(db, out time);
                    ResourcesStoresCache.TryRemove(db, out databaseTask);
                    return;
                }
                if (databaseTask.Status != TaskStatus.RanToCompletion)
                {
                    return; // still starting up
                }

                var database = databaseTask.Result;
                if (skipIfActive &&
                    (SystemTime.UtcNow - LastWork(database)).TotalMinutes < 10)
                {
                    // this document might not be actively working with user, but it is actively doing indexes, we will 
                    // wait with unloading this database until it hasn't done indexing for a while.
                    // This prevent us from shutting down big databases that have been left alone to do indexing work.
                    return;
                }
                try
                {
                    database.Dispose();
                }
                catch (Exception e)
                {
                    Logger.ErrorException("Could not cleanup tenant database: " + db, e);
                    return;
                }
                LastRecentlyUsed.TryRemove(db, out time);
                ResourcesStoresCache.TryRemove(db, out databaseTask);

                var onDatabaseCleanupOccured = CleanupOccured;
                if (onDatabaseCleanupOccured != null)
                    onDatabaseCleanupOccured(db);
            }
        }

        protected abstract DateTime LastWork(TResource resource);

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
        protected InMemoryRavenConfiguration CreateConfiguration(
            string tenantId, 
            DatabaseDocument document, 
            string folderPropName,
            InMemoryRavenConfiguration parentConfiguration)
        {
            var config = new InMemoryRavenConfiguration
            {
                Settings = new NameValueCollection(parentConfiguration.Settings),
            };

            SetupTenantConfiguration(config);

            config.CustomizeValuesForTenant(tenantId);


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
            config.Settings["Raven/VirtualDir"] = config.Settings["Raven/VirtualDir"] + "/" + tenantId;

            config.DatabaseName = tenantId;
            config.IsTenantDatabase = true;

            config.Initialize();
            config.CopyParentSettings(parentConfiguration);
            return config;
        }



        public void Lock(string tenantId, Action actionToTake)
        {
            if (Locks.TryAdd(tenantId) == false)
                throw new InvalidOperationException(tenantId + "' is currently locked and cannot be accessed");
            try
            {
                Cleanup(tenantId, false);
                actionToTake();
            }
            finally
            {
                Locks.TryRemove(tenantId);
            }
        }


        public void Dispose()
        {
            var exceptionAggregator = new ExceptionAggregator(Logger, "Failure to dispose landlord");
            using (ResourcesStoresCache.WithAllLocks())
            {
                // shut down all databases in parallel, avoid having to wait for each one
                Parallel.ForEach(ResourcesStoresCache.Values, dbTask =>
                {
                    if (dbTask.IsCompleted == false)
                    {
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
                                Logger.WarnException("Failure in deferred disposal of a database", e);
                            }
                        });
                    }
                    else if (dbTask.Status == TaskStatus.RanToCompletion)
                    {
                        exceptionAggregator.Execute(dbTask.Result.Dispose);
                    }
                    // there is no else, the db is probably faulted
                });
                ResourcesStoresCache.Clear();
            }
        }
    }
}