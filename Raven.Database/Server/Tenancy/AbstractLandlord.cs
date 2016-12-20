using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography;
using System.Security.Principal;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Logging;
using Raven.Abstractions.Util;
using Raven.Database.Common;
using Raven.Database.Config;
using Raven.Database.Extensions;
using Raven.Database.Impl;
using Raven.Database.Server.Connections;
using Raven.Database.Server.Security;
using Raven.Json.Linq;
using Sparrow.Collections;

namespace Raven.Database.Server.Tenancy
{
    public abstract class AbstractLandlord<TResource> : IResourceLandlord<TResource>
        where TResource : IResourceStore
    {
        protected static string DisposingLock = Guid.NewGuid().ToString();

        protected readonly SemaphoreSlim ResourceSemaphore;

        protected static readonly ILog Logger = LogManager.GetCurrentClassLogger();

        protected readonly ConcurrentSet<string> Locks = new ConcurrentSet<string>(StringComparer.OrdinalIgnoreCase);

        protected readonly ConcurrentDictionary<string, ManualResetEvent> Cleanups = new ConcurrentDictionary<string, ManualResetEvent>(StringComparer.OrdinalIgnoreCase);

        public readonly AtomicDictionary<Task<TResource>> ResourcesStoresCache =
                new AtomicDictionary<Task<TResource>>(StringComparer.OrdinalIgnoreCase);

        public event Action<string> CleanupOccured;

        protected readonly ConcurrentDictionary<string, TransportState> ResourseTransportStates = new ConcurrentDictionary<string, TransportState>(StringComparer.OrdinalIgnoreCase);

        public abstract string ResourcePrefix { get; }

        protected readonly InMemoryRavenConfiguration systemConfiguration;
        protected readonly DocumentDatabase systemDatabase;

        protected readonly TimeSpan ConcurrentResourceLoadTimeout;

        protected AbstractLandlord(DocumentDatabase systemDatabase)
        {
            systemConfiguration = systemDatabase.Configuration;
            this.systemDatabase = systemDatabase;
            ResourceSemaphore = new SemaphoreSlim(systemDatabase.Configuration.MaxConcurrentResourceLoads);
            ConcurrentResourceLoadTimeout = systemDatabase.Configuration.ConcurrentResourceLoadTimeout;
        }

        public int MaxSecondsForTaskToWaitForDatabaseToLoad
        {
            get
            {
                return systemConfiguration.MaxSecondsForTaskToWaitForDatabaseToLoad;
            }
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
                    var unprotectedValue = ProtectedData.Unprotect(bytes, entrophy, DataProtectionScope.CurrentUser);
                    databaseDocument.SecuredSettings[prop.Key] = Encoding.UTF8.GetString(unprotectedValue);
                }
                catch (Exception e)
                {
                    Logger.WarnException("Could not unprotect secured db data " + prop.Key + " setting the value to '<data could not be decrypted>'", e);
                    databaseDocument.SecuredSettings[prop.Key] = Constants.DataCouldNotBeDecrypted;
                }
            }
        }

        public void Cleanup(string resource,
            TimeSpan? skipIfActiveInDuration,
            Func<TResource, bool> shouldSkip = null,
            DocumentChangeTypes notificationType = DocumentChangeTypes.None)
        {
            if (Cleanups.TryAdd(resource, new ManualResetEvent(false)) == false)
                return;

            try
            {
                using (ResourcesStoresCache.WithAllLocks())
                {
                    DateTime time;
                    Task<TResource> resourceTask;
                    if (ResourcesStoresCache.TryGetValue(resource, out resourceTask) == false)
                    {
                        LastRecentlyUsed.TryRemove(resource, out time);
                        return;
                    }
                    if (resourceTask.Status == TaskStatus.Faulted || resourceTask.Status == TaskStatus.Canceled)
                    {
                        LastRecentlyUsed.TryRemove(resource, out time);
                        ResourcesStoresCache.TryRemove(resource, out resourceTask);
                        return;
                    }
                    if (resourceTask.Status != TaskStatus.RanToCompletion)
                    {
                        return; // still starting up
                    }

                    var database = resourceTask.Result;
                    if ((skipIfActiveInDuration != null && (SystemTime.UtcNow - LastWork(database)) < skipIfActiveInDuration) ||
                        (shouldSkip != null && shouldSkip(database)))
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
                        Logger.ErrorException("Could not cleanup tenant database: " + resource, e);
                        return;
                    }

                    LastRecentlyUsed.TryRemove(resource, out time);
                    ResourcesStoresCache.TryRemove(resource, out resourceTask);

                    if (notificationType == DocumentChangeTypes.Delete)
                    {
                        TransportState transportState;
                        ResourseTransportStates.TryRemove(resource, out transportState);
                        if (transportState != null)
                        {
                            transportState.Dispose();
                        }
                    }

                    var onResourceCleanupOccured = CleanupOccured;
                    if (onResourceCleanupOccured != null)
                        onResourceCleanupOccured(resource);
                }
            }
            finally
            {
                ManualResetEvent cleanupLock;
                if (Cleanups.TryRemove(resource, out cleanupLock))
                    cleanupLock.Set();
            }
        }

        protected abstract DateTime LastWork(TResource resource);

        public void Lock(string tenantId, Action actionToTake)
        {
            if (Locks.TryAdd(tenantId) == false)
                throw new InvalidOperationException(tenantId + "' is currently locked and cannot be accessed");
            try
            {
                Cleanup(tenantId, skipIfActiveInDuration: null);
                actionToTake();
            }
            finally
            {
                Locks.TryRemove(tenantId);
            }
        }

        public void Dispose()
        {
            Locks.TryAdd(DisposingLock);

            var exceptionAggregator = new ExceptionAggregator(Logger, "Failure to dispose landlord");
            exceptionAggregator.Execute(() =>
            {
                foreach (var databaseTransportState in ResourseTransportStates)
                {
                    databaseTransportState.Value.Dispose();
                }
            });

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

            try
            {
                ResourceSemaphore.Dispose();
            }
            catch (Exception e)
            {
                Logger.WarnException("Failed to dispose resource semaphore", e);
            }
        }

        public abstract Task<TResource> GetResourceInternal(string resourceName);

        public abstract bool TryGetOrCreateResourceStore(string resourceName, out Task<TResource> resourceTask);

        private ConcurrentDictionary<string, DateTime> _lastRecentlyUsed;
        public ConcurrentDictionary<string, DateTime> LastRecentlyUsed
        {
            get
            {
                if (_lastRecentlyUsed == null)
                    _lastRecentlyUsed = new ConcurrentDictionary<string, DateTime>(StringComparer.OrdinalIgnoreCase);

                return _lastRecentlyUsed;
            }
        }
        public InMemoryRavenConfiguration GetSystemConfiguration()
        {
            return systemConfiguration;
        }
    }
}