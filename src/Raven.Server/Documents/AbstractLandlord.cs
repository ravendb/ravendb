using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.Logging;
using Raven.Server.ServerWide;
using Raven.Server.Utils;
using Sparrow;

namespace Raven.Server.Documents
{
    public abstract class AbstractLandlord<TResource> : IDisposable 
        where TResource : IDisposable
    {
        protected static readonly ILog Log = LogManager.GetLogger(typeof(AbstractLandlord<TResource>).FullName);

        protected readonly ServerStore ServerStore;
        protected readonly SemaphoreSlim ResourceSemaphore;
        protected readonly TimeSpan ConcurrentResourceLoadTimeout;

        public readonly ConcurrentDictionary<StringSegment, Task<TResource>> ResourcesStoresCache =
            new ConcurrentDictionary<StringSegment, Task<TResource>>();

        public readonly ConcurrentDictionary<StringSegment, DateTime> LastRecentlyUsed =
            new ConcurrentDictionary<StringSegment, DateTime>();

        protected readonly ConcurrentDictionary<string, AsyncManualResetEvent> Modification = 
            new ConcurrentDictionary<string, AsyncManualResetEvent>(StringComparer.OrdinalIgnoreCase);

        public AbstractLandlord(ServerStore serverStore)
        {
            ServerStore = serverStore;
            ResourceSemaphore = new SemaphoreSlim(ServerStore.Configuration.Databases.MaxConcurrentResourceLoads);
            ConcurrentResourceLoadTimeout = ServerStore.Configuration.Databases.ConcurrentResourceLoadTimeout.AsTimeSpan;
        }

        public abstract Task<TResource> GetResourceInternal(StringSegment resourceName);

        public abstract Task<Task<TResource>> TryGetOrCreateResourceStore(StringSegment resourceName);

        public void Dispose()
        {
            var tasks = ResourcesStoresCache.Select(resourceTask => resourceTask.Value.ContinueWith(task =>
            {
                try
                {
                    task.Result.Dispose();
                }
                catch (Exception)
                {
                    /* Nothing we can do here */
                }
            })).ToArray();
            Task.WaitAll(tasks);
            ResourcesStoresCache.Clear();

            try
            {
                ResourceSemaphore.Dispose();
            }
            catch (Exception e)
            {
                Log.WarnException("Failed to dispose resource semaphore", e);
            }
        }

        public async Task ModifyResource(string resourceName)
        {
            if (Modification.TryAdd(resourceName, new AsyncManualResetEvent()) == false)
                return;

            DateTime time;
            Task<TResource> resourceTask;
            if (ResourcesStoresCache.TryRemove(resourceName, out resourceTask) == false)
            {
                LastRecentlyUsed.TryRemove(resourceName, out time);
                return;
            }
            if (resourceTask.Status == TaskStatus.Faulted || resourceTask.Status == TaskStatus.Canceled)
            {
                LastRecentlyUsed.TryRemove(resourceName, out time);
                ResourcesStoresCache.TryRemove(resourceName, out resourceTask);
                return;
            }
            if (resourceTask.Status != TaskStatus.RanToCompletion)
            {
                throw new InvalidOperationException("Couldn't modify the database while it is loading");
            }

            var database = await resourceTask;
            try
            {
                database.Dispose();
            }
            catch (Exception e)
            {
                Log.ErrorException("Could not dispose database: " + resourceName, e);
            }

            LastRecentlyUsed.TryRemove(resourceName, out time);
            ResourcesStoresCache.TryRemove(resourceName, out resourceTask);

            AsyncManualResetEvent modifyLock;
            if (Modification.TryRemove(resourceName, out modifyLock))
                modifyLock.Set();
        }
    }
}