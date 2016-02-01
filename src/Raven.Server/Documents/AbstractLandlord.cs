using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.Logging;
using Raven.Abstractions.Util;
using Raven.Server.Json;
using Raven.Server.ServerWide;

namespace Raven.Server.Documents
{
    public abstract class AbstractLandlord<TResource> : IResourceLandlord<TResource>
        where TResource : IResourceStore
    {
        protected static readonly ILog Log = LogManager.GetLogger(typeof(AbstractLandlord<TResource>).FullName);

        protected readonly ServerStore ServerStore;
        protected readonly SemaphoreSlim ResourceSemaphore;
        protected readonly TimeSpan ConcurrentResourceLoadTimeout;

        public readonly AtomicDictionary<Task<TResource>> ResourcesStoresCache =
            new AtomicDictionary<Task<TResource>>(StringComparer.OrdinalIgnoreCase);

        public readonly ConcurrentDictionary<string, DateTime> LastRecentlyUsed = 
            new ConcurrentDictionary<string, DateTime>(StringComparer.OrdinalIgnoreCase);

        public AbstractLandlord(ServerStore serverStore)
        {
            ServerStore = serverStore;
            ResourceSemaphore = new SemaphoreSlim(ServerStore.Configuration.Databases.MaxConcurrentResourceLoads);
            ConcurrentResourceLoadTimeout = ServerStore.Configuration.Databases.ConcurrentResourceLoadTimeout.AsTimeSpan;
        }

        public abstract Task<TResource> GetResourceInternal(string resourceName, RavenOperationContext context);

        public abstract bool TryGetOrCreateResourceStore(string resourceName, RavenOperationContext context, out Task<TResource> resourceTask);

        public void Dispose()
        {
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
    }
}