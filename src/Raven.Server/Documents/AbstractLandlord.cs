using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.Logging;
using Raven.Abstractions.Util;
using Raven.Server.Json;
using Raven.Server.Routing;
using Raven.Server.ServerWide;

namespace Raven.Server.Documents
{
    public abstract class AbstractLandlord<TResource> : IDisposable
    {
        protected static readonly ILog Log = LogManager.GetLogger(typeof(AbstractLandlord<TResource>).FullName);

        protected readonly ServerStore ServerStore;
        protected readonly SemaphoreSlim ResourceSemaphore;
        protected readonly TimeSpan ConcurrentResourceLoadTimeout;

        public readonly ConcurrentDictionary<StringSegment, Task<TResource>> ResourcesStoresCache =
            new ConcurrentDictionary<StringSegment, Task<TResource>>();

        public readonly ConcurrentDictionary<StringSegment, DateTime> LastRecentlyUsed =
            new ConcurrentDictionary<StringSegment, DateTime>();

        public AbstractLandlord(ServerStore serverStore)
        {
            ServerStore = serverStore;
            ResourceSemaphore = new SemaphoreSlim(ServerStore.Configuration.Databases.MaxConcurrentResourceLoads);
            ConcurrentResourceLoadTimeout = ServerStore.Configuration.Databases.ConcurrentResourceLoadTimeout.AsTimeSpan;
        }

        public abstract Task<TResource> GetResourceInternal(StringSegment resourceName);

        public abstract bool TryGetOrCreateResourceStore(StringSegment resourceName, out Task<TResource> resourceTask);

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