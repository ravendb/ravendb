using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.Logging;
using Raven.Server.ServerWide;
using Raven.Server.Utils;
using Sparrow;
using Sparrow.Collections;

namespace Raven.Server.Documents
{
    public abstract class AbstractLandlord<TResource> : IDisposable 
        where TResource : IDisposable
    {
        protected static readonly ILog Log = LogManager.GetLogger(typeof(AbstractLandlord<TResource>).FullName);

        protected readonly ServerStore ServerStore;
        protected readonly SemaphoreSlim ResourceSemaphore;
        protected readonly TimeSpan ConcurrentResourceLoadTimeout;
        protected static string DisposingLock = Guid.NewGuid().ToString();

        public readonly ConcurrentDictionary<StringSegment, Task<TResource>> ResourcesStoresCache =
            new ConcurrentDictionary<StringSegment, Task<TResource>>(CaseInsensitiveStringSegmentEqualityComparer.Instance);

        public readonly ConcurrentDictionary<StringSegment, DateTime> LastRecentlyUsed =
            new ConcurrentDictionary<StringSegment, DateTime>(CaseInsensitiveStringSegmentEqualityComparer.Instance);

        protected readonly ConcurrentSet<string> Locks = 
            new ConcurrentSet<string>(StringComparer.OrdinalIgnoreCase);

        public AbstractLandlord(ServerStore serverStore)
        {
            ServerStore = serverStore;
            ResourceSemaphore = new SemaphoreSlim(ServerStore.Configuration.Databases.MaxConcurrentResourceLoads);
            ConcurrentResourceLoadTimeout = ServerStore.Configuration.Databases.ConcurrentResourceLoadTimeout.AsTimeSpan;
        }

        public abstract Task<TResource> TryGetOrCreateResourceStore(StringSegment resourceName);

        public void Dispose()
        {
            Locks.TryAdd(DisposingLock);

            var exceptionAggregator = new ExceptionAggregator(Log, "Failure to dispose landlord");

            // shut down all databases in parallel, avoid having to wait for each one
            Parallel.ForEach(ResourcesStoresCache.Values, new ParallelOptions
            {
                // we limit the number of resources we dispose concurrently to avoid
                // putting too much pressure on the I/O system if a disposing db need
                // to flush data to disk
                MaxDegreeOfParallelism = Math.Max(1, Environment.ProcessorCount / 2),
            }, dbTask =>
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
                            Log.WarnException("Failure in deferred disposal of a database", e);
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
                Log.WarnException("Failed to dispose resource semaphore", e);
            }
        }

        private void UnloadResource(string resourceName)
        {
            DateTime time;
            Task<TResource> resourceTask;
            if (ResourcesStoresCache.TryGetValue(resourceName, out resourceTask) == false)
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
                throw new InvalidOperationException($"Couldn't modify '{resourceName}' while it is loading");
            }

            // will never wait, we checked that we already run to completion here
            var database = resourceTask.Result;
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
        }

        public void UnloadAndLock(string resourceName, Action actionToTake)
        {
            if (Locks.TryAdd(resourceName) == false)
                throw new InvalidOperationException(resourceName + "' is currently locked and cannot be accessed");

            try
            {
                UnloadResource(resourceName);
                actionToTake();
            }
            finally
            {
                Locks.TryRemove(resourceName);
            }
        }
    }
}