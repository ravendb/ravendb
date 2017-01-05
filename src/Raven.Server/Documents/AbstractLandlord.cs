using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions;
using Raven.Server.ServerWide;
using Raven.Server.Utils;
using Sparrow;
using Sparrow.Collections;
using Sparrow.Logging;

namespace Raven.Server.Documents
{
    public abstract class AbstractLandlord<TResource> : IDisposable 
        where TResource : IDisposable
    {
        protected static Logger Logger;
        protected int HasLocks;

        protected readonly ServerStore ServerStore;
        protected readonly SemaphoreSlim ResourceSemaphore;
        protected readonly TimeSpan ConcurrentResourceLoadTimeout;
        protected static string DisposingLock = Guid.NewGuid().ToString();

        public readonly ResourceCache<TResource> ResourcesStoresCache = new ResourceCache<TResource>();

        public readonly ConcurrentDictionary<StringSegment, DateTime> LastRecentlyUsed =
            new ConcurrentDictionary<StringSegment, DateTime>(CaseInsensitiveStringSegmentEqualityComparer.Instance);

        protected readonly ConcurrentSet<string> Locks = 
            new ConcurrentSet<string>(StringComparer.OrdinalIgnoreCase);

        protected AbstractLandlord(ServerStore serverStore)
        {
            ServerStore = serverStore;
            ResourceSemaphore = new SemaphoreSlim(ServerStore.Configuration.Databases.MaxConcurrentResourceLoads);
            ConcurrentResourceLoadTimeout = ServerStore.Configuration.Databases.ConcurrentResourceLoadTimeout.AsTimeSpan;
            Logger = LoggingSource.Instance.GetLogger<AbstractLandlord<TResource>>("Raven/Server");
        }

        public TimeSpan DatabaseLoadTimeout => ServerStore.Configuration.Server.MaxTimeForTaskToWaitForDatabaseToLoad.AsTimeSpan;

        public abstract Task<TResource> TryGetOrCreateResourceStore(StringSegment resourceName);

        public void Dispose()
        {
            Locks.TryAdd(DisposingLock);

            var exceptionAggregator = new ExceptionAggregator(Logger, "Failure to dispose landlord");

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
                            if (Logger.IsInfoEnabled)
                                Logger.Info("Failure in deferred disposal of a database", e);
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
                if (Logger.IsInfoEnabled)
                    Logger.Info("Failed to dispose resource semaphore", e);
            }

            exceptionAggregator.ThrowIfNeeded();
        }

        public void UnloadResource(string resourceName, TimeSpan? skipIfActiveInDuration, Func<TResource, bool> shouldSkip = null)
        {
            DateTime time;
            Task<TResource> resourceTask;
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
            {
                throw new InvalidOperationException($"Couldn't modify '{resourceName}' while it is loading, current status {resourceTaskStatus}");
            }

            // will never wait, we checked that we already run to completion here
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
                if (Logger.IsInfoEnabled)
                    Logger.Info("Could not dispose database: " + resourceName, e);
            }

            LastRecentlyUsed.TryRemove(resourceName, out time);
            ResourcesStoresCache.TryRemove(resourceName, out resourceTask);
        }

        public void UnloadAndLock(string resourceName, Action actionToTake)
        {
            if (Locks.TryAdd(resourceName) == false)
                throw new InvalidOperationException(resourceName + "' is currently locked and cannot be accessed");
            Interlocked.Increment(ref HasLocks);
            try
            {
                UnloadResource(resourceName, null);
                actionToTake();
            }
            finally
            {
                Locks.TryRemove(resourceName);
                Interlocked.Decrement(ref HasLocks);
            }
        }

        public abstract DateTime LastWork(TResource resource);

    }
}