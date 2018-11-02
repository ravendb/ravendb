using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.FileSystem;
using Raven.Abstractions.Logging;

namespace Raven.Database.FileSystem.Synchronization
{
    public class SynchronizationQueue
    {
        private static readonly ILog Log = LogManager.GetCurrentClassLogger();

        private readonly ConcurrentDictionary<string, ConcurrentDictionary<string, SynchronizationWorkItem>>
            activeSynchronizations =
                new ConcurrentDictionary<string, ConcurrentDictionary<string, SynchronizationWorkItem>>();

        private readonly ConcurrentDictionary<string, ReaderWriterLockSlim> pendingRemoveLocks =
            new ConcurrentDictionary<string, ReaderWriterLockSlim>();

        private readonly ConcurrentDictionary<string, ConcurrentQueue<SynchronizationWorkItem>> pendingSynchronizations =
            new ConcurrentDictionary<string, ConcurrentQueue<SynchronizationWorkItem>>();

        public IEnumerable<SynchronizationDetails> Pending
        {
            get
            {
                return from destinationPending in pendingSynchronizations
                       from pendingFile in destinationPending.Value
                       select new SynchronizationDetails
                       {
                           DestinationUrl = destinationPending.Key,
                           FileName = pendingFile.FileName,
                           Type = pendingFile.SynchronizationType,
                           FileETag = pendingFile.FileETag
                       };
            }
        }

        public IEnumerable<SynchronizationDetails> Active
        {
            get
            {
                return from destinationActive in activeSynchronizations
                       from activeFile in destinationActive.Value
                       select new SynchronizationDetails
                       {
                           DestinationUrl = destinationActive.Key,
                           FileName = activeFile.Key,
                           Type = activeFile.Value.SynchronizationType,
                           FileETag = activeFile.Value.FileETag
                       };
            }
        }

        public int GetTotalPendingTasks()
        {
            return pendingSynchronizations.Sum(queue => queue.Value.Count);
        }

        public int GetTotalActiveTasks()
        {
            return activeSynchronizations.Sum(queue => queue.Value.Count);
        }

        public int NumberOfActiveSynchronizationsFor(string destinationFileSystemUrl)
        {
            return activeSynchronizations.GetOrAdd(destinationFileSystemUrl, new ConcurrentDictionary<string, SynchronizationWorkItem>())
                                         .Count;
        }

        public int NumberOfPendingSynchronizationsFor(string destinationFileSystemUrl)
        {
            return pendingSynchronizations.GetOrAdd(destinationFileSystemUrl, new ConcurrentQueue<SynchronizationWorkItem>())
                                         .Count;
        }

        public bool EnqueueSynchronization(string destinationFileSystemUrl, SynchronizationWorkItem workItem)
        {
            pendingRemoveLocks.GetOrAdd(destinationFileSystemUrl, new ReaderWriterLockSlim())
                              .EnterUpgradeableReadLock();

            try
            {
                var pendingForDestination = pendingSynchronizations.GetOrAdd(destinationFileSystemUrl,
                                                                             new ConcurrentQueue<SynchronizationWorkItem>());

                // if delete work is enqueued and there are other synchronization works for a given file then remove them from a queue
                if (workItem.SynchronizationType == SynchronizationType.Delete &&
                    pendingForDestination.Any(x => x.FileName == workItem.FileName && x.SynchronizationType != SynchronizationType.Delete))
                {
                    pendingRemoveLocks.GetOrAdd(destinationFileSystemUrl, new ReaderWriterLockSlim()).EnterWriteLock();

                    try
                    {
                        var modifiedQueue = new ConcurrentQueue<SynchronizationWorkItem>();

                        foreach (var pendingWork in pendingForDestination)
                        {
                            if (pendingWork.FileName != workItem.FileName)
                                modifiedQueue.Enqueue(pendingWork);
                        }

                        modifiedQueue.Enqueue(workItem);

                        pendingForDestination = pendingSynchronizations.AddOrUpdate(destinationFileSystemUrl, modifiedQueue,
                                                                                    (key, value) => modifiedQueue);
                    }
                    finally
                    {
                        pendingRemoveLocks.GetOrAdd(destinationFileSystemUrl, new ReaderWriterLockSlim()).ExitWriteLock();
                    }
                }

                foreach (var pendingWork in pendingForDestination)
                {
                    // if there is a file in pending synchronizations do not add it again
                    if (pendingWork.Equals(workItem))
                    {
                        if (Log.IsDebugEnabled)
                            Log.Debug("{0} for a file {1} and a destination {2} was already existed in a pending queue",
                                  workItem.GetType().Name, workItem.FileName, destinationFileSystemUrl);
                        return false;
                    }
                }

                var activeForDestination = activeSynchronizations.GetOrAdd(destinationFileSystemUrl,
                                                                           new ConcurrentDictionary<string, SynchronizationWorkItem>
                                                                               ());

                // if there is a work in an active synchronizations do not add it again
                if (activeForDestination.ContainsKey(workItem.FileName) && activeForDestination[workItem.FileName].Equals(workItem))
                {
                    if (Log.IsDebugEnabled)
                        Log.Debug("{0} for a file {1} and a destination {2} was already existed in an active queue",
                              workItem.GetType().Name, workItem.FileName, destinationFileSystemUrl);
                    return false;
                }

                pendingForDestination.Enqueue(workItem);
                if (Log.IsDebugEnabled)
                    Log.Debug("{0} for a file {1} and a destination {2} was enqueued", workItem.GetType().Name, workItem.FileName,
                          destinationFileSystemUrl);
            }
            finally
            {
                pendingRemoveLocks.GetOrAdd(destinationFileSystemUrl, new ReaderWriterLockSlim()).ExitUpgradeableReadLock();
            }

            return true;
        }

        public bool TryDequePending(string destinationFileSystemUrl, out SynchronizationWorkItem workItem)
        {
            var readerWriterLockSlim = pendingRemoveLocks.GetOrAdd(destinationFileSystemUrl, new ReaderWriterLockSlim());
            readerWriterLockSlim.EnterReadLock();
            try
            {
                ConcurrentQueue<SynchronizationWorkItem> pendingForDestination;
                if (pendingSynchronizations.TryGetValue(destinationFileSystemUrl, out pendingForDestination) == false)
                {
                    workItem = null;
                    return false;
                }

                return pendingForDestination.TryDequeue(out workItem);
            }
            finally
            {
                readerWriterLockSlim.ExitReadLock();
            }
        }

        public bool IsDifferentWorkForTheSameFileBeingPerformed(SynchronizationWorkItem work, string destinationFileSystemUrl)
        {
            ConcurrentDictionary<string, SynchronizationWorkItem> activeForDestination;
            if (!activeSynchronizations.TryGetValue(destinationFileSystemUrl, out activeForDestination))
                return false;

            SynchronizationWorkItem activeWork;
            return activeForDestination.TryGetValue(work.FileName, out activeWork) && !activeWork.Equals(work);
        }

        public void SynchronizationStarted(SynchronizationWorkItem work, string destinationFileSystemUrl)
        {
            var activeForDestination = activeSynchronizations.GetOrAdd(destinationFileSystemUrl,
                                                                       new ConcurrentDictionary<string, SynchronizationWorkItem>());

            if (activeForDestination.TryAdd(work.FileName, work))
            {
                if (Log.IsDebugEnabled)
                    Log.Debug("File '{0}' with ETag {1} was added to an active synchronization queue for a destination {2}",
                          work.FileName,
                          work.FileETag, destinationFileSystemUrl);
            }
        }

        public void SynchronizationFinished(SynchronizationWorkItem work, string destinationFileSystemUrl)
        {
            ConcurrentDictionary<string, SynchronizationWorkItem> activeDestinationTasks;

            if (activeSynchronizations.TryGetValue(destinationFileSystemUrl, out activeDestinationTasks) == false)
            {
                Log.Warn("Could not get an active synchronization queue for {0}", destinationFileSystemUrl);
                return;
            }

            SynchronizationWorkItem removingItem;
            if (activeDestinationTasks.TryRemove(work.FileName, out removingItem))
            {
                if (Log.IsDebugEnabled)
                    Log.Debug("File '{0}' with ETag {1} was removed from an active synchronization queue for a destination {2}",
                          work.FileName,
                          work.FileETag, destinationFileSystemUrl);
            }
        }

        public void CancelActiveSynchronizations(string fileName)
        {
            foreach (var destSync in activeSynchronizations)
            {
                foreach (var activeSynchronization in destSync.Value)
                {
                    if (activeSynchronization.Key == fileName)
                        activeSynchronization.Value.Cancel();
                }
            }
        }
    }
}
