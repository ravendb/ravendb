using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using NLog;
using Raven.Abstractions.Extensions;
using Raven.Client.RavenFS;

namespace Raven.Database.Server.RavenFS.Synchronization
{
	public class SynchronizationQueue
	{
		private static readonly Logger Log = LogManager.GetCurrentClassLogger();

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

        public int NumberOfActiveSynchronizationTasksFor(SynchronizationDestination destination)
		{
			return
				activeSynchronizations.GetOrAdd(destination.FileSystemUrl, new ConcurrentDictionary<string, SynchronizationWorkItem>()).Count;
		}

        public void EnqueueSynchronization(SynchronizationDestination destination, SynchronizationWorkItem workItem)
		{
			pendingRemoveLocks.GetOrAdd(destination.FileSystemUrl, new ReaderWriterLockSlim()).EnterUpgradeableReadLock();

			try
			{
				var pendingForDestination = pendingSynchronizations.GetOrAdd(destination.FileSystemUrl,
																			 new ConcurrentQueue<SynchronizationWorkItem>());

				// if delete work is enqueued and there are other synchronization works for a given file then remove them from a queue
				if (workItem.SynchronizationType == SynchronizationType.Delete &&
					pendingForDestination.Any(
						x => x.FileName == workItem.FileName && x.SynchronizationType != SynchronizationType.Delete))
				{
					pendingRemoveLocks.GetOrAdd(destination.FileSystemUrl, new ReaderWriterLockSlim()).EnterWriteLock();

					try
					{
						var modifiedQueue = new ConcurrentQueue<SynchronizationWorkItem>();

						foreach (var pendingWork in pendingForDestination)
						{
							if (pendingWork.FileName != workItem.FileName)
								modifiedQueue.Enqueue(pendingWork);
						}

						modifiedQueue.Enqueue(workItem);

						pendingForDestination = pendingSynchronizations.AddOrUpdate(destination.FileSystemUrl, modifiedQueue,
																					(key, value) => modifiedQueue);
					}
					finally
					{
						pendingRemoveLocks.GetOrAdd(destination.FileSystemUrl, new ReaderWriterLockSlim()).ExitWriteLock();
					}
				}

				foreach (var pendingWork in pendingForDestination)
				{
					// if there is a file in pending synchronizations do not add it again
					if (pendingWork.Equals(workItem))
					{
						Log.Debug("{0} for a file {1} and a destination {2} was already existed in a pending queue",
								  workItem.GetType().Name, workItem.FileName, destination);
						return;
					}

					// if there is a work for a file of the same type but with lower file ETag just refresh existing work metadata and do not enqueue again
					if (pendingWork.FileName == workItem.FileName &&
						pendingWork.SynchronizationType == workItem.SynchronizationType &&
						Buffers.Compare(workItem.FileETag.ToByteArray(), pendingWork.FileETag.ToByteArray()) > 0)
					{
						pendingWork.RefreshMetadata();
						Log.Debug(
							"{0} for a file {1} and a destination {2} was already existed in a pending queue but with older ETag, it's metadata has been refreshed",
							workItem.GetType().Name, workItem.FileName, destination);
						return;
					}
				}

				var activeForDestination = activeSynchronizations.GetOrAdd(destination.FileSystemUrl,
																		   new ConcurrentDictionary<string, SynchronizationWorkItem>
																			   ());

				// if there is a work in an active synchronizations do not add it again
				if (activeForDestination.ContainsKey(workItem.FileName) && activeForDestination[workItem.FileName].Equals(workItem))
				{
					Log.Debug("{0} for a file {1} and a destination {2} was already existed in an active queue",
							  workItem.GetType().Name, workItem.FileName, destination);
					return;
				}

				pendingForDestination.Enqueue(workItem);
				Log.Debug("{0} for a file {1} and a destination {2} was enqueued", workItem.GetType().Name, workItem.FileName,
						  destination);
			}
			finally
			{
				pendingRemoveLocks.GetOrAdd(destination.FileSystemUrl, new ReaderWriterLockSlim()).ExitUpgradeableReadLock();
			}
		}

        public bool TryDequePendingSynchronization(SynchronizationDestination destination, out SynchronizationWorkItem workItem)
		{
			var readerWriterLockSlim = pendingRemoveLocks.GetOrAdd(destination.FileSystemUrl, new ReaderWriterLockSlim());
			readerWriterLockSlim.EnterReadLock();
			try
			{
				ConcurrentQueue<SynchronizationWorkItem> pendingForDestination;
				if (pendingSynchronizations.TryGetValue(destination.FileSystemUrl, out pendingForDestination) == false)
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

        public bool IsDifferentWorkForTheSameFileBeingPerformed(SynchronizationWorkItem work, SynchronizationDestination destination)
		{
			ConcurrentDictionary<string, SynchronizationWorkItem> activeForDestination;
			if (!activeSynchronizations.TryGetValue(destination.FileSystemUrl, out activeForDestination))
				return false;

			SynchronizationWorkItem activeWork;
			return activeForDestination.TryGetValue(work.FileName, out activeWork) && !activeWork.Equals(work);
		}

		public void SynchronizationStarted(SynchronizationWorkItem work, SynchronizationDestination destination)
		{
			var activeForDestination = activeSynchronizations.GetOrAdd(destination.FileSystemUrl,
																	   new ConcurrentDictionary<string, SynchronizationWorkItem>());

			if (activeForDestination.TryAdd(work.FileName, work))
			{
				Log.Debug("File '{0}' with ETag {1} was added to an active synchronization queue for a destination {2}",
						  work.FileName,
						  work.FileETag, destination);
			}
		}

        public void SynchronizationFinished(SynchronizationWorkItem work, SynchronizationDestination destination)
		{
			ConcurrentDictionary<string, SynchronizationWorkItem> activeDestinationTasks;

			if (activeSynchronizations.TryGetValue(destination.FileSystemUrl, out activeDestinationTasks) == false)
			{
				Log.Warn("Could not get an active synchronization queue for {0}", destination);
				return;
			}

			SynchronizationWorkItem removingItem;
			if (activeDestinationTasks.TryRemove(work.FileName, out removingItem))
			{
				Log.Debug("File '{0}' with ETag {1} was removed from an active synchronization queue for a destination {2}",
						  work.FileName,
						  work.FileETag, destination);
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
