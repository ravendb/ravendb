using System;
using System.Linq;
using System.Threading;
using Raven.Abstractions.Data;
using Raven.Client.RavenFS;
using Raven.Database.Util;

namespace Raven.Database.Server.Connections
{
	public class ConnectionState
	{
		private readonly ConcurrentSet<string> matchingIndexes =
			new ConcurrentSet<string>(StringComparer.InvariantCultureIgnoreCase);
		private readonly ConcurrentSet<string> matchingDocuments =
			new ConcurrentSet<string>(StringComparer.InvariantCultureIgnoreCase);

		private readonly ConcurrentSet<string> matchingDocumentPrefixes =
			new ConcurrentSet<string>(StringComparer.InvariantCultureIgnoreCase);

		private readonly ConcurrentSet<string> matchingDocumentsInCollection =
			new ConcurrentSet<string>(StringComparer.InvariantCultureIgnoreCase);

		private readonly ConcurrentSet<string> matchingDocumentsOfType =
			new ConcurrentSet<string>(StringComparer.InvariantCultureIgnoreCase);

		private readonly ConcurrentSet<string> matchingBulkInserts =
			new ConcurrentSet<string>(StringComparer.InvariantCultureIgnoreCase);

		private readonly ConcurrentSet<string> matchingFolders =
			new ConcurrentSet<string>(StringComparer.InvariantCultureIgnoreCase);

		private IEventsTransport eventsTransport;

		private int watchAllDocuments;
		private int watchAllIndexes;
		private int watchAllReplicationConflicts;
		private int watchCancellations;
		private int watchConfig;
		private int watchConflicts;
		private int watchSync;

		public ConnectionState(IEventsTransport eventsTransport)
		{
			this.eventsTransport = eventsTransport;
		}

		public object DebugStatus
		{
			get
			{
				return new
				{
					eventsTransport.Id,
					eventsTransport.Connected,
					WatchAllDocuments = watchAllDocuments > 0,
					WatchAllIndexes = watchAllIndexes > 0,
					WatchConfig = watchConfig > 0,
					WatchConflicts = watchConflicts > 0,
					WatchSync = watchSync > 0,
					WatchCancellations = watchCancellations > 0,
					WatchDocumentPrefixes = matchingDocumentPrefixes.ToArray(),
					WatchDocumentsInCollection = matchingDocumentsInCollection.ToArray(),
					WatchIndexes = matchingIndexes.ToArray(),
					WatchDocuments = matchingDocuments.ToArray(),
					WatchedFolders = matchingFolders.ToArray()
				};
			}
		}

		public void WatchIndex(string name)
		{
			matchingIndexes.TryAdd(name);
		}

		public void UnwatchIndex(string name)
		{
			matchingIndexes.TryRemove(name);
		}

		public void WatchBulkInsert(string operationId)
		{
			matchingBulkInserts.TryAdd(operationId);
		}

		public void UnwatchBulkInsert(string operationId)
		{
			matchingBulkInserts.TryRemove(operationId);
		}

		public void WatchAllIndexes()
		{
			Interlocked.Increment(ref watchAllIndexes);
		}

		public void UnwatchAllIndexes()
		{
			Interlocked.Decrement(ref watchAllIndexes);
		}

		public void WatchConflicts()
		{
			Interlocked.Increment(ref watchConflicts);
		}

		public void UnwatchConflicts()
		{
			Interlocked.Decrement(ref watchConflicts);
		}

		public void WatchSync()
		{
			Interlocked.Increment(ref watchSync);
		}

		public void UnwatchSync()
		{
			Interlocked.Decrement(ref watchSync);
		}

		public void WatchFolder(string folder)
		{
			matchingFolders.TryAdd(folder);
		}

		public void UnwatchFolder(string folder)
		{
			matchingFolders.TryRemove(folder);
		}

		public void WatchCancellations()
		{
			Interlocked.Increment(ref watchCancellations);
		}

		public void UnwatchCancellations()
		{
			Interlocked.Decrement(ref watchCancellations);
		}

		public void WatchConfig()
		{
			Interlocked.Increment(ref watchConfig);
		}

		public void UnwatchConfig()
		{
			Interlocked.Decrement(ref watchConfig);
		}

		public void Send(BulkInsertChangeNotification bulkInsertChangeNotification)
		{
			var value = new { Value = bulkInsertChangeNotification, Type = "BulkInsertChangeNotification" };

			if (matchingBulkInserts.Contains(bulkInsertChangeNotification.OperationId.ToString()) == false)
				return;

			Enqueue(value);
		}

		public void Send(DocumentChangeNotification documentChangeNotification)
		{
			var value = new { Value = documentChangeNotification, Type = "DocumentChangeNotification" };
			if (watchAllDocuments > 0)
			{
				Enqueue(value);
				return;
			}

			if (documentChangeNotification.Id != null && matchingDocuments.Contains(documentChangeNotification.Id))
			{
				Enqueue(value);
				return;
			}

			var hasPrefix = documentChangeNotification.Id != null && matchingDocumentPrefixes
				.Any(x => documentChangeNotification.Id.StartsWith(x, StringComparison.InvariantCultureIgnoreCase));

			if (hasPrefix)
			{
				Enqueue(value);
				return;
			}

			var hasCollection = documentChangeNotification.CollectionName != null && matchingDocumentsInCollection
				.Any(x => string.Equals(x, documentChangeNotification.CollectionName, StringComparison.InvariantCultureIgnoreCase));

			if (hasCollection)
			{
				Enqueue(value);
				return;
			}

			var hasType = documentChangeNotification.TypeName != null && matchingDocumentsOfType
				.Any(x => string.Equals(x, documentChangeNotification.TypeName, StringComparison.InvariantCultureIgnoreCase));

			if (hasType)
			{
				Enqueue(value);
				return;
			}

			if (documentChangeNotification.Id != null || documentChangeNotification.CollectionName != null || documentChangeNotification.TypeName != null)
			{
				return;
			}

			Enqueue(value);
		}

		public void Send(IndexChangeNotification indexChangeNotification)
		{
			var value = new { Value = indexChangeNotification, Type = "IndexChangeNotification" };

			if (watchAllIndexes > 0)
			{
				Enqueue(value);
				return;
			}

			if (matchingIndexes.Contains(indexChangeNotification.Name) == false)
				return;

			Enqueue(value);
		}

		public void Send(ReplicationConflictNotification replicationConflictNotification)
		{
			var value = new { Value = replicationConflictNotification, Type = "ReplicationConflictNotification" };

			if (watchAllReplicationConflicts <= 0)
			{
				return;
			}

			Enqueue(value);
		}

		public void Send(Notification notification)
		{
			if (ShouldSend(notification))
			{
				var value = new { Value = notification, Type = notification.GetType().Name };

				Enqueue(value);
			}
		}

		private bool ShouldSend(Notification notification)
		{
			if (notification is FileChange &&
				matchingFolders.Any(
					f => ((FileChange)notification).File.StartsWith(f, StringComparison.InvariantCultureIgnoreCase)))
			{
				return true;
			}

			if (notification is ConfigChange && watchConfig > 0)
			{
				return true;
			}

			if (notification is ConflictNotification && watchConflicts > 0)
			{
				return true;
			}

			if (notification is SynchronizationUpdate && watchSync > 0)
			{
				return true;
			}

			if (notification is UploadFailed && watchCancellations > 0)
			{
				return true;
			}

			return false;
		}

	//	private readonly ConcurrentQueue<Notification> pendingMessages = new ConcurrentQueue<Notification>();

		//private async Task Enqueue(Notification msg)
		//{
		//	if (eventsTransport == null || eventsTransport.Connected == false)
		//	{
		//		pendingMessages.Enqueue(msg);
		//		return;
		//	}
		//	try
		//	{
		//		eventsTransport.SendAsync(msg);
		//		pendingMessages.Enqueue(msg);
		//	}
		//	catch (Exception)
		//	{
		//	}
		//}

		private void Enqueue(object msg)
		{
			if (eventsTransport == null || eventsTransport.Connected == false)
			{
				return;
			}

			eventsTransport.SendAsync(msg);
		}

		public void WatchAllDocuments()
		{
			Interlocked.Increment(ref watchAllDocuments);
		}

		public void UnwatchAllDocuments()
		{
			Interlocked.Decrement(ref watchAllDocuments);
		}

		public void WatchDocument(string name)
		{
			matchingDocuments.TryAdd(name);
		}

		public void UnwatchDocument(string name)
		{
			matchingDocuments.TryRemove(name);
		}

		public void WatchDocumentPrefix(string name)
		{
			matchingDocumentPrefixes.TryAdd(name);
		}

		public void UnwatchDocumentPrefix(string name)
		{
			matchingDocumentPrefixes.TryRemove(name);
		}

		public void WatchDocumentInCollection(string name)
		{
			matchingDocumentsInCollection.TryAdd(name);
		}

		public void UnwatchDocumentInCollection(string name)
		{
			matchingDocumentsInCollection.TryRemove(name);
		}

		public void WatchDocumentOfType(string name)
		{
			matchingDocumentsOfType.TryAdd(name);
		}

		public void UnwatchDocumentOfType(string name)
		{
			matchingDocumentsOfType.TryRemove(name);
		}

		public void WatchAllReplicationConflicts()
		{
			Interlocked.Increment(ref watchAllReplicationConflicts);
		}

		public void UnwatchAllReplicationConflicts()
		{
			Interlocked.Decrement(ref watchAllReplicationConflicts);
		}

		public void Reconnect(IEventsTransport transport)
		{
			eventsTransport = transport;
		}

		public void Dispose()
		{
			if (eventsTransport != null)
				eventsTransport.Dispose();
		}
	}
}