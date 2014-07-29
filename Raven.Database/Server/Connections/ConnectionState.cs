using System;
using System.Linq;
using System.Threading;
using Raven.Abstractions.Data;
using Raven.Database.Util;
using Raven.Abstractions.FileSystem;
using Raven.Abstractions.FileSystem.Notifications;

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

        private readonly ConcurrentSet<string> matchingDbLogs =
            new ConcurrentSet<string>(StringComparer.InvariantCultureIgnoreCase);
        private readonly ConcurrentSet<string> matchingFsLogs =
            new ConcurrentSet<string>(StringComparer.InvariantCultureIgnoreCase);
        private readonly ConcurrentSet<string> matchingCountersLogs =
            new ConcurrentSet<string>(StringComparer.InvariantCultureIgnoreCase);
        
		private IEventsTransport eventsTransport;

		private int watchAllDocuments;
		private int watchAllIndexes;
	    private int watchAllTransformers;
		private int watchAllReplicationConflicts;
		private int watchCancellations;
		private int watchConfig;
		private int watchConflicts;
		private int watchSync;
        private int watchAdminLog;

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
                    WatchAllTransformers = watchAllTransformers > 0,
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

	    public void WatchDBLog(string dbName)
	    {
	        matchingDbLogs.TryAdd(dbName);
	    }
        public void UnwatchDBLog(string dbName)
        {
            matchingDbLogs.TryRemove(dbName);
        }

        public void WatchAdminLog()
        {
            Interlocked.Increment(ref watchAdminLog);
        }
        public void UnwatchAdminLog()
        {
            Interlocked.Decrement(ref watchAdminLog);
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

        public void WatchTransformers()
        {
            Interlocked.Increment(ref watchAllTransformers);
        }

        public void UnwatchTransformers()
        {
            Interlocked.Decrement(ref watchAllTransformers);
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
		    if (!matchingBulkInserts.Contains(string.Empty) && 
                !matchingBulkInserts.Contains(bulkInsertChangeNotification.OperationId.ToString())) 
                return;
		    Enqueue(new { Value = bulkInsertChangeNotification, Type = "BulkInsertChangeNotification" });
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

        public void Send(LogNotification logNotification)
        {
            var value = new { Value = logNotification, Type = "LogNotification" };
            if (watchAdminLog > 0)
            {
                Enqueue(value);
                return;
            }

            if (logNotification.TenantType == LogTenantType.Database && 
                matchingDbLogs.Any(x=>  string.Equals(x,logNotification.TenantName,StringComparison.InvariantCultureIgnoreCase)))
            {
                Enqueue(value);
                return;
            }
            if (logNotification.TenantType == LogTenantType.Filesystem && 
                matchingFsLogs.Any(x => string.Equals(x, logNotification.TenantName, StringComparison.InvariantCultureIgnoreCase)))
            {
                Enqueue(value);
                return;
            }
            if (logNotification.TenantType == LogTenantType.CounterStorage && 
                matchingCountersLogs.Any(x => string.Equals(x, logNotification.TenantName, StringComparison.InvariantCultureIgnoreCase)))
            {
                Enqueue(value);
            }
        }

		public void Send(IndexChangeNotification indexChangeNotification)
		{
		    if (watchAllIndexes > 0)
			{
				Enqueue(new { Value = indexChangeNotification, Type = "IndexChangeNotification" });
				return;
			}

			if (matchingIndexes.Contains(indexChangeNotification.Name) == false)
				return;

			Enqueue(new { Value = indexChangeNotification, Type = "IndexChangeNotification" });
		}

        public void Send(TransformerChangeNotification transformerChangeNotification)
        {
            if (watchAllTransformers > 0)
            {
                Enqueue(new { Value = transformerChangeNotification, Type = "TransformerChangeNotification" });
            }
        }

		public void Send(ReplicationConflictNotification replicationConflictNotification)
		{
		    if (watchAllReplicationConflicts <= 0)
			{
				return;
			}

			Enqueue(new { Value = replicationConflictNotification, Type = "ReplicationConflictNotification" });
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
			if (notification is FileChangeNotification &&
				matchingFolders.Any(
					f => ((FileChangeNotification)notification).File.StartsWith(f, StringComparison.InvariantCultureIgnoreCase)))
			{
				return true;
			}

			if (notification is ConfigurationChangeNotification && watchConfig > 0)
			{
				return true;
			}

			if (notification is ConflictNotification && watchConflicts > 0)
			{
				return true;
			}

			if (notification is SynchronizationUpdateNotification && watchSync > 0)
			{
				return true;
			}

			if (notification is CancellationNotification && watchCancellations > 0)
			{
				return true;
			}

			return false;
		}

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