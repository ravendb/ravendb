using System;
using System.Linq;
using System.Threading;
using Raven.Abstractions.Data;
using Raven.Database.Util;

namespace Raven.Database.Server.Connections
{
	public class DocumentsConnectionState
	{
		private readonly Action<object> enqueue;

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

		private int watchAllDocuments;
		private int watchAllIndexes;
		private int watchAllTransformers;
		private int watchAllReplicationConflicts;

		public DocumentsConnectionState(Action<object> enqueue)
		{
			this.enqueue = enqueue;
		}

		public object DebugStatus
		{
			get
			{
				return new
				{
					WatchAllDocuments = watchAllDocuments > 0,
					WatchAllIndexes = watchAllIndexes > 0,
					WatchAllTransformers = watchAllTransformers > 0,
					WatchAllReplicationConflicts = watchAllReplicationConflicts > 0,
					WatchedIndexes = matchingIndexes.ToArray(),
					WatchedDocuments = matchingDocuments.ToArray(),
					WatchedDocumentPrefixes = matchingDocumentPrefixes.ToArray(),
					WatchedDocumentsInCollection = matchingDocumentsInCollection.ToArray(),
					WatchedDocumentsOfType = matchingDocumentsOfType.ToArray(),
					WatchedBulkInserts = matchingBulkInserts.ToArray(),
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

		public void WatchAllIndexes()
		{
			Interlocked.Increment(ref watchAllIndexes);
		}

		public void UnwatchAllIndexes()
		{
			Interlocked.Decrement(ref watchAllIndexes);
		}

		public void WatchTransformers()
		{
			Interlocked.Increment(ref watchAllTransformers);
		}

		public void UnwatchTransformers()
		{
			Interlocked.Decrement(ref watchAllTransformers);
		}

		public void WatchDocument(string name)
		{
			matchingDocuments.TryAdd(name);
		}

		public void UnwatchDocument(string name)
		{
			matchingDocuments.TryRemove(name);
		}

		public void WatchAllDocuments()
		{
			Interlocked.Increment(ref watchAllDocuments);
		}

		public void UnwatchAllDocuments()
		{
			Interlocked.Decrement(ref watchAllDocuments);
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

		public void WatchBulkInsert(string operationId)
		{
			matchingBulkInserts.TryAdd(operationId);
		}

		public void UnwatchBulkInsert(string operationId)
		{
			matchingBulkInserts.TryRemove(operationId);
		}

		public void Send(BulkInsertChangeNotification bulkInsertChangeNotification)
		{
			if (!matchingBulkInserts.Contains(string.Empty) &&
			    !matchingBulkInserts.Contains(bulkInsertChangeNotification.OperationId.ToString()))
				return;
			enqueue(new { Value = bulkInsertChangeNotification, Type = "BulkInsertChangeNotification" });
		}

		public void Send(DocumentChangeNotification documentChangeNotification)
		{
			var value = new { Value = documentChangeNotification, Type = "DocumentChangeNotification" };
			if (watchAllDocuments > 0)
			{
				enqueue(value);
				return;
			}

			if (documentChangeNotification.Id != null && matchingDocuments.Contains(documentChangeNotification.Id))
			{
				enqueue(value);
				return;
			}

			var hasPrefix = documentChangeNotification.Id != null && matchingDocumentPrefixes
				.Any(x => documentChangeNotification.Id.StartsWith(x, StringComparison.InvariantCultureIgnoreCase));

			if (hasPrefix)
			{
				enqueue(value);
				return;
			}

			var hasCollection = documentChangeNotification.CollectionName != null && matchingDocumentsInCollection
				.Any(x => string.Equals(x, documentChangeNotification.CollectionName, StringComparison.InvariantCultureIgnoreCase));

			if (hasCollection)
			{
				enqueue(value);
				return;
			}

			var hasType = documentChangeNotification.TypeName != null && matchingDocumentsOfType
				.Any(x => string.Equals(x, documentChangeNotification.TypeName, StringComparison.InvariantCultureIgnoreCase));

			if (hasType)
			{
				enqueue(value);
				return;
			}

			if (documentChangeNotification.Id != null || documentChangeNotification.CollectionName != null || documentChangeNotification.TypeName != null)
			{
				return;
			}

			enqueue(value);
		}

		public void Send(IndexChangeNotification indexChangeNotification)
		{
			if (watchAllIndexes > 0)
			{
				enqueue(new { Value = indexChangeNotification, Type = "IndexChangeNotification" });
				return;
			}

			if (matchingIndexes.Contains(indexChangeNotification.Name) == false)
				return;

			enqueue(new { Value = indexChangeNotification, Type = "IndexChangeNotification" });
		}

		public void Send(TransformerChangeNotification transformerChangeNotification)
		{
			if (watchAllTransformers > 0)
			{
				enqueue(new { Value = transformerChangeNotification, Type = "TransformerChangeNotification" });
			}
		}

		public void Send(ReplicationConflictNotification replicationConflictNotification)
		{
			if (watchAllReplicationConflicts <= 0)
			{
				return;
			}

			enqueue(new { Value = replicationConflictNotification, Type = "ReplicationConflictNotification" });
		}
	}
}