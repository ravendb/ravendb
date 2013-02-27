using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Raven.Abstractions.Data;
using Raven.Database.Util;

namespace Raven.Database.Server.Connections
{
	public class ConnectionState
	{
		private readonly ConcurrentSet<string> matchingIndexes =
			new ConcurrentSet<string>(StringComparer.OrdinalIgnoreCase);
		private readonly ConcurrentSet<string> matchingDocuments =
			new ConcurrentSet<string>(StringComparer.OrdinalIgnoreCase);

		private readonly ConcurrentSet<string> matchingDocumentPrefixes =
			new ConcurrentSet<string>(StringComparer.OrdinalIgnoreCase);

		private readonly ConcurrentQueue<object> pendingMessages = new ConcurrentQueue<object>();

		private EventsTransport eventsTransport;

		private int watchAllDocuments;
		private int watchAllIndexes;
		private int watchAllReplicationConflicts;

		public ConnectionState(EventsTransport eventsTransport)
		{
			this.eventsTransport = eventsTransport;
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

		public void Send(DocumentChangeNotification documentChangeNotification)
		{
			var value = new { Value = documentChangeNotification, Type = "DocumentChangeNotification" };
			if (watchAllDocuments > 0)
			{
				Enqueue(value);
				return;
			}

			if (documentChangeNotification.Id != null)
			{
				if (matchingDocuments.Contains(documentChangeNotification.Id))
				{
					Enqueue(value);
					return;
				}

				var hasPrefix = matchingDocumentPrefixes.Any(
						x => documentChangeNotification.Id.StartsWith(x, StringComparison.OrdinalIgnoreCase));
				if (hasPrefix == false)
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

			if (watchAllReplicationConflicts > 0)
			{
				Enqueue(value);
				return;
			}

			Enqueue(value);
		}

		private void Enqueue(object msg)
		{
			if (eventsTransport == null || eventsTransport.Connected == false)
			{
				pendingMessages.Enqueue(msg);
				return;
			}

			eventsTransport.SendAsync(msg)
				.ContinueWith(task =>
								{
									if (task.IsFaulted == false)
										return;
									pendingMessages.Enqueue(msg);
								});
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

		public void Reconnect(EventsTransport transport)
		{
			eventsTransport = transport;
			var items = new List<object>();
			object result;
			while (pendingMessages.TryDequeue(out result))
			{
				items.Add(result);
			}

			eventsTransport.SendManyAsync(items)
				.ContinueWith(task =>
								{
									if (task.IsFaulted == false)
										return;
									foreach (var item in items)
									{
										pendingMessages.Enqueue(item);
									}
								});
		}

		public void Disconnect()
		{
			if (eventsTransport != null)
				eventsTransport.Disconnect();
		}

		public void WatchAllReplicationConflicts()
		{
			Interlocked.Increment(ref watchAllReplicationConflicts);
		}

		public void UnwatchAllReplicationConflicts()
		{
			Interlocked.Decrement(ref watchAllReplicationConflicts);
		}
	}
}