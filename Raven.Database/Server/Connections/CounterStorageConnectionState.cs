using System;
using System.Linq;
using Raven.Abstractions.Counters.Notifications;
using Raven.Database.Util;

namespace Raven.Database.Server.Connections
{
	public class CounterStorageConnectionState
	{
		private readonly Action<object> enqueue;

		private readonly ConcurrentSet<string> matchingChanges =
			new ConcurrentSet<string>(StringComparer.InvariantCultureIgnoreCase);

		private readonly ConcurrentSet<string> matchingLocalChanges =
			new ConcurrentSet<string>(StringComparer.InvariantCultureIgnoreCase);

		private readonly ConcurrentSet<string> matchingReplicationChanges =
			new ConcurrentSet<string>(StringComparer.InvariantCultureIgnoreCase);

		private readonly ConcurrentSet<string> matchingBulkOperations =
			new ConcurrentSet<string>(StringComparer.InvariantCultureIgnoreCase);

		public object DebugStatus
		{
			get
			{
				return new
				{
					WatchedChanges = matchingChanges.ToArray(),
					WatchedLocalChanges = matchingLocalChanges.ToArray(),
					WatchedReplicationChanges = matchingReplicationChanges.ToArray(),
					WatchedBulkOperationsChanges = matchingBulkOperations.ToArray()
				};
			}
		}

		public CounterStorageConnectionState(Action<object> enqueue)
		{
			this.enqueue = enqueue;
		}

		public void WatchChange(string name)
		{
			matchingChanges.TryAdd(name);
		}

		public void UnwatchChange(string name)
		{
			matchingChanges.TryRemove(name);
		}

		public void WatchLocalChange(string name)
		{
			matchingLocalChanges.TryRemove(name);
		}

		public void UnwatchLocalChange(string name)
		{
			matchingLocalChanges.TryRemove(name);
		}

		public void WatchReplicationChange(string name)
		{
			matchingReplicationChanges.TryAdd(name);
		}

		public void UnwatchReplicationChange(string name)
		{
			matchingReplicationChanges.TryAdd(name);
		}

		public void WatchCounterBulkOperation(string operationId)
		{
			matchingBulkOperations.TryAdd(operationId);
		}

		public void UnwatchCounterBulkOperation(string operationId)
		{
			matchingBulkOperations.TryAdd(operationId);
		}

		public void Send(CounterChangeNotification notification)
		{
			var value = new {Value = notification, Type = notification.GetType().Name};
			var counterPrefix = string.Concat(notification.GroupName, "/", notification.CounterName);

			var hasChanges = (notification.Type.HasFlag(CounterChangeType.All) && matchingChanges.Contains(counterPrefix));
			if (hasChanges)
			{
				enqueue(value);
			}

			var hasLocalChanges = (notification.Type.HasFlag(CounterChangeType.Local) && matchingLocalChanges.Contains(counterPrefix));
			if (hasLocalChanges)
			{
				enqueue(value);
			}

			var hasReplicationChanges = (notification.Type.HasFlag(CounterChangeType.Replication) && matchingReplicationChanges.Contains(counterPrefix));
			if (hasReplicationChanges)
			{
				enqueue(value);
			}
		}

		public void Send(CounterBulkOperationNotification notification)
		{
			if (matchingBulkOperations.Contains(string.Empty) == false &&
				matchingBulkOperations.Contains(notification.OperationId.ToString()) == false)
				return;

			var value = new { Value = notification, Type = notification.GetType().Name };
			enqueue(value);
		}
	}
}