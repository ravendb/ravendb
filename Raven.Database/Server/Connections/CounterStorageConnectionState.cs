using System;
using System.Linq;
using Raven.Abstractions.Counters.Notifications;
using Raven.Database.Util;

namespace Raven.Database.Server.Connections
{
	public class CounterStorageConnectionState
	{
		private readonly Action<object> enqueue;

		private readonly ConcurrentSet<string> matchingChanges = new ConcurrentSet<string>(StringComparer.InvariantCultureIgnoreCase);
		private readonly ConcurrentSet<string> matchingLocalChanges = new ConcurrentSet<string>(StringComparer.InvariantCultureIgnoreCase);
		private readonly ConcurrentSet<string> matchingReplicationChanges = new ConcurrentSet<string>(StringComparer.InvariantCultureIgnoreCase);
		private readonly ConcurrentSet<string> matchingBulkOperations = new ConcurrentSet<string>(StringComparer.InvariantCultureIgnoreCase);
		private readonly string localChangeNotificationType = typeof(LocalChangeNotification).Name;
		private readonly string changeNotificationType = typeof(ChangeNotification).Name;
		private readonly string replicaitonChangeNotificationType = typeof(ReplicationChangeNotification).Name;

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
			matchingLocalChanges.TryAdd(name);
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
			matchingReplicationChanges.TryRemove(name);
		}

		public void WatchCounterBulkOperation(string operationId)
		{
			matchingBulkOperations.TryAdd(operationId);
		}

		public void UnwatchCounterBulkOperation(string operationId)
		{
			matchingBulkOperations.TryRemove(operationId);
		}

		public void Send(LocalChangeNotification notification)
		{
			var counterPrefix = GetCounterPrefix(notification.GroupName, notification.CounterName);

			if (matchingLocalChanges.Contains(counterPrefix))
			{
				var value = new { Value = notification, Type = localChangeNotificationType };
				enqueue(value);
			}

			if (matchingChanges.Contains(counterPrefix))
			{
				var value = new { Value = notification, Type = changeNotificationType };
				enqueue(value);
			}
		}

		public void Send(ReplicationChangeNotification notification)
		{
			var counterPrefix = GetCounterPrefix(notification.GroupName, notification.CounterName);
			
			if (matchingReplicationChanges.Contains(counterPrefix))
			{
				var value = new  { Value = notification, Type = replicaitonChangeNotificationType };
				enqueue(value);
			}

			if (matchingChanges.Contains(counterPrefix))
			{
				var value = new { Value = notification, Type = changeNotificationType };
				enqueue(value);
			}
		}

		private static string GetCounterPrefix(string groupName, string counterName)
		{
			return string.Concat(groupName, "/", counterName);
		}

		public void Send(BulkOperationNotification notification)
		{
			if (matchingBulkOperations.Contains(string.Empty) == false &&
				matchingBulkOperations.Contains(notification.OperationId.ToString()) == false)
				return;

			var value = new { Value = notification, Type = notification.GetType().Name };
			enqueue(value);
		}
	}
}