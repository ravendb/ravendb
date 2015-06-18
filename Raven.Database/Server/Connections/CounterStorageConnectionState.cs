using System;
using System.Linq;
using Raven.Abstractions.Counters.Notifications;
using Raven.Database.Util;
using Raven.Abstractions.Data;

namespace Raven.Database.Server.Connections
{
	public class CounterStorageConnectionState
	{
		private readonly Action<object> enqueue;

		private readonly ConcurrentSet<string> matchingChanges = new ConcurrentSet<string>(StringComparer.InvariantCultureIgnoreCase);
		private readonly ConcurrentSet<string> matchingPrefixes = new ConcurrentSet<string>(StringComparer.InvariantCultureIgnoreCase);
		private readonly ConcurrentSet<string> matchingGroups = new ConcurrentSet<string>(StringComparer.InvariantCultureIgnoreCase);
		private readonly ConcurrentSet<string> matchingBulkOperations = new ConcurrentSet<string>(StringComparer.InvariantCultureIgnoreCase);
		private readonly string changeNotificationType = typeof(ChangeNotification).Name;
		private readonly string startingWithNotification = typeof(StartingWithNotification).Name;
		private readonly string inGroupNotificationType = typeof(InGroupNotification).Name;
		private readonly string bulkOperationNotification = typeof(BulkOperationNotification).Name;

		public object DebugStatus
		{
			get
			{
				return new
				{
					WatchedChanges = matchingChanges.ToArray(),
					WatchedLocalChanges = matchingPrefixes.ToArray(),
					WatchedReplicationChanges = matchingGroups.ToArray(),
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

		public void WatchPrefix(string name)
		{
			matchingPrefixes.TryAdd(name);
		}

		public void UnwatchPrefix(string name)
		{
			matchingPrefixes.TryRemove(name);
		}

		public void WatchCountersInGroup(string name)
		{
			matchingGroups.TryAdd(name);
		}

		public void UnwatchCountersInGroup(string name)
		{
			matchingGroups.TryRemove(name);
		}

		public void WatchCounterBulkOperation(string operationId)
		{
			matchingBulkOperations.TryAdd(operationId);
		}

		public void UnwatchCounterBulkOperation(string operationId)
		{
			matchingBulkOperations.TryRemove(operationId);
		}

		public void Send(ChangeNotification notification)
		{
			var counterPrefix = GetCounterPrefix(notification.GroupName, notification.CounterName);

			if (matchingChanges.Contains(counterPrefix))
			{
				var value = new { Value = notification, Type = changeNotificationType };
				enqueue(value);
			}

			if (matchingPrefixes.Any(prefix => counterPrefix.StartsWith(prefix)))
			{
				var value = new { Value = notification, Type = startingWithNotification };
				enqueue(value);
			}

			if (matchingGroups.Contains(notification.GroupName))
			{
				var value = new { Value = notification, Type = inGroupNotificationType };
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

			var value = new { Value = notification, Type = bulkOperationNotification };
			enqueue(value);
		}
	}
}