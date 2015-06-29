using System;
using System.Linq;
using Raven.Abstractions.TimeSeries.Notifications;
using Raven.Abstractions.Data;
using Sparrow.Collections;

namespace Raven.Database.Server.Connections
{
	public class TimeSeriesConnectionState
	{
		private readonly Action<object> enqueue;

		private readonly ConcurrentSet<string> matchingKeys = new ConcurrentSet<string>(StringComparer.InvariantCultureIgnoreCase);
		// private readonly ConcurrentSet<string> matchingRanges = new ConcurrentSet<string>(StringComparer.InvariantCultureIgnoreCase);
		private readonly ConcurrentSet<string> matchingBulkOperations = new ConcurrentSet<string>(StringComparer.InvariantCultureIgnoreCase);
		private readonly string changeNotificationType = typeof(TimeSeriesKeyNotification).Name;
		// private readonly string rangesNotificationType = typeof(TimeSeriesRangeChangeNotification).Name;
		private readonly string bulkOperationNotificationType = typeof(TimeSeriesBulkOperationNotification).Name;

		public object DebugStatus
		{
			get
			{
				return new
				{
					WatchedKeys = matchingKeys.ToArray(),
					// WatchedRanges = matchingRanges.ToArray(),
					WatchedBulkOperationsChanges = matchingBulkOperations.ToArray()
				};
			}
		}

		public TimeSeriesConnectionState(Action<object> enqueue)
		{
			this.enqueue = enqueue;
		}

		public void WatchKey(string key)
		{
			matchingKeys.TryAdd(key);
		}

		public void UnwatchChange(string key)
		{
			matchingKeys.TryRemove(key);
		}

		/*public void WatchRange(string key)
		{
			matchingRanges.TryAdd(key);
		}

		public void UnwatchRange(string name)
		{
			matchingRanges.TryRemove(name);
		}*/

		public void WatchTimeSeriesBulkOperation(string operationId)
		{
			matchingBulkOperations.TryAdd(operationId);
		}

		public void UnwatchTimeSeriesBulkOperation(string operationId)
		{
			matchingBulkOperations.TryRemove(operationId);
		}

		public void Send(TimeSeriesKeyNotification notification)
		{
			if (matchingKeys.Contains(notification.Key))
			{
				var value = new { Value = notification, Type = changeNotificationType };
				enqueue(value);
			}

			/*if (matchingRanges.Any(prefix => prefix.InRange(notification.At)))
			{
				var value = new { Value = notification, Type = rangesNotificationType };
				enqueue(value);
			}*/
		}

		public void Send(TimeSeriesBulkOperationNotification notification)
		{
			if (matchingBulkOperations.Contains(string.Empty) == false &&
				matchingBulkOperations.Contains(notification.OperationId.ToString()) == false)
				return;

			var value = new { Value = notification, Type = bulkOperationNotificationType };
			enqueue(value);
		}
	}
}