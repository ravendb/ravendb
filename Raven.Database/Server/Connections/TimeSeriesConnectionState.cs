using System;
using System.Linq;
using System.Threading;
using Raven.Abstractions.TimeSeries.Notifications;
using Sparrow.Collections;

namespace Raven.Database.Server.Connections
{
	public class TimeSeriesConnectionState
	{
		private readonly Action<object> enqueue;

		private readonly ConcurrentSet<string> matchingKeyChanges = new ConcurrentSet<string>(StringComparer.InvariantCultureIgnoreCase);
		// private readonly ConcurrentSet<string> matchingRanges = new ConcurrentSet<string>(StringComparer.InvariantCultureIgnoreCase);
		private readonly ConcurrentSet<string> matchingBulkOperations = new ConcurrentSet<string>(StringComparer.InvariantCultureIgnoreCase);
		private readonly string keyChangeNotificationType = typeof(TimeSeriesChangeNotification).Name;
		// private readonly string rangesNotificationType = typeof(TimeSeriesRangeChangeNotification).Name;
		private readonly string bulkOperationNotificationType = typeof(BulkOperationNotification).Name;
		private int watchAllTimeSeries;

		public object DebugStatus
		{
			get
			{
				return new
				{
					WatchedKeyChanges = matchingKeyChanges.ToArray(),
					// WatchedRanges = matchingRanges.ToArray(),
					WatchedBulkOperationsChanges = matchingBulkOperations.ToArray()
				};
			}
		}

		public TimeSeriesConnectionState(Action<object> enqueue)
		{
			this.enqueue = enqueue;
		}

		public void WatchAllTimeSeries()
		{
			Interlocked.Increment(ref watchAllTimeSeries);
		}

		public void UnwatchAllTimeSeries()
		{
			Interlocked.Decrement(ref watchAllTimeSeries);
		}

		public void WatchKeyChange(string name)
		{
			matchingKeyChanges.TryAdd(name);
		}

		public void UnwatchKeyChange(string name)
		{
			matchingKeyChanges.TryRemove(name);
		}

		public void WatchTimeSeriesBulkOperation(string operationId)
		{
			matchingBulkOperations.TryAdd(operationId);
		}

		public void UnwatchTimeSeriesBulkOperation(string operationId)
		{
			matchingBulkOperations.TryRemove(operationId);
		}

		public void Send(TimeSeriesChangeNotification notification)
		{
			var timeSeriesPrefix = string.Concat(notification.Type, "/", notification.Key);
			if (watchAllTimeSeries > 0 || matchingKeyChanges.Contains(timeSeriesPrefix))
			{
				var value = new { Value = notification, Type = keyChangeNotificationType };
				enqueue(value);
			}

			/*if (matchingRanges.Any(prefix => prefix.InRange(notification.At)))
			{
				var value = new { Value = notification, Type = rangesNotificationType };
				enqueue(value);
			}*/
		}

		public void Send(BulkOperationNotification notification)
		{
			if (matchingBulkOperations.Contains(string.Empty) == false &&
				matchingBulkOperations.Contains(notification.OperationId.ToString()) == false)
				return;

			var value = new { Value = notification, Type = bulkOperationNotificationType };
			enqueue(value);
		}
	}
}