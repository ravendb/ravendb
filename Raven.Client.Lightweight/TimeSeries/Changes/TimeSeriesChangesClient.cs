using System;
using System.Collections.Generic;
using System.Net;
using System.Threading.Tasks;
using Raven.Abstractions.TimeSeries.Notifications;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Client.Changes;
using Raven.Client.Connection;
using Raven.Json.Linq;
using Sparrow.Collections;

namespace Raven.Client.TimeSeries.Changes
{
	public class TimeSeriesChangesClient : RemoteChangesClientBase<ITimeSeriesChanges, TimeSeriesConnectionState, TimeSeriesConvention>, ITimeSeriesChanges
    {
		private readonly ConcurrentSet<string> watchedChanges = new ConcurrentSet<string>();
		private readonly ConcurrentSet<string> watchedPrefixes = new ConcurrentSet<string>();
		private readonly ConcurrentSet<string> watchedTimeSeriesInGroup = new ConcurrentSet<string>();
		private readonly ConcurrentSet<string> watchedBulkOperations = new ConcurrentSet<string>();

		public TimeSeriesChangesClient(string url, string apiKey,
                                       ICredentials credentials,
                                       HttpJsonRequestFactory jsonRequestFactory, TimeSeriesConvention timeSeriesConventions,
                                       Action onDispose)
            : base(url, apiKey, credentials, jsonRequestFactory, timeSeriesConventions, onDispose)
        {

        }

        protected override async Task SubscribeOnServer()
        {
			foreach (var matchingChange in watchedChanges)
            {
				await Send("watch-time-series-change", matchingChange).ConfigureAwait(false);
            }

			foreach (var matchingLocalChange in watchedPrefixes)
			{
				await Send("watch-time-series-prefix", matchingLocalChange).ConfigureAwait(false);
			}

			foreach (var matchingReplicationChange in watchedTimeSeriesInGroup)
			{
				await Send("watch-time-series-in-group", matchingReplicationChange).ConfigureAwait(false);
			}

			foreach (var matchingBulkOperation in watchedBulkOperations)
			{
				await Send("watch-bulk-operation", matchingBulkOperation).ConfigureAwait(false);
			}
        }

		protected override void NotifySubscribers(string type, RavenJObject value, IEnumerable<KeyValuePair<string, TimeSeriesConnectionState>> connections)
        {
            switch (type)
            {
				case "ChangeNotification":
					var changeNotification = value.JsonDeserialization<ChangeNotification>();
                    foreach (var timeSeries in connections)
                    {
                        timeSeries.Value.Send(changeNotification);
                    }
                    break;
				case "StartingWithNotification":
					var timeSeriesStartingWithNotification = value.JsonDeserialization<StartingWithNotification>();
					foreach (var timeSeries in connections)
					{
						timeSeries.Value.Send(timeSeriesStartingWithNotification);
					}
					break;
				case "InGroupNotification":
					var timeSeriesInGroupNotification = value.JsonDeserialization<InGroupNotification>();
                    foreach (var timeSeries in connections)
                    {
						timeSeries.Value.Send(timeSeriesInGroupNotification);
                    }
                    break;
				case "BulkOperationNotification":
					var bulkOperationNotification = value.JsonDeserialization<BulkOperationNotification>();
                    foreach (var timeSeries in connections)
                    {
                        timeSeries.Value.Send(bulkOperationNotification);
                    }
                    break;
                default:
                    break;
            }
        }

	    public IObservableWithTask<ChangeNotification> ForChange(string groupName, string timeSeriesName)
	    {
			if (string.IsNullOrWhiteSpace(groupName))
				throw new ArgumentException("Group name cannot be empty!");

			if (string.IsNullOrWhiteSpace(timeSeriesName))
				throw new ArgumentException("TimeSeries name cannot be empty");

			var fullTimeSeriesName = FullTimeSeriesName(groupName, timeSeriesName);
			var key = string.Concat("time-series-change/", fullTimeSeriesName);
			var timeSeries = Counters.GetOrAdd(key, s =>
			{
				var changeSubscriptionTask = AfterConnection(() =>
				{
					watchedChanges.TryAdd(fullTimeSeriesName);
					return Send("watch-time-series-change", fullTimeSeriesName);
				});

				return new TimeSeriesConnectionState(
					() =>
					{
						watchedChanges.TryRemove(fullTimeSeriesName);
						Send("unwatch-time-series-change", fullTimeSeriesName);
						Counters.Remove(key);
					},
					changeSubscriptionTask);
			});

			var taskedObservable = new TaskedObservable<ChangeNotification, TimeSeriesConnectionState>(
								timeSeries,
								notification => string.Equals(notification.GroupName, groupName, StringComparison.OrdinalIgnoreCase) &&
												string.Equals(notification.TimeSeriesName, timeSeriesName, StringComparison.OrdinalIgnoreCase));
			timeSeries.OnChangeNotification += taskedObservable.Send;
			timeSeries.OnError += taskedObservable.Error;

			return taskedObservable;
	    }

	    private static string FullTimeSeriesName(string groupName, string timeSeriesName)
	    {
			return string.Concat(groupName, Constants.TimeSeries.Separator, timeSeriesName);
	    }

		public IObservableWithTask<StartingWithNotification> ForTimeSeriesStartingWith(string groupName, string prefixForName)
	    {
			if (string.IsNullOrWhiteSpace(groupName))
				throw new ArgumentException("Group name cannot be empty!");

			if (string.IsNullOrWhiteSpace(prefixForName))
				throw new ArgumentException("Prefix for time-series name cannot be empty");

			var timeSeriesPrefix = FullTimeSeriesName(groupName, prefixForName);
			var key = string.Concat("time-series-starting-with/", timeSeriesPrefix);
			var timeSeries = Counters.GetOrAdd(key, s =>
			{
				var changeSubscriptionTask = AfterConnection(() =>
				{
					watchedPrefixes.TryAdd(timeSeriesPrefix);
					return Send("watch-time-series-prefix", timeSeriesPrefix);
				});

				return new TimeSeriesConnectionState(
					() =>
					{
						watchedPrefixes.TryRemove(timeSeriesPrefix);
						Send("unwatch-time-series-prefix", timeSeriesPrefix);
						Counters.Remove(key);
					},
					changeSubscriptionTask);
			});

			var taskedObservable = new TaskedObservable<StartingWithNotification, TimeSeriesConnectionState>(
				timeSeries,
				notification =>
				{
					var t = string.Equals(notification.GroupName, groupName, StringComparison.OrdinalIgnoreCase) &&
					        notification.TimeSeriesName.StartsWith(prefixForName, StringComparison.OrdinalIgnoreCase);
					return t;
				});
			timeSeries.OnTimeSeriesStartingWithNotification += taskedObservable.Send;
			timeSeries.OnError += taskedObservable.Error;

			return taskedObservable;
	    }

		public IObservableWithTask<InGroupNotification> ForTimeSeriesInGroup(string groupName)
	    {
			if (string.IsNullOrWhiteSpace(groupName))
				throw new ArgumentException("Group name cannot be empty!");

			var key = string.Concat("time-series-in-group/", groupName);
			var timeSeries = Counters.GetOrAdd(key, s =>
			{
				var changeSubscriptionTask = AfterConnection(() =>
				{
					watchedTimeSeriesInGroup.TryAdd(groupName);
					return Send("watch-time-series-in-group", groupName);
				});

				return new TimeSeriesConnectionState(
					() =>
					{
						watchedTimeSeriesInGroup.TryRemove(groupName);
						Send("unwatch-time-series-in-group", groupName);
						Counters.Remove(key);
					},
					changeSubscriptionTask);
			});

			var taskedObservable = new TaskedObservable<InGroupNotification, TimeSeriesConnectionState>(
								timeSeries,
								notification => string.Equals(notification.GroupName, groupName, StringComparison.OrdinalIgnoreCase));
			timeSeries.OnTimeSeriesInGroupNotification += taskedObservable.Send;
			timeSeries.OnError += taskedObservable.Error;

			return taskedObservable;
	    }

	    public IObservableWithTask<BulkOperationNotification> ForBulkOperation(Guid? operationId = null)
	    {
			var id = operationId != null ? operationId.ToString() : string.Empty;

		    var key = "bulk-operations/" + id;
		    var timeSeries = Counters.GetOrAdd(key, s =>
			{
				watchedBulkOperations.TryAdd(id);
				var bulkOperationSubscriptionTask = AfterConnection(() =>
				{
					if (watchedBulkOperations.Contains(id)) // might have been removed in the meantime
						return Send("watch-bulk-operation", id);
					return Task;
				});

				return new TimeSeriesConnectionState(
					() =>
					{
						watchedBulkOperations.TryRemove(id);
						Send("unwatch-bulk-operation", id);
						Counters.Remove(key);
					},
					bulkOperationSubscriptionTask);
			});

			var taskedObservable = new TaskedObservable<BulkOperationNotification, TimeSeriesConnectionState>(
				timeSeries,
				notification => operationId == null || notification.OperationId == operationId);

			timeSeries.OnBulkOperationNotification += taskedObservable.Send;
			timeSeries.OnError += taskedObservable.Error;

			return taskedObservable;
	    }
    }
}
