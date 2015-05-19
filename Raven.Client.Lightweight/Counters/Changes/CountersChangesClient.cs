using System;
using System.Collections.Generic;
using System.Net;
using System.Threading.Tasks;
using Raven.Abstractions.Counters.Notifications;
using Raven.Abstractions.Extensions;
using Raven.Client.Changes;
using Raven.Client.Connection;
using Raven.Database.Util;
using Raven.Json.Linq;

namespace Raven.Client.Counters.Changes
{

    public class CountersChangesClient : RemoteChangesClientBase<ICountersChanges, CountersConnectionState>, ICountersChanges
    {
		private readonly ConcurrentSet<string> watchedChanges = new ConcurrentSet<string>(StringComparer.InvariantCultureIgnoreCase);
		private readonly ConcurrentSet<string> watchedLocalChanges = new ConcurrentSet<string>(StringComparer.InvariantCultureIgnoreCase);
		private readonly ConcurrentSet<string> watchedReplicationChanges = new ConcurrentSet<string>(StringComparer.InvariantCultureIgnoreCase);
		private readonly ConcurrentSet<string> watchedBulkOperations = new ConcurrentSet<string>(StringComparer.InvariantCultureIgnoreCase);

		public CountersChangesClient(string url, string apiKey,
                                       ICredentials credentials,
                                       HttpJsonRequestFactory jsonRequestFactory, Convention conventions,
                                       Action onDispose)
            : base(url, apiKey, credentials, jsonRequestFactory, conventions, onDispose)
        {

        }

        protected override async Task SubscribeOnServer()
        {
			foreach (var matchingChange in watchedChanges)
            {
				await Send("watch-change", matchingChange).ConfigureAwait(false);
            }

			foreach (var matchingLocalChange in watchedLocalChanges)
			{
				await Send("watch-local-change", matchingLocalChange).ConfigureAwait(false);
			}

			foreach (var matchingReplicationChange in watchedReplicationChanges)
			{
				await Send("watch-replication-change", matchingReplicationChange).ConfigureAwait(false);
			}

			foreach (var matchingBulkOperation in watchedBulkOperations)
			{
				await Send("watch-bulk-operation", matchingBulkOperation).ConfigureAwait(false);
			}
        }

		protected override void NotifySubscribers(string type, RavenJObject value, IEnumerable<KeyValuePair<string, CountersConnectionState>> connections)
        {
            switch (type)
            {
				case "ChangeNotification":
					var changeNotification = value.JsonDeserialization<ChangeNotification>();
                    foreach (var counter in connections)
                    {
                        counter.Value.Send(changeNotification);
                    }
                    break;
				case "LocalChangeNotification":
					var localChangeNotification = value.JsonDeserialization<LocalChangeNotification>();
					foreach (var counter in connections)
					{
						counter.Value.Send(localChangeNotification);
					}
					break;
				case "ReplicationChangeNotification":
					var replicationChangeNotification = value.JsonDeserialization<ReplicationChangeNotification>();
                    foreach (var counter in connections)
                    {
						counter.Value.Send(replicationChangeNotification);
                    }
                    break;
				case "BulkOperationNotification":
					var bulkOperationNotification = value.JsonDeserialization<BulkOperationNotification>();
                    foreach (var counter in connections)
                    {
                        counter.Value.Send(bulkOperationNotification);
                    }
                    break;
                default:
                    break;
            }
        }

	    public IObservableWithTask<ChangeNotification> ForChange(string groupName, string counterName)
	    {
			if (string.IsNullOrWhiteSpace(groupName))
				throw new ArgumentException("Group name cannot be empty!");

			if (string.IsNullOrWhiteSpace(counterName))
				throw new ArgumentException("Counter name cannot be empty");

			var fullCounterName = FullCounterName(groupName, counterName);
			var key = string.Concat("change/", fullCounterName);
			var counter = Counters.GetOrAdd(key, s =>
			{
				var changeSubscriptionTask = AfterConnection(() =>
				{
					watchedChanges.TryAdd(fullCounterName);
					return Send("watch-change", fullCounterName);
				});

				return new CountersConnectionState(
					() =>
					{
						watchedChanges.TryRemove(fullCounterName);
						Send("unwatch-change", fullCounterName);
						Counters.Remove(key);
					},
					changeSubscriptionTask);
			});

			var taskedObservable = new TaskedObservable<ChangeNotification, CountersConnectionState>(
								counter,
								notification => string.Equals(notification.GroupName, groupName, StringComparison.OrdinalIgnoreCase) &&
												string.Equals(notification.CounterName, counterName, StringComparison.OrdinalIgnoreCase));
			counter.OnChangeNotification += taskedObservable.Send;
			counter.OnError += taskedObservable.Error;

			return taskedObservable;
	    }

	    private static string FullCounterName(string groupName, string counterName)
	    {
		    return string.Concat(groupName, "/", counterName);
	    }

	    public IObservableWithTask<LocalChangeNotification> ForLocalCounterChange(string groupName, string counterName)
	    {
			if (string.IsNullOrWhiteSpace(groupName))
				throw new ArgumentException("Group name cannot be empty!");

			if (string.IsNullOrWhiteSpace(counterName))
				throw new ArgumentException("Counter name cannot be empty");

			var fullCounterName = FullCounterName(groupName, counterName);
			var key = string.Concat("change/", fullCounterName);
			var counter = Counters.GetOrAdd(key, s =>
			{
				var changeSubscriptionTask = AfterConnection(() =>
				{
					watchedChanges.TryAdd(fullCounterName);
					return Send("watch-local-change", fullCounterName);
				});

				return new CountersConnectionState(
					() =>
					{
						watchedChanges.TryRemove(fullCounterName);
						Send("unwatch-local-change", fullCounterName);
						Counters.Remove(key);
					},
					changeSubscriptionTask);
			});

			var taskedObservable = new TaskedObservable<LocalChangeNotification, CountersConnectionState>(
								counter,
								notification => string.Equals(notification.GroupName, groupName, StringComparison.OrdinalIgnoreCase) &&
												string.Equals(notification.CounterName, counterName, StringComparison.OrdinalIgnoreCase));
			counter.OnLocalChangeNotification += taskedObservable.Send;
			counter.OnError += taskedObservable.Error;

			return taskedObservable;
	    }

	    public IObservableWithTask<ReplicationChangeNotification> ForReplicationChange(string groupName, string counterName)
	    {
			if (string.IsNullOrWhiteSpace(groupName))
				throw new ArgumentException("Group name cannot be empty!");

			if (string.IsNullOrWhiteSpace(counterName))
				throw new ArgumentException("Counter name cannot be empty");

			var fullCounterName = FullCounterName(groupName, counterName);
			var key = string.Concat("change/", fullCounterName);
			var counter = Counters.GetOrAdd(key, s =>
			{
				var changeSubscriptionTask = AfterConnection(() =>
				{
					watchedChanges.TryAdd(fullCounterName);
					return Send("watch-replication-change", fullCounterName);
				});

				return new CountersConnectionState(
					() =>
					{
						watchedChanges.TryRemove(fullCounterName);
						Send("unwatch-replication-change", fullCounterName);
						Counters.Remove(key);
					},
					changeSubscriptionTask);
			});

			var taskedObservable = new TaskedObservable<ReplicationChangeNotification, CountersConnectionState>(
								counter,
								notification => string.Equals(notification.GroupName, groupName, StringComparison.OrdinalIgnoreCase) &&
												string.Equals(notification.CounterName, counterName, StringComparison.OrdinalIgnoreCase));
			counter.OnReplicationChangeNotification += taskedObservable.Send;
			counter.OnError += taskedObservable.Error;

			return taskedObservable;
	    }

	    public IObservableWithTask<BulkOperationNotification> ForBulkOperation(Guid? operationId = null)
	    {
			var id = operationId != null ? operationId.ToString() : string.Empty;

		    var key = "bulk-operations/" + id;
		    var counter = Counters.GetOrAdd(key, s =>
			{
				watchedBulkOperations.TryAdd(id);
				var bulkOperationSubscriptionTask = AfterConnection(() =>
				{
					if (watchedBulkOperations.Contains(id)) // might have been removed in the meantime
						return Send("watch-bulk-operation", id);
					return Task;
				});

				return new CountersConnectionState(
					() =>
					{
						watchedBulkOperations.TryRemove(id);
						Send("unwatch-bulk-operation", id);
						Counters.Remove(key);
					},
					bulkOperationSubscriptionTask);
			});

			var taskedObservable = new TaskedObservable<BulkOperationNotification, CountersConnectionState>(
				counter,
				notification => operationId == null || notification.OperationId == operationId);

			counter.OnBulkOperationNotification += taskedObservable.Send;
			counter.OnError += taskedObservable.Error;

			return taskedObservable;
	    }
    }
}
