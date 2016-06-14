using System;
using System.Collections.Generic;
using System.Net;
using System.Threading.Tasks;
using Raven.Abstractions.TimeSeries.Notifications;
using Raven.Abstractions.Extensions;
using Raven.Client.Changes;
using Raven.Client.Connection;
using Raven.Json.Linq;
using Sparrow.Collections;

namespace Raven.Client.TimeSeries.Changes
{
    public class TimeSeriesChangesClient : RemoteChangesClientBase<ITimeSeriesChanges, TimeSeriesConnectionState, TimeSeriesConvention>, ITimeSeriesChanges
    {
        private readonly ConcurrentSet<string> watchedKeysChanges = new ConcurrentSet<string>();
        private readonly ConcurrentSet<string> watchedBulkOperations = new ConcurrentSet<string>();
        private bool watchAllTimeSeries;

        public TimeSeriesChangesClient(string url, string apiKey,
                                       ICredentials credentials,
                                       HttpJsonRequestFactory jsonRequestFactory, TimeSeriesConvention conventions,
                                       Action onDispose)
            : base(url, apiKey, credentials, jsonRequestFactory, conventions, onDispose)
        {

        }

        protected override async Task SubscribeOnServer()
        {
            if (watchAllTimeSeries)
                await Send("watch-time-series", null).ConfigureAwait(false);

            foreach (var matchingKey in watchedKeysChanges)
            {
                await Send("watch-time-series-key-changes", matchingKey).ConfigureAwait(false);
            }

            foreach (var matchingBulkOperation in watchedBulkOperations)
            {
                await Send("watch-bulk-operation", matchingBulkOperation).ConfigureAwait(false);
            }
        }

        protected override void NotifySubscribers(string type, RavenJObject value, List<TimeSeriesConnectionState> connections)
        {
            switch (type)
            {
                case "KeyChangeNotification":
                    var changeNotification = value.JsonDeserialization<TimeSeriesChangeNotification>();
                    foreach (var timeSeries in connections)
                    {
                        timeSeries.Send(changeNotification);
                    }
                    break;
                case "BulkOperationNotification":
                    var bulkOperationNotification = value.JsonDeserialization<BulkOperationNotification>();
                    foreach (var timeSeries in connections)
                    {
                        timeSeries.Send(bulkOperationNotification);
                    }
                    break;
                default:
                    throw new InvalidOperationException("Type not valid: " + type);
            }
        }

        public IObservableWithTask<TimeSeriesChangeNotification> ForAllTimeSeries()
        {
            var timeSeries = GetOrAddConnectionState("all-time-series", "watch-time-series-key-change", "unwatch-time-series-key-change",
                () => watchAllTimeSeries = true,
                () => watchAllTimeSeries = false,
                null);

            var taskedObservable = new TaskedObservable<TimeSeriesChangeNotification, TimeSeriesConnectionState>(
                timeSeries,
                notification => true);

            timeSeries.OnChangeNotification += taskedObservable.Send;
            timeSeries.OnError += taskedObservable.Error;

            return taskedObservable;
        }

        public IObservableWithTask<TimeSeriesChangeNotification> ForKey(string type, string key)
        {
            if (string.IsNullOrWhiteSpace(type))
                throw new ArgumentException("Type cannot be empty!");

            if (string.IsNullOrWhiteSpace(key))
                throw new ArgumentException("Key cannot be empty!");

            var keyWithType = type + "/" + key;
            var timeSeries = GetOrAddConnectionState("time-series-key-change/" + keyWithType, "watch-time-series-key-change", "unwatch-time-series-key-change",
                () => watchedKeysChanges.TryAdd(keyWithType),
                () => watchedKeysChanges.TryRemove(keyWithType),
                keyWithType);

            var taskedObservable = new TaskedObservable<TimeSeriesChangeNotification, TimeSeriesConnectionState>(
                timeSeries,
                notification => string.Equals(notification.Type, type, StringComparison.OrdinalIgnoreCase) &&
                                string.Equals(notification.Key, key, StringComparison.OrdinalIgnoreCase));
            timeSeries.OnChangeNotification += taskedObservable.Send;
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
                    existingConnectionState =>
                    {
                        TimeSeriesConnectionState _;
                        if (Counters.TryGetValue("bulk-operations/" + id, out _))
                            return _.Task;

                        Counters.GetOrAdd("bulk-operations/" + id, x => existingConnectionState);

                        return AfterConnection(() =>
                        {
                            if (watchedBulkOperations.Contains(id)) // might have been removed in the meantime
                                return Send("watch-bulk-operation", id);
                            return Task;
                        });
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