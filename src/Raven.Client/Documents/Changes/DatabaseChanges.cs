using System;
using System.Diagnostics;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations;
using Raven.Client.Extensions;
using Raven.Client.Http;
using Sparrow.Json;

namespace Raven.Client.Documents.Changes
{
    internal class DatabaseChanges : AbstractDatabaseChanges<DatabaseConnectionState>, IDatabaseChanges, ISingleNodeDatabaseChanges
    {
        public DatabaseChanges(RequestExecutor requestExecutor, string databaseName, Action onDispose, string nodeTag)
            : base(requestExecutor, databaseName, onDispose, nodeTag, throttleConnection: false)
        {
        }

        async Task<IDatabaseChanges> IConnectableChanges<IDatabaseChanges>.EnsureConnectedNow()
        {
            var changes = await EnsureConnectedNowAsync().ConfigureAwait(false);
            return changes as IDatabaseChanges;
        }

        async Task<ISingleNodeDatabaseChanges> IConnectableChanges<ISingleNodeDatabaseChanges>.EnsureConnectedNow()
        {
            var changes = await EnsureConnectedNowAsync().ConfigureAwait(false);
            return changes as ISingleNodeDatabaseChanges;
        }

        public IChangesObservable<DocumentChange> ForDocument(string docId)
        {
            if (string.IsNullOrWhiteSpace(docId))
                throw new ArgumentException("Value cannot be null or whitespace.", nameof(docId));

            var counter = GetOrAddConnectionState("docs/" + docId, "watch-doc", "unwatch-doc", docId);

            var taskedObservable = new ChangesObservable<DocumentChange, DatabaseConnectionState>(
                counter,
                notification => string.Equals(notification.Id, docId, StringComparison.OrdinalIgnoreCase));

            return taskedObservable;
        }

        public IChangesObservable<DocumentChange> ForAllDocuments()
        {
            var counter = GetOrAddConnectionState("all-docs", "watch-docs", "unwatch-docs", null);

            var taskedObservable = new ChangesObservable<DocumentChange, DatabaseConnectionState>(
                counter,
                _ => true);

            return taskedObservable;
        }

        internal IChangesObservable<AggressiveCacheChange> ForAggressiveCaching()
        {
            var counter = GetOrAddConnectionState("aggressive-caching", "watch-aggressive-caching", "unwatch-aggressive-caching", null);

            var taskedObservable = new ChangesObservable<AggressiveCacheChange, DatabaseConnectionState>(
                counter,
                notification => true);

            return taskedObservable;
        }

        public IChangesObservable<OperationStatusChange> ForOperationId(long operationId)
        {
            if (string.IsNullOrWhiteSpace(_nodeTag))
                throw new ArgumentException("Changes API must be provided a node tag in order to track node-specific operations.");

            var counter = GetOrAddConnectionState("operations/" + operationId, "watch-operation", "unwatch-operation", operationId.ToString());

            var taskedObservable = new ChangesObservable<OperationStatusChange, DatabaseConnectionState>(
                counter,
                notification => notification.OperationId == operationId);

            return taskedObservable;
        }

        public IChangesObservable<OperationStatusChange> ForAllOperations()
        {
            if (string.IsNullOrWhiteSpace(_nodeTag))
                throw new ArgumentException("Changes API must be provided a node tag in order to track node-specific operations.");

            var counter = GetOrAddConnectionState("all-operations", "watch-operations", "unwatch-operations", null);

            var taskedObservable = new ChangesObservable<OperationStatusChange, DatabaseConnectionState>(
                counter,
                _ => true);

            return taskedObservable;
        }

        public IChangesObservable<IndexChange> ForIndex(string indexName)
        {
            if (string.IsNullOrWhiteSpace(indexName))
                throw new ArgumentException("Value cannot be null or whitespace.", nameof(indexName));

            var counter = GetOrAddConnectionState("indexes/" + indexName, "watch-index", "unwatch-index", indexName);

            var taskedObservable = new ChangesObservable<IndexChange, DatabaseConnectionState>(
                counter,
                notification => string.Equals(notification.Name, indexName, StringComparison.OrdinalIgnoreCase));

            return taskedObservable;
        }

        public IChangesObservable<IndexChange> ForAllIndexes()
        {
            var counter = GetOrAddConnectionState("all-indexes", "watch-indexes", "unwatch-indexes", null);

            var taskedObservable = new ChangesObservable<IndexChange, DatabaseConnectionState>(
                counter,
                _ => true);

            return taskedObservable;
        }

        public IChangesObservable<DocumentChange> ForDocumentsStartingWith(string docIdPrefix)
        {
            if (string.IsNullOrWhiteSpace(docIdPrefix))
                throw new ArgumentException("Value cannot be null or whitespace.", nameof(docIdPrefix));

            var counter = GetOrAddConnectionState("prefixes/" + docIdPrefix, "watch-prefix", "unwatch-prefix", docIdPrefix);

            var taskedObservable = new ChangesObservable<DocumentChange, DatabaseConnectionState>(
                counter,
                notification => notification.Id != null && notification.Id.StartsWith(docIdPrefix, StringComparison.OrdinalIgnoreCase));

            return taskedObservable;
        }

        public IChangesObservable<DocumentChange> ForDocumentsInCollection(string collectionName)
        {
            if (string.IsNullOrWhiteSpace(collectionName))
                throw new ArgumentException("Value cannot be null or whitespace.", nameof(collectionName));

            var counter = GetOrAddConnectionState("collections/" + collectionName, "watch-collection", "unwatch-collection", collectionName);

            var taskedObservable = new ChangesObservable<DocumentChange, DatabaseConnectionState>(
                counter,
                notification => string.Equals(collectionName, notification.CollectionName, StringComparison.OrdinalIgnoreCase));

            return taskedObservable;
        }

        public IChangesObservable<DocumentChange> ForDocumentsInCollection<TEntity>()
        {
            var collectionName = RequestExecutor.Conventions.GetCollectionName(typeof(TEntity));
            return ForDocumentsInCollection(collectionName);
        }

        public IChangesObservable<CounterChange> ForAllCounters()
        {
            var counter = GetOrAddConnectionState("all-counters", "watch-counters", "unwatch-counters", null);

            var taskedObservable = new ChangesObservable<CounterChange, DatabaseConnectionState>(
                counter,
                _ => true);

            return taskedObservable;
        }

        public IChangesObservable<CounterChange> ForCounter(string counterName)
        {
            if (string.IsNullOrWhiteSpace(counterName))
                throw new ArgumentException("Value cannot be null or whitespace.", nameof(counterName));

            var counter = GetOrAddConnectionState($"counter/{counterName}", "watch-counter", "unwatch-counter", counterName);

            var taskedObservable = new ChangesObservable<CounterChange, DatabaseConnectionState>(
                counter,
                notification => string.Equals(counterName, notification.Name, StringComparison.OrdinalIgnoreCase));

            return taskedObservable;
        }

        public IChangesObservable<CounterChange> ForCounterOfDocument(string documentId, string counterName)
        {
            if (string.IsNullOrWhiteSpace(documentId))
                throw new ArgumentException("Value cannot be null or whitespace.", nameof(documentId));
            if (string.IsNullOrWhiteSpace(counterName))
                throw new ArgumentException("Value cannot be null or whitespace.", nameof(counterName));

            var counter = GetOrAddConnectionState($"document/{documentId}/counter/{counterName}", "watch-document-counter", "unwatch-document-counter", value: null, values: new[] { documentId, counterName });

            var taskedObservable = new ChangesObservable<CounterChange, DatabaseConnectionState>(
                counter,
                notification => string.Equals(counterName, notification.Name, StringComparison.OrdinalIgnoreCase) && string.Equals(documentId, notification.DocumentId, StringComparison.OrdinalIgnoreCase));

            return taskedObservable;
        }

        public IChangesObservable<CounterChange> ForCountersOfDocument(string documentId)
        {
            if (string.IsNullOrWhiteSpace(documentId))
                throw new ArgumentException("Value cannot be null or whitespace.", nameof(documentId));

            var counter = GetOrAddConnectionState($"document/{documentId}/counter", "watch-document-counters", "unwatch-document-counters", documentId);

            var taskedObservable = new ChangesObservable<CounterChange, DatabaseConnectionState>(
                counter,
                notification => string.Equals(documentId, notification.DocumentId, StringComparison.OrdinalIgnoreCase));

            return taskedObservable;
        }

        public IChangesObservable<TimeSeriesChange> ForAllTimeSeries()
        {
            var counter = GetOrAddConnectionState("all-timeseries", "watch-all-timeseries", "unwatch-all-timeseries", null);

            var taskedObservable = new ChangesObservable<TimeSeriesChange, DatabaseConnectionState>(
                counter,
                _ => true);

            return taskedObservable;
        }

        public IChangesObservable<TimeSeriesChange> ForTimeSeries(string timeSeriesName)
        {
            if (string.IsNullOrWhiteSpace(timeSeriesName))
                throw new ArgumentException("Value cannot be null or whitespace.", nameof(timeSeriesName));

            var counter = GetOrAddConnectionState($"timeseries/{timeSeriesName}", "watch-timeseries", "unwatch-timeseries", timeSeriesName);

            var taskedObservable = new ChangesObservable<TimeSeriesChange, DatabaseConnectionState>(
                counter,
                notification => string.Equals(timeSeriesName, notification.Name, StringComparison.OrdinalIgnoreCase));

            return taskedObservable;
        }

        public IChangesObservable<TimeSeriesChange> ForTimeSeriesOfDocument(string documentId, string timeSeriesName)
        {
            if (string.IsNullOrWhiteSpace(documentId))
                throw new ArgumentException("Value cannot be null or whitespace.", nameof(documentId));
            if (string.IsNullOrWhiteSpace(timeSeriesName))
                throw new ArgumentException("Value cannot be null or whitespace.", nameof(timeSeriesName));

            var counter = GetOrAddConnectionState($"document/{documentId}/timeseries/{timeSeriesName}", "watch-document-timeseries", "unwatch-document-timeseries", value: null, values: new[] { documentId, timeSeriesName });

            var taskedObservable = new ChangesObservable<TimeSeriesChange, DatabaseConnectionState>(
                counter,
                notification => string.Equals(timeSeriesName, notification.Name, StringComparison.OrdinalIgnoreCase) && string.Equals(documentId, notification.DocumentId, StringComparison.OrdinalIgnoreCase));

            return taskedObservable;
        }

        public IChangesObservable<TimeSeriesChange> ForTimeSeriesOfDocument(string documentId)
        {
            if (string.IsNullOrWhiteSpace(documentId))
                throw new ArgumentException("Value cannot be null or whitespace.", nameof(documentId));

            var counter = GetOrAddConnectionState($"document/{documentId}/timeseries", "watch-all-document-timeseries", "unwatch-all-document-timeseries", documentId);

            var taskedObservable = new ChangesObservable<TimeSeriesChange, DatabaseConnectionState>(
                counter,
                notification => string.Equals(documentId, notification.DocumentId, StringComparison.OrdinalIgnoreCase));

            return taskedObservable;
        }

        protected override DatabaseConnectionState CreateDatabaseConnectionState(Func<Task> onConnect, Func<Task> onDisconnect) => new(onConnect, onDisconnect);

        protected override void ProcessNotification(string type, BlittableJsonReaderObject change)
        {
            change.TryGet("Value", out BlittableJsonReaderObject value);
            NotifySubscribers(type, value);
        }

        private void NotifySubscribers(string type, BlittableJsonReaderObject value)
        {
            switch (type)
            {
                case nameof(AggressiveCacheChange):
                    foreach (var state in States.ForceEnumerateInThreadSafeManner())
                    {
                        state.Value.Send(AggressiveCacheChange.Instance);
                    }
                    break;
                case nameof(DocumentChange):
                    var documentChange = DocumentChange.FromJson(value);
                    foreach (var state in States.ForceEnumerateInThreadSafeManner())
                    {
                        state.Value.Send(documentChange);
                    }
                    break;

                case nameof(CounterChange):
                    var counterChange = CounterChange.FromJson(value);
                    foreach (var state in States.ForceEnumerateInThreadSafeManner())
                    {
                        state.Value.Send(counterChange);
                    }
                    break;

                case nameof(TimeSeriesChange):
                    var timeSeriesChange = TimeSeriesChange.FromJson(value);
                    foreach (var state in States.ForceEnumerateInThreadSafeManner())
                    {
                        state.Value.Send(timeSeriesChange);
                    }
                    break;

                case nameof(IndexChange):
                    var indexChange = IndexChange.FromJson(value);
                    foreach (var state in States.ForceEnumerateInThreadSafeManner())
                    {
                        state.Value.Send(indexChange);
                    }
                    break;

                case nameof(OperationStatusChange):
                    var operationStatusChange = OperationStatusChange.FromJson(value);
                    foreach (var state in States.ForceEnumerateInThreadSafeManner())
                    {
                        state.Value.Send(operationStatusChange);
                    }
                    break;

                case nameof(TopologyChange):
                    var topologyChange = TopologyChange.FromJson(value);

                    var requestExecutor = RequestExecutor;
                    if (requestExecutor != null)
                    {
                        var node = new ServerNode
                        {
                            Url = topologyChange.Url,
                            Database = topologyChange.Database
                        };

                        requestExecutor.UpdateTopologyAsync(new RequestExecutor.UpdateTopologyParameters(node)
                        {
                            TimeoutInMs = 0,
                            ForceUpdate = true,
                            DebugTag = "topology-change-notification"
                        }).ConfigureAwait(false);
                    }
                    break;

                default:
                    throw new NotSupportedException(type);
            }
        }
    }
}
