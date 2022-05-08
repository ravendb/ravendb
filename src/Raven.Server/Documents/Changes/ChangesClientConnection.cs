using System;
using System.Net.WebSockets;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Changes;
using Raven.Client.Documents.Operations;
using Raven.Server.ServerWide.Context;
using Sparrow.Collections;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Changes
{
    public class ChangesClientConnection : AbstractChangesClientConnection<DocumentsOperationContext>
    {
        private readonly ConcurrentSet<string> _matchingDocuments = new(StringComparer.OrdinalIgnoreCase);

        private readonly ConcurrentSet<string> _matchingIndexes = new(StringComparer.OrdinalIgnoreCase);

        private readonly ConcurrentSet<string> _matchingDocumentPrefixes = new(StringComparer.OrdinalIgnoreCase);

        private readonly ConcurrentSet<string> _matchingDocumentsInCollection = new(StringComparer.OrdinalIgnoreCase);

        private readonly ConcurrentSet<string> _matchingDocumentsOfType = new(StringComparer.OrdinalIgnoreCase);

        private readonly ConcurrentSet<string> _matchingCounters = new(StringComparer.OrdinalIgnoreCase);

        private readonly ConcurrentSet<string> _matchingDocumentCounters = new(StringComparer.OrdinalIgnoreCase);

        private readonly ConcurrentSet<DocumentIdAndNamePair> _matchingDocumentCounter = new();

        private readonly ConcurrentSet<string> _matchingTimeSeries = new(StringComparer.OrdinalIgnoreCase);

        private readonly ConcurrentSet<string> _matchingAllDocumentTimeSeries = new(StringComparer.OrdinalIgnoreCase);

        private readonly ConcurrentSet<DocumentIdAndNamePair> _matchingDocumentTimeSeries = new();

        private readonly ConcurrentSet<long> _matchingOperations = new();

        private bool _watchTopology;

        private int _watchAllDocuments;
        private int _watchAllOperations;
        private int _watchAllIndexes;
        private int _watchAllCounters;
        private int _watchAllTimeSeries;

        public ChangesClientConnection(WebSocket webSocket, DocumentDatabase database, bool throttleConnection, bool fromStudio)
            : base(webSocket, database.DocumentsStorage.ContextPool, database.DatabaseShutdown, throttleConnection, fromStudio)
        {
        }

        public void SendOperationStatusChangeNotification(OperationStatusChange change)
        {
            if (_watchAllOperations > 0)
            {
                Send(change);
                return;
            }

            if (_matchingOperations.Contains(change.OperationId))
            {
                Send(change);
            }
        }

        public void SendCounterChanges(CounterChange change)
        {
            if (IsDisposed)
                return;

            if (_watchAllCounters > 0)
            {
                Send(change);
                return;
            }

            if (change.Name != null && _matchingCounters.Contains(change.Name))
            {
                Send(change);
                return;
            }

            if (change.DocumentId != null && _matchingDocumentCounters.Contains(change.DocumentId))
            {
                Send(change);
                return;
            }

            if (change.DocumentId != null && change.Name != null && _matchingDocumentCounter.Count > 0)
            {
                var parameters = new DocumentIdAndNamePair(change.DocumentId, change.Name);
                if (_matchingDocumentCounter.Contains(parameters))
                {
                    Send(change);
                    return;
                }
            }
        }

        public void SendTimeSeriesChanges(TimeSeriesChange change)
        {
            if (IsDisposed)
                return;

            if (_watchAllTimeSeries > 0)
            {
                Send(change);
                return;
            }

            if (change.Name != null && _matchingTimeSeries.Contains(change.Name))
            {
                Send(change);
                return;
            }

            if (change.DocumentId != null && _matchingAllDocumentTimeSeries.Contains(change.DocumentId))
            {
                Send(change);
                return;
            }

            if (change.DocumentId != null && change.Name != null && _matchingDocumentTimeSeries.Count > 0)
            {
                var parameters = new DocumentIdAndNamePair(change.DocumentId, change.Name);
                if (_matchingDocumentTimeSeries.Contains(parameters))
                {
                    Send(change);
                    return;
                }
            }
        }

        public void SendDocumentChanges(DocumentChange change)
        {
            // this is a precaution, in order to overcome an observed race condition between change client disconnection and raising changes
            if (IsDisposed)
                return;

            if (_watchAllDocuments > 0)
            {
                Send(change);
                return;
            }

            if (change.Id != null && _matchingDocuments.Contains(change.Id))
            {
                Send(change);
                return;
            }

            var hasPrefix = change.Id != null && HasItemStartingWith(_matchingDocumentPrefixes, change.Id);
            if (hasPrefix)
            {
                Send(change);
                return;
            }

            var hasCollection = change.CollectionName != null && HasItemEqualsTo(_matchingDocumentsInCollection, change.CollectionName);
            if (hasCollection)
            {
                Send(change);
                return;
            }

            if (change.Id == null && change.CollectionName == null)
            {
                Send(change);
            }
        }

        public void SendIndexChanges(IndexChange change)
        {
            if (_watchAllIndexes > 0)
            {
                Send(change);
                return;
            }

            if (change.Name != null && _matchingIndexes.Contains(change.Name))
            {
                Send(change);
            }
        }

        public void SendTopologyChanges(TopologyChange change)
        {
            if (_watchTopology)
            {
                Send(change);
            }
        }

        private void Send(OperationStatusChange change)
        {
            var value = CreateValueToSend(nameof(OperationStatusChange), change.ToJson());

            AddToQueue(new SendQueueItem
            {
                ValueToSend = value,
                AllowSkip = false
            });
        }

        private void Send(TopologyChange change)
        {
            var value = CreateValueToSend(nameof(TopologyChange), change.ToJson());

            AddToQueue(new SendQueueItem
            {
                ValueToSend = value,
                AllowSkip = true
            });
        }

        private void Send(CounterChange change)
        {
            var value = CreateValueToSend(nameof(CounterChange), change.ToJson());

            AddToQueue(new SendQueueItem
            {
                ValueToSend = value,
                AllowSkip = true
            });
        }

        private void Send(TimeSeriesChange change)
        {
            var value = CreateValueToSend(nameof(TimeSeriesChange), change.ToJson());

            AddToQueue(new SendQueueItem
            {
                ValueToSend = value,
                AllowSkip = true
            });
        }

        private void Send(DocumentChange change)
        {
            var value = CreateValueToSend(nameof(DocumentChange), change.ToJson());

            AddToQueue(new SendQueueItem
            {
                ValueToSend = value,
                AllowSkip = true
            });
        }

        private void Send(IndexChange change)
        {
            var value = CreateValueToSend(nameof(IndexChange), change.ToJson());

            AddToQueue(new SendQueueItem
            {
                ValueToSend = value,
                AllowSkip = change.Type == IndexChangeTypes.BatchCompleted
            });
        }

        private static bool HasItemStartingWith(ConcurrentSet<string> set, string value)
        {
            if (set.Count == 0)
                return false;
            foreach (string item in set)
            {
                if (value.StartsWith(item, StringComparison.OrdinalIgnoreCase))
                    return true;
            }
            return false;
        }

        private static bool HasItemEqualsTo(ConcurrentSet<string> set, string value)
        {
            if (set.Count == 0)
                return false;
            foreach (string item in set)
            {
                if (value.Equals(item, StringComparison.OrdinalIgnoreCase))
                    return true;
            }
            return false;
        }

        protected override ValueTask WatchTopologyAsync()
        {
            _watchTopology = true;
            return ValueTask.CompletedTask;
        }

        protected override ValueTask WatchDocumentAsync(string docId)
        {
            _matchingDocuments.TryAdd(docId);
            return ValueTask.CompletedTask;
        }

        protected override ValueTask UnwatchDocumentAsync(string docId)
        {
            _matchingDocuments.TryRemove(docId);
            return ValueTask.CompletedTask;
        }

        protected override ValueTask WatchAllDocumentsAsync()
        {
            Interlocked.Increment(ref _watchAllDocuments);
            return ValueTask.CompletedTask;
        }

        protected override ValueTask UnwatchAllDocumentsAsync()
        {
            Interlocked.Decrement(ref _watchAllDocuments);
            return ValueTask.CompletedTask;
        }

        protected override ValueTask WatchCounterAsync(string name)
        {
            _matchingCounters.TryAdd(name);
            return ValueTask.CompletedTask;
        }

        protected override ValueTask UnwatchCounterAsync(string name)
        {
            _matchingCounters.TryRemove(name);
            return ValueTask.CompletedTask;
        }

        protected override ValueTask WatchDocumentCountersAsync(string docId)
        {
            _matchingDocumentCounters.TryAdd(docId);
            return ValueTask.CompletedTask;
        }

        protected override ValueTask UnwatchDocumentCountersAsync(string docId)
        {
            _matchingDocumentCounters.TryRemove(docId);
            return ValueTask.CompletedTask;
        }

        protected override ValueTask WatchDocumentCounterAsync(BlittableJsonReaderArray parameters)
        {
            var val = GetParameters(parameters);

            _matchingDocumentCounter.TryAdd(val);
            return ValueTask.CompletedTask;
        }

        protected override ValueTask UnwatchDocumentCounterAsync(BlittableJsonReaderArray parameters)
        {
            var val = GetParameters(parameters);

            _matchingDocumentCounter.TryRemove(val);
            return ValueTask.CompletedTask;
        }

        protected override ValueTask WatchAllCountersAsync()
        {
            Interlocked.Increment(ref _watchAllCounters);
            return ValueTask.CompletedTask;
        }

        protected override ValueTask UnwatchAllCountersAsync()
        {
            Interlocked.Decrement(ref _watchAllCounters);
            return ValueTask.CompletedTask;
        }

        protected override ValueTask WatchTimeSeriesAsync(string name)
        {
            _matchingTimeSeries.TryAdd(name);
            return ValueTask.CompletedTask;
        }

        protected override ValueTask UnwatchTimeSeriesAsync(string name)
        {
            _matchingTimeSeries.TryRemove(name);
            return ValueTask.CompletedTask;
        }

        protected override ValueTask WatchAllDocumentTimeSeriesAsync(string docId)
        {
            _matchingAllDocumentTimeSeries.TryAdd(docId);
            return ValueTask.CompletedTask;
        }

        protected override ValueTask UnwatchAllDocumentTimeSeriesAsync(string docId)
        {
            _matchingAllDocumentTimeSeries.TryRemove(docId);
            return ValueTask.CompletedTask;
        }

        protected override ValueTask WatchDocumentTimeSeriesAsync(BlittableJsonReaderArray parameters)
        {
            var val = GetParameters(parameters);

            _matchingDocumentTimeSeries.TryAdd(val);

            return ValueTask.CompletedTask;
        }

        protected override ValueTask UnwatchDocumentTimeSeriesAsync(BlittableJsonReaderArray parameters)
        {
            var val = GetParameters(parameters);

            _matchingDocumentTimeSeries.TryRemove(val);

            return ValueTask.CompletedTask;
        }

        protected override ValueTask WatchAllTimeSeriesAsync()
        {
            Interlocked.Increment(ref _watchAllTimeSeries);
            return ValueTask.CompletedTask;
        }

        protected override ValueTask UnwatchAllTimeSeriesAsync()
        {
            Interlocked.Decrement(ref _watchAllTimeSeries);
            return ValueTask.CompletedTask;
        }

        protected override ValueTask WatchDocumentPrefixAsync(string name)
        {
            _matchingDocumentPrefixes.TryAdd(name);
            return ValueTask.CompletedTask;
        }

        protected override ValueTask UnwatchDocumentPrefixAsync(string name)
        {
            _matchingDocumentPrefixes.TryRemove(name);
            return ValueTask.CompletedTask;
        }

        protected override ValueTask WatchDocumentInCollectionAsync(string name)
        {
            _matchingDocumentsInCollection.TryAdd(name);
            return ValueTask.CompletedTask;
        }

        protected override ValueTask UnwatchDocumentInCollectionAsync(string name)
        {
            _matchingDocumentsInCollection.TryRemove(name);
            return ValueTask.CompletedTask;
        }

        protected override ValueTask WatchDocumentOfTypeAsync(string name)
        {
            _matchingDocumentsOfType.TryAdd(name);
            return ValueTask.CompletedTask;
        }

        protected override ValueTask UnwatchDocumentOfTypeAsync(string name)
        {
            _matchingDocumentsOfType.TryRemove(name);
            return ValueTask.CompletedTask;
        }

        protected override ValueTask WatchAllIndexesAsync()
        {
            Interlocked.Increment(ref _watchAllIndexes);
            return ValueTask.CompletedTask;
        }

        protected override ValueTask UnwatchAllIndexesAsync()
        {
            Interlocked.Decrement(ref _watchAllIndexes);
            return ValueTask.CompletedTask;
        }

        protected override ValueTask WatchIndexAsync(string name)
        {
            _matchingIndexes.TryAdd(name);
            return ValueTask.CompletedTask;
        }

        protected override ValueTask UnwatchIndexAsync(string name)
        {
            _matchingIndexes.TryRemove(name);
            return ValueTask.CompletedTask;
        }

        protected override ValueTask WatchOperationAsync(long operationId)
        {
            _matchingOperations.TryAdd(operationId);
            return ValueTask.CompletedTask;
        }

        protected override ValueTask UnwatchOperationAsync(long operationId)
        {
            _matchingOperations.TryRemove(operationId);
            return ValueTask.CompletedTask;
        }

        protected override ValueTask WatchAllOperationsAsync()
        {
            Interlocked.Increment(ref _watchAllOperations);
            return ValueTask.CompletedTask;
        }

        protected override ValueTask UnwatchAllOperationsAsync()
        {
            Interlocked.Decrement(ref _watchAllOperations);
            return ValueTask.CompletedTask;
        }

        private static DynamicJsonValue CreateValueToSend(string type, DynamicJsonValue value)
        {
            return new DynamicJsonValue
            {
                ["Type"] = type,
                ["Value"] = value
            };
        }
    }
}
