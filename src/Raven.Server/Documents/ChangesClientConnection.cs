using System;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net.WebSockets;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Changes;
using Raven.Client.Documents.Operations;
using Raven.Client.Util;
using Sparrow;
using Sparrow.Collections;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Json.Sync;
using Sparrow.LowMemory;
using Sparrow.Server.Collections;
using Sparrow.Threading;
using Sparrow.Utils;

namespace Raven.Server.Documents
{
    public class ChangesClientConnection : ILowMemoryHandler, IDisposable
    {
        private static long _counter;

        private readonly WebSocket _webSocket;
        private readonly DocumentDatabase _documentDatabase;
        private readonly AsyncQueue<ChangeValue> _sendQueue = new AsyncQueue<ChangeValue>();
        private readonly MultipleUseFlag _lowMemoryFlag = new();

        public CancellationTokenSource CancellationToken { get; }

        private readonly CancellationToken _disposeToken;

        private readonly DateTime _startedAt;

        private readonly ConcurrentSet<string> _matchingIndexes = new ConcurrentSet<string>(StringComparer.OrdinalIgnoreCase);

        private readonly ConcurrentSet<string> _matchingDocuments = new ConcurrentSet<string>(StringComparer.OrdinalIgnoreCase);

        private readonly ConcurrentSet<string> _matchingDocumentPrefixes = new ConcurrentSet<string>(StringComparer.OrdinalIgnoreCase);

        private readonly ConcurrentSet<string> _matchingDocumentsInCollection = new ConcurrentSet<string>(StringComparer.OrdinalIgnoreCase);

        private readonly ConcurrentSet<string> _matchingDocumentsOfType = new ConcurrentSet<string>(StringComparer.OrdinalIgnoreCase);

        private readonly ConcurrentSet<string> _matchingCounters = new ConcurrentSet<string>(StringComparer.OrdinalIgnoreCase);

        private readonly ConcurrentSet<string> _matchingDocumentCounters = new ConcurrentSet<string>(StringComparer.OrdinalIgnoreCase);

        private readonly ConcurrentSet<DocumentIdAndNamePair> _matchingDocumentCounter = new ConcurrentSet<DocumentIdAndNamePair>();

        private readonly ConcurrentSet<string> _matchingTimeSeries = new ConcurrentSet<string>(StringComparer.OrdinalIgnoreCase);

        private readonly ConcurrentSet<string> _matchingAllDocumentTimeSeries = new ConcurrentSet<string>(StringComparer.OrdinalIgnoreCase);

        private readonly ConcurrentSet<DocumentIdAndNamePair> _matchingDocumentTimeSeries = new ConcurrentSet<DocumentIdAndNamePair>();

        private readonly ConcurrentSet<long> _matchingOperations = new ConcurrentSet<long>();

        private bool _watchTopology = false;

        private int _watchAllDocuments;
        private int _watchAllOperations;
        private int _watchAllIndexes;
        private int _watchAllCounters;
        private int _watchAllTimeSeries;
        private bool _aggressiveChanges;

        public class ChangeValue
        {
            public object ValueToSend;
            public bool AllowSkip;
        }

        public ChangesClientConnection(WebSocket webSocket, DocumentDatabase documentDatabase, bool fromStudio)
        {
            IsChangesConnectionOriginatedFromStudio = fromStudio;
            _webSocket = webSocket;
            _documentDatabase = documentDatabase;
            _startedAt = SystemTime.UtcNow;
            CancellationToken = CancellationTokenSource.CreateLinkedTokenSource(documentDatabase.DatabaseShutdown);
            _disposeToken = CancellationToken.Token;

            LowMemoryNotification.Instance.RegisterLowMemoryHandler(this);
        }

        public long Id = Interlocked.Increment(ref _counter);

        public TimeSpan Age => SystemTime.UtcNow - _startedAt;

        public void WatchTopology()
        {
            _watchTopology = true;
        }

        public void WatchDocument(string docId)
        {
            _matchingDocuments.TryAdd(docId);
        }

        public void UnwatchDocument(string docId)
        {
            _matchingDocuments.TryRemove(docId);
        }

        public void WatchAllDocuments()
        {
            Interlocked.Increment(ref _watchAllDocuments);
        }

        public void UnwatchAllDocuments()
        {
            Interlocked.Decrement(ref _watchAllDocuments);
        }

        public void WatchCounter(string name)
        {
            _matchingCounters.TryAdd(name);
        }

        public void UnwatchCounter(string name)
        {
            _matchingCounters.TryRemove(name);
        }

        public void WatchDocumentCounters(string docId)
        {
            _matchingDocumentCounters.TryAdd(docId);
        }

        public void UnwatchDocumentCounters(string docId)
        {
            _matchingDocumentCounters.TryRemove(docId);
        }

        public void WatchDocumentCounter(BlittableJsonReaderArray parameters)
        {
            var val = GetParameters(parameters);

            _matchingDocumentCounter.TryAdd(val);
        }

        public void UnwatchDocumentCounter(BlittableJsonReaderArray parameters)
        {
            var val = GetParameters(parameters);

            _matchingDocumentCounter.TryRemove(val);
        }

        public void WatchAllCounters()
        {
            Interlocked.Increment(ref _watchAllCounters);
        }

        public void UnwatchAllCounters()
        {
            Interlocked.Decrement(ref _watchAllCounters);
        }

        public void WatchTimeSeries(string name)
        {
            _matchingTimeSeries.TryAdd(name);
        }

        public void UnwatchTimeSeries(string name)
        {
            _matchingTimeSeries.TryRemove(name);
        }

        public void WatchAllDocumentTimeSeries(string docId)
        {
            _matchingAllDocumentTimeSeries.TryAdd(docId);
        }

        public void UnwatchAllDocumentTimeSeries(string docId)
        {
            _matchingAllDocumentTimeSeries.TryRemove(docId);
        }

        public void WatchDocumentTimeSeries(BlittableJsonReaderArray parameters)
        {
            var val = GetParameters(parameters);

            _matchingDocumentTimeSeries.TryAdd(val);
        }

        public void UnwatchDocumentTimeSeries(BlittableJsonReaderArray parameters)
        {
            var val = GetParameters(parameters);

            _matchingDocumentTimeSeries.TryRemove(val);
        }

        public void WatchAllTimeSeries()
        {
            Interlocked.Increment(ref _watchAllTimeSeries);
        }

        public void UnwatchAllTimeSeries()
        {
            Interlocked.Decrement(ref _watchAllTimeSeries);
        }

        public void WatchDocumentPrefix(string name)
        {
            _matchingDocumentPrefixes.TryAdd(name);
        }

        public void UnwatchDocumentPrefix(string name)
        {
            _matchingDocumentPrefixes.TryRemove(name);
        }

        public void WatchDocumentInCollection(string name)
        {
            _matchingDocumentsInCollection.TryAdd(name);
        }

        public void UnwatchDocumentInCollection(string name)
        {
            _matchingDocumentsInCollection.TryRemove(name);
        }

        public void WatchDocumentOfType(string name)
        {
            _matchingDocumentsOfType.TryAdd(name);
        }

        public void UnwatchDocumentOfType(string name)
        {
            _matchingDocumentsOfType.TryRemove(name);
        }

        public void WatchAllIndexes()
        {
            Interlocked.Increment(ref _watchAllIndexes);
        }

        public void UnwatchAllIndexes()
        {
            Interlocked.Decrement(ref _watchAllIndexes);
        }

        public void WatchIndex(string name)
        {
            _matchingIndexes.TryAdd(name);
        }

        public void UnwatchIndex(string name)
        {
            _matchingIndexes.TryRemove(name);
        }

        private static bool HasItemStartingWith(ConcurrentSet<string> set, string value)
        {
            if (set.IsEmpty)
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
            if (set.IsEmpty)
                return false;
            foreach (string item in set)
            {
                if (value.Equals(item, StringComparison.OrdinalIgnoreCase))
                    return true;
            }
            return false;
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

            if (change.DocumentId != null && change.Name != null && _matchingDocumentCounter.IsEmpty == false)
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

            if (change.DocumentId != null && change.Name != null && _matchingDocumentTimeSeries.IsEmpty == false)
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

            if (_aggressiveChanges)
            {
                Debug.Assert(_watchAllDocuments == 0 && (_matchingDocuments == null || _matchingDocuments.Count == 0));

                if (AggressiveCacheChange.ShouldUpdateAggressiveCache(change))
                    PulseAggressiveCaching();
                return;
            }

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
            if (_aggressiveChanges)
            {
                Debug.Assert(_watchAllIndexes == 0 && (_matchingIndexes == null || _matchingIndexes.Count == 0));

                if (AggressiveCacheChange.ShouldUpdateAggressiveCache(change))
                    PulseAggressiveCaching();
                return;
            }

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

        internal static readonly ChangeValue AggressiveCachingPulseValue = new()
        {
            AllowSkip = true,
            ValueToSend = new AggressiveCacheChangeFactory()
        };

        private void PulseAggressiveCaching()
        {
            _sendQueue.AddIfEmpty(AggressiveCachingPulseValue);
        }

        public void SendTopologyChanges(TopologyChange change)
        {
            if (_watchTopology)
            {
                Send(change);
            }
        }

        private void Send(TopologyChange change)
        {
            var value = new DynamicJsonValue
            {
                ["Type"] = nameof(TopologyChange),
                ["Value"] = change.ToJson()
            };

            if (_disposeToken.IsCancellationRequested == false)
                _sendQueue.Enqueue(new ChangeValue
                {
                    ValueToSend = value,
                    AllowSkip = true
                });
        }

        private void Send(CounterChange change)
        {
            var value = new DynamicJsonValue
            {
                ["Type"] = nameof(CounterChange),
                ["Value"] = change.ToJson()
            };

            if (_disposeToken.IsCancellationRequested == false)
                _sendQueue.Enqueue(new ChangeValue
                {
                    ValueToSend = value,
                    AllowSkip = true
                });
        }

        private void Send(TimeSeriesChange change)
        {
            var value = new DynamicJsonValue
            {
                ["Type"] = nameof(TimeSeriesChange),
                ["Value"] = change.ToJson()
            };

            if (_disposeToken.IsCancellationRequested == false)
                _sendQueue.Enqueue(new ChangeValue
                {
                    ValueToSend = value,
                    AllowSkip = true
                });
        }

        private void Send(DocumentChange change)
        {
            var value = new DynamicJsonValue
            {
                ["Type"] = nameof(DocumentChange),
                ["Value"] = change.ToJson()
            };

            if (_disposeToken.IsCancellationRequested == false)
                _sendQueue.Enqueue(new ChangeValue
                {
                    ValueToSend = value,
                    AllowSkip = true
                });
        }

        private void Send(IndexChange change)
        {
            var value = new DynamicJsonValue
            {
                ["Type"] = nameof(IndexChange),
                ["Value"] = change.ToJson()
            };

            if (_disposeToken.IsCancellationRequested == false)
                _sendQueue.Enqueue(new ChangeValue
                {
                    ValueToSend = value,
                    AllowSkip = change.Type == IndexChangeTypes.BatchCompleted
                });
        }

        public void WatchOperation(long operationId)
        {
            _matchingOperations.TryAdd(operationId);
        }

        public void UnwatchOperation(long operationId)
        {
            _matchingOperations.TryRemove(operationId);
        }

        public void WatchAllOperations()
        {
            Interlocked.Increment(ref _watchAllOperations);
        }

        public void UnwatchAllOperations()
        {
            Interlocked.Decrement(ref _watchAllOperations);
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

        private void Send(OperationStatusChange change)
        {
            var value = new DynamicJsonValue
            {
                ["Type"] = nameof(OperationStatusChange),
                ["Value"] = change.ToJson()
            };

            if (_disposeToken.IsCancellationRequested == false)
                _sendQueue.Enqueue(new ChangeValue
                {
                    ValueToSend = value,
                    AllowSkip = false
                });
        }

        public async Task StartSendingNotifications(bool throttleConnection)
        {
            using (_documentDatabase.DocumentsStorage.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            {
                await using (var ms = new MemoryStream())
                {
                    var sp = Stopwatch.StartNew();
                    var sendTaskSp = Stopwatch.StartNew();

                    while (true)
                    {
                        if (_disposeToken.IsCancellationRequested)
                            break;

                        ms.SetLength(0);
                        context.Reset();
                        context.Renew();

                        var messagesCount = 0;
                        using (var writer = new BlittableJsonTextWriter(context, ms))
                        {
                            sp.Restart();

                            var first = true;
                            writer.WriteStartArray();

                            do
                            {
                                var value = await GetNextMessage(throttleConnection);
                                if (value == null || _disposeToken.IsCancellationRequested)
                                    break;

                                _documentDatabase.ForTestingPurposes?.OnNextMessageChangesApi?.Invoke(value, _webSocket);

                                if (first == false)
                                    writer.WriteComma();

                                first = false;

                                switch (value)
                                {
                                    case DynamicJsonValue djv:
                                        context.Write(writer, djv);
                                        break;
                                    case DatabaseChangeFactory cf:
                                        context.Write(writer, cf.CreateJson());
                                        break;
                                }

                                messagesCount++;

                                writer.Flush();

                                if (ms.Length > 16 * 1024)
                                    break;
                            } while (_sendQueue.IsEmpty == false && sp.Elapsed < TimeSpan.FromSeconds(5));

                            writer.WriteEndArray();
                        }

                        if (_disposeToken.IsCancellationRequested)
                            break;

                        ms.TryGetBuffer(out ArraySegment<byte> bytes);

                        var sendTask = _webSocket.SendAsync(bytes, WebSocketMessageType.Text, true, _disposeToken);
                        if (sendTask.IsCompleted)
                        {
                            await sendTask;
                            continue;
                        }

                        await WaitForSendTaskAsync(sendTask, sendTaskSp, messagesCount, ms);
                    }
                }
            }
        }

        private async Task WaitForSendTaskAsync(Task sendTask, Stopwatch sp, int messagesCount, MemoryStream ms)
        {
            sp.Restart();

            while (true)
            {
                var waitTask = TimeoutManager.WaitFor(TimeSpan.FromSeconds(5), _disposeToken);

                var result = await Task.WhenAny(sendTask, waitTask);
                if (result == sendTask)
                {
                    await sendTask;
                    return;
                }

                var isLowMemory = _lowMemoryFlag.IsRaised();
                switch (isLowMemory)
                {
                    case true:
                        if (_sendQueue.Count < 16 * 1024)
                        {
                            // we are in low memory state and the number of messages in the queue is reasonable.
                            // continue waiting for the send task to complete.
                            continue;
                        }

                        break;

                    case false:
                        if (_sendQueue.Count < 128 * 1024)
                        {
                            // we aren't in low memory state and the number of pending messages is less than 128K.
                            // continue waiting for the send task to complete.
                            continue;
                        }

                        break;
                }

                // - we waited for some time for the send task to complete,
                // - we are in low memory state and the number of messages waiting in the queue exceeds 16K
                // - OR we have 128K messages waiting in the queue.
                // - we'll close the WebSocket connection and let the client reconnect again.

                try
                {
                    CancellationToken.Cancel();
                }
                catch (ObjectDisposedException)
                {
                    // the connection was already disposed
                }

                throw new TimeoutException($"Waited for {sp.Elapsed} to send {messagesCount:#,#;;0} messages to the client but was unsuccessful " +
                                           $"(total size: {new Sparrow.Size(ms.Length, SizeUnit.Bytes)}), " +
                                           $"low memory state: {isLowMemory} and there are {_sendQueue.Count:#,#;;0} messages waiting in the queue. " +
                                           $"Changes connection from studio: {IsChangesConnectionOriginatedFromStudio}. " +
                                           $"Closing the changes WebSocket and letting the client reconnect again.");
            }
        }

        private object _skippedMessage;
        private DateTime _lastSendMessage;

        private async Task<object> GetNextMessage(bool throttleConnection)
        {
            while (true)
            {
                var nextMessage = await _sendQueue.TryDequeueAsync(TimeSpan.FromSeconds(5));
                if (nextMessage.Item1 == false)
                {
                    var dynamicJsonValue = _skippedMessage;
                    _skippedMessage = null;
                    return dynamicJsonValue;
                }
                var msg = nextMessage.Item2;
                if (throttleConnection && msg.AllowSkip)
                {
                    if (DateTime.UtcNow - _lastSendMessage < TimeSpan.FromSeconds(5))
                    {
                        _skippedMessage = msg.ValueToSend;
                        continue;
                    }
                }
                _skippedMessage = null;
                _lastSendMessage = DateTime.UtcNow;
                return msg.ValueToSend;
            }
        }

        public bool IsChangesConnectionOriginatedFromStudio { get; }

        private SingleUseFlag _isDisposed = new SingleUseFlag();

        public bool IsDisposed => _isDisposed.IsRaised();

        public void Dispose()
        {
            _isDisposed.Raise();
            CancellationToken.Cancel();
            _sendQueue.Enqueue(new ChangeValue
            {
                AllowSkip = false,
                ValueToSend = null
            });
            CancellationToken.Dispose();
        }

        public void Confirm(int commandId)
        {
            _sendQueue.Enqueue(new ChangeValue
            {
                ValueToSend = new DynamicJsonValue
                {
                    ["CommandId"] = commandId,
                    ["Type"] = "Confirm"
                },
                AllowSkip = false
            });
        }

        public void SendSupportedFeatures()
        {
            _sendQueue.Enqueue(new ChangeValue
            {
                ValueToSend = new DynamicJsonValue
                {
                    [nameof(ChangesSupportedFeatures.TopologyChange)] = true,
                    [nameof(ChangesSupportedFeatures.AggressiveCachingChange)] = true,
                },
                AllowSkip = false
            });
        }

        public void HandleCommand(string command, string commandParameter, BlittableJsonReaderArray commandParameters)
        {
            long.TryParse(commandParameter, out long commandParameterAsLong);

            if (Match(command, "watch-index"))
            {
                WatchIndex(commandParameter);
            }
            else if (Match(command, "unwatch-index"))
            {
                UnwatchIndex(commandParameter);
            }
            else if (Match(command, "watch-indexes"))
            {
                WatchAllIndexes();
            }
            else if (Match(command, "unwatch-indexes"))
            {
                UnwatchAllIndexes();
            }
            else if (Match(command, "watch-doc"))
            {
                WatchDocument(commandParameter);
            }
            else if (Match(command, "unwatch-doc"))
            {
                UnwatchDocument(commandParameter);
            }
            else if (Match(command, "watch-docs"))
            {
                WatchAllDocuments();
            }
            else if (Match(command, "unwatch-docs"))
            {
                UnwatchAllDocuments();
            }
            else if (Match(command, "watch-prefix"))
            {
                WatchDocumentPrefix(commandParameter);
            }
            else if (Equals(command, "unwatch-prefix"))
            {
                UnwatchDocumentPrefix(commandParameter);
            }
            else if (Match(command, "watch-collection"))
            {
                WatchDocumentInCollection(commandParameter);
            }
            else if (Equals(command, "unwatch-collection"))
            {
                UnwatchDocumentInCollection(commandParameter);
            }
            else if (Match(command, "watch-type"))
            {
                WatchDocumentOfType(commandParameter);
            }
            else if (Equals(command, "unwatch-type"))
            {
                UnwatchDocumentOfType(commandParameter);
            }
            else if (Equals(command, "watch-operation"))
            {
                WatchOperation(commandParameterAsLong);
            }
            else if (Equals(command, "unwatch-operation"))
            {
                UnwatchOperation(commandParameterAsLong);
            }
            else if (Equals(command, "watch-operations"))
            {
                WatchAllOperations();
            }
            else if (Equals(command, "unwatch-operations"))
            {
                UnwatchAllOperations();
            }
            else if (Match(command, "watch-counters"))
            {
                WatchAllCounters();
            }
            else if (Match(command, "unwatch-counters"))
            {
                UnwatchAllCounters();
            }
            else if (Match(command, "watch-counter"))
            {
                WatchCounter(commandParameter);
            }
            else if (Match(command, "unwatch-counter"))
            {
                UnwatchCounter(commandParameter);
            }
            else if (Match(command, "watch-document-counters"))
            {
                WatchDocumentCounters(commandParameter);
            }
            else if (Match(command, "unwatch-document-counters"))
            {
                UnwatchDocumentCounters(commandParameter);
            }
            else if (Match(command, "watch-document-counter"))
            {
                WatchDocumentCounter(commandParameters);
            }
            else if (Match(command, "unwatch-document-counter"))
            {
                UnwatchDocumentCounter(commandParameters);
            }
            else if (Match(command, "watch-all-timeseries"))
            {
                WatchAllTimeSeries();
            }
            else if (Match(command, "unwatch-all-timeseries"))
            {
                UnwatchAllTimeSeries();
            }
            else if (Match(command, "watch-timeseries"))
            {
                WatchTimeSeries(commandParameter);
            }
            else if (Match(command, "unwatch-timeseries"))
            {
                UnwatchTimeSeries(commandParameter);
            }
            else if (Match(command, "watch-all-document-timeseries"))
            {
                WatchAllDocumentTimeSeries(commandParameter);
            }
            else if (Match(command, "unwatch-all-document-timeseries"))
            {
                UnwatchAllDocumentTimeSeries(commandParameter);
            }
            else if (Match(command, "watch-document-timeseries"))
            {
                WatchDocumentTimeSeries(commandParameters);
            }
            else if (Match(command, "unwatch-document-timeseries"))
            {
                UnwatchDocumentTimeSeries(commandParameters);
            }
            else if (Match(command, "watch-topology-change"))
            {
                WatchTopology();
            }
            else if (Match(command, "watch-aggressive-caching"))
            {
                _aggressiveChanges = true;
            }
            else if (Match(command, "unwatch-aggressive-caching"))
            {
                _aggressiveChanges = false;
            }
            else
            {
                throw new ArgumentOutOfRangeException(nameof(command), "Command argument is not valid");
            }
        }

        protected static bool Match(string x, string y)
        {
            return string.Equals(x, y, StringComparison.OrdinalIgnoreCase);
        }

        public DynamicJsonValue GetDebugInfo()
        {
            return new DynamicJsonValue
            {
                ["Id"] = Id,
                ["PendingMessagesCount"] = _sendQueue.Count,
                ["State"] = _webSocket.State.ToString(),
                ["CloseStatus"] = _webSocket.CloseStatus,
                ["CloseStatusDescription"] = _webSocket.CloseStatusDescription,
                ["SubProtocol"] = _webSocket.SubProtocol,
                ["Age"] = Age,
                ["WatchAllDocuments"] = _watchAllDocuments > 0,
                ["WatchAllIndexes"] = _watchAllIndexes > 0,
                ["WatchAllCounters"] = _watchAllCounters > 0,
                ["WatchAllTimeSeries"] = _watchAllTimeSeries > 0,
                ["WatchAllOperations"] = _watchAllOperations > 0,
                ["WatchDocumentPrefixes"] = _matchingDocumentPrefixes.ToArray(),
                ["WatchDocumentsInCollection"] = _matchingDocumentsInCollection.ToArray(),
                ["WatchIndexes"] = _matchingIndexes.ToArray(),
                ["WatchDocuments"] = _matchingDocuments.ToArray(),
                ["WatchCounters"] = _matchingCounters.ToArray(),
                ["WatchCounterOfDocument"] = _matchingDocumentCounter.Select(x => x.ToJson()).ToArray(),
                ["WatchCountersOfDocument"] = _matchingDocumentCounters.ToArray(),
                ["WatchTimeSeries"] = _matchingTimeSeries.ToArray(),
                ["WatchTimeSeriesOfDocument"] = _matchingDocumentTimeSeries.Select(x => x.ToJson()).ToArray(),
                ["WatchAllTimeSeriesOfDocument"] = _matchingAllDocumentTimeSeries.ToArray()
            };
        }

        private static DocumentIdAndNamePair GetParameters(BlittableJsonReaderArray parameters)
        {
            if (parameters == null)
                throw new ArgumentNullException(nameof(parameters));

            if (parameters.Length != 2)
                throw new InvalidOperationException("Expected to get 2 parameters, but got " + parameters.Length);

            return new DocumentIdAndNamePair(parameters[0].ToString(), parameters[1].ToString());
        }

        private struct DocumentIdAndNamePair
        {
            public DocumentIdAndNamePair(string documentId, string name)
            {
                DocumentId = documentId;
                Name = name;
            }

            public readonly string DocumentId;

            public readonly string Name;

            private bool Equals(DocumentIdAndNamePair other)
            {
                return string.Equals(DocumentId, other.DocumentId, StringComparison.OrdinalIgnoreCase)
                       && string.Equals(Name, other.Name, StringComparison.OrdinalIgnoreCase);
            }

            public override bool Equals(object obj)
            {
                if (ReferenceEquals(null, obj))
                    return false;

                return obj is DocumentIdAndNamePair pair && Equals(pair);
            }

            public override int GetHashCode()
            {
                unchecked
                {
                    return ((DocumentId != null ? StringComparer.OrdinalIgnoreCase.GetHashCode(DocumentId) : 0) * 397)
                           ^ (Name != null ? StringComparer.OrdinalIgnoreCase.GetHashCode(Name) : 0);
                }
            }

            public DynamicJsonValue ToJson()
            {
                return new DynamicJsonValue
                {
                    [nameof(DocumentId)] = DocumentId,
                    [nameof(Name)] = Name
                };
            }
        }

        public void LowMemory(LowMemorySeverity lowMemorySeverity)
        {
            _lowMemoryFlag.Raise();
        }

        public void LowMemoryOver()
        {
            _lowMemoryFlag.Lower();
        }

        private abstract class DatabaseChangeFactory
        {
            public abstract DynamicJsonValue CreateJson();
        }

        private class AggressiveCacheChangeFactory : DatabaseChangeFactory
        {
            public override DynamicJsonValue CreateJson()
            {
                return new DynamicJsonValue
                {
                    ["Type"] = nameof(AggressiveCacheChange),
                };
            }
        }
    }
}
