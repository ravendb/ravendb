using System;
using System.Diagnostics;
using System.IO;
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
using Sparrow.LowMemory;
using Sparrow.Server.Collections;
using Sparrow.Threading;
using Sparrow.Utils;

namespace Raven.Server.Documents.Changes;

public abstract class AbstractChangesClientConnection<TOperationContext> : ILowMemoryHandler, IDisposable
    where TOperationContext : JsonOperationContext
{
    private readonly WebSocket _webSocket;
    private readonly AsyncQueue<SendQueueItem> _sendQueue = new();

    private readonly MultipleUseFlag _lowMemoryFlag = new();

    private readonly CancellationTokenSource _cts;
    public CancellationToken DisposeToken => _cts.Token;

    private readonly DateTime _startedAt;

    private object _skippedMessage;
    private DateTime _lastSendMessage;

    protected readonly JsonContextPoolBase<TOperationContext> ContextPool;
    private readonly bool _throttleConnection;

    private readonly ConcurrentSet<long> _matchingOperations = new();

    private int _watchAllOperations;

    private bool _watchTopology;

    protected AbstractChangesClientConnection(WebSocket webSocket, JsonContextPoolBase<TOperationContext> contextPool, CancellationToken databaseShutdown, bool throttleConnection, bool fromStudio)
    {
        Id = ChangesClientConnectionId.GetNextId();
        IsChangesConnectionOriginatedFromStudio = fromStudio;
        ContextPool = contextPool;
        _throttleConnection = throttleConnection;
        _webSocket = webSocket;
        _startedAt = SystemTime.UtcNow;
        _cts = CancellationTokenSource.CreateLinkedTokenSource(databaseShutdown);
    }

    public readonly long Id;

    public TimeSpan Age => SystemTime.UtcNow - _startedAt;

    private ValueTask WatchTopologyAsync()
    {
        _watchTopology = true;
        return ValueTask.CompletedTask;
    }

    protected abstract ValueTask WatchDocumentAsync(string docId, CancellationToken token);

    protected abstract ValueTask UnwatchDocumentAsync(string docId, CancellationToken token);

    protected abstract ValueTask WatchAllDocumentsAsync(CancellationToken token);

    protected abstract ValueTask UnwatchAllDocumentsAsync(CancellationToken token);

    protected abstract ValueTask WatchCounterAsync(string name, CancellationToken token);

    protected abstract ValueTask UnwatchCounterAsync(string name, CancellationToken token);

    protected abstract ValueTask WatchDocumentCountersAsync(string docId, CancellationToken token);

    protected abstract ValueTask UnwatchDocumentCountersAsync(string docId, CancellationToken token);

    protected abstract ValueTask WatchDocumentCounterAsync(BlittableJsonReaderArray parameters, CancellationToken token);

    protected abstract ValueTask UnwatchDocumentCounterAsync(BlittableJsonReaderArray parameters, CancellationToken token);

    protected abstract ValueTask WatchAllCountersAsync(CancellationToken token);

    protected abstract ValueTask UnwatchAllCountersAsync(CancellationToken token);

    protected abstract ValueTask WatchTimeSeriesAsync(string name, CancellationToken token);

    protected abstract ValueTask UnwatchTimeSeriesAsync(string name, CancellationToken token);

    protected abstract ValueTask WatchAllDocumentTimeSeriesAsync(string docId, CancellationToken token);

    protected abstract ValueTask UnwatchAllDocumentTimeSeriesAsync(string docId, CancellationToken token);

    protected abstract ValueTask WatchDocumentTimeSeriesAsync(BlittableJsonReaderArray parameters, CancellationToken token);

    protected abstract ValueTask UnwatchDocumentTimeSeriesAsync(BlittableJsonReaderArray parameters, CancellationToken token);

    protected abstract ValueTask WatchAllTimeSeriesAsync(CancellationToken token);

    protected abstract ValueTask UnwatchAllTimeSeriesAsync(CancellationToken token);

    protected abstract ValueTask WatchDocumentPrefixAsync(string name, CancellationToken token);

    protected abstract ValueTask UnwatchDocumentPrefixAsync(string name, CancellationToken token);

    protected abstract ValueTask WatchDocumentInCollectionAsync(string name, CancellationToken token);

    protected abstract ValueTask UnwatchDocumentInCollectionAsync(string name, CancellationToken token);

    protected abstract ValueTask WatchAllIndexesAsync(CancellationToken token);

    protected abstract ValueTask UnwatchAllIndexesAsync(CancellationToken token);

    protected abstract ValueTask WatchIndexAsync(string name, CancellationToken token);

    protected abstract ValueTask UnwatchIndexAsync(string name, CancellationToken token);

    protected abstract ValueTask WatchAggressiveCachingAsync(CancellationToken token);

    protected abstract ValueTask UnwatchAggressiveCachingAsync(CancellationToken token);

    private ValueTask WatchOperationAsync(long operationId)
    {
        _matchingOperations.TryAdd(operationId);
        return ValueTask.CompletedTask;
    }

    private ValueTask UnwatchOperationAsync(long operationId)
    {
        _matchingOperations.TryRemove(operationId);
        return ValueTask.CompletedTask;
    }

    private ValueTask WatchAllOperationsAsync()
    {
        Interlocked.Increment(ref _watchAllOperations);
        return ValueTask.CompletedTask;
    }

    private ValueTask UnwatchAllOperationsAsync()
    {
        Interlocked.Decrement(ref _watchAllOperations);
        return ValueTask.CompletedTask;
    }

    public async Task StartSendingNotificationsAsync()
    {
        using (ContextPool.AllocateOperationContext(out JsonOperationContext context))
        {
            await using (var ms = RecyclableMemoryStreamFactory.GetMemoryStream())
            {
                var sp = Stopwatch.StartNew();
                var sendTaskSp = Stopwatch.StartNew();

                while (true)
                {
                    if (DisposeToken.IsCancellationRequested)
                        break;

                    ms.SetLength(0);
                    context.Reset();
                    context.Renew();

                    var messagesCount = 0;
                    await using (var writer = new AsyncBlittableJsonTextWriter(context, ms))
                    {
                        sp.Restart();

                        var first = true;
                        writer.WriteStartArray();

                        do
                        {
                            using var message = await GetNextMessage();
                            var value = message.Value;
                            if (value == null || DisposeToken.IsCancellationRequested)
                                break;

                            if (first == false)
                                writer.WriteComma();

                            first = false;

                            switch (value)
                            {
                                case DynamicJsonValue djv:
                                    context.Write(writer, djv);
                                    break;
                                case BlittableJsonReaderObject bjro:
                                    context.Write(writer, bjro.CloneForConcurrentRead(context));
                                    break;
                                case ChangesClientConnection.DatabaseChangeFactory cf:
                                    context.Write(writer, cf.CreateJson());
                                    break;
                            }
                            messagesCount++;
                            await writer.FlushAsync(DisposeToken);

                            if (ms.Length > 16 * 1024)
                                break;
                        } while (_sendQueue.IsEmpty == false && sp.Elapsed < TimeSpan.FromSeconds(5));

                        writer.WriteEndArray();
                    }

                    if (DisposeToken.IsCancellationRequested)
                        break;

                    ms.TryGetBuffer(out ArraySegment<byte> bytes);
                    var sendTask = _webSocket.SendAsync(bytes, WebSocketMessageType.Text, true, DisposeToken);
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
            var waitTask = TimeoutManager.WaitFor(TimeSpan.FromSeconds(5), DisposeToken);

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
                _cts.Cancel();
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

    protected virtual Message CreateMessage(object message) => new Message(message, onDispose: null);

    private async ValueTask<Message> GetNextMessage()
    {
        while (true)
        {
            var nextMessage = await _sendQueue.TryDequeueAsync(TimeSpan.FromSeconds(5));
            if (nextMessage.Item1 == false)
            {
                var dynamicJsonValue = _skippedMessage;
                _skippedMessage = null;
                return CreateMessage(dynamicJsonValue);
            }
            var msg = nextMessage.Item2;
            if (_throttleConnection && msg.AllowSkip)
            {
                if (DateTime.UtcNow - _lastSendMessage < TimeSpan.FromSeconds(5))
                {
                    _skippedMessage = msg.ValueToSend;
                    continue;
                }
            }
            _skippedMessage = null;
            _lastSendMessage = DateTime.UtcNow;
            return CreateMessage(msg.ValueToSend);
        }
    }

    public bool IsChangesConnectionOriginatedFromStudio { get; }

    private readonly SingleUseFlag _isDisposed = new();

    public bool IsDisposed => _isDisposed.IsRaised();

    public virtual void Dispose()
    {
        _isDisposed.Raise();
        _cts.Cancel();
        _sendQueue.Enqueue(new SendQueueItem
        {
            AllowSkip = false,
            ValueToSend = null
        });
        _cts.Dispose();
    }

    public void Confirm(int commandId)
    {
        AddToQueue(new SendQueueItem
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
        AddToQueue(new SendQueueItem
        {
            ValueToSend = new DynamicJsonValue
            {
                [nameof(ChangesSupportedFeatures.TopologyChange)] = true,
                [nameof(ChangesSupportedFeatures.AggressiveCachingChange)] = true,
            },
            AllowSkip = false
        });
    }

    protected void AddToQueue(SendQueueItem item, bool addIfEmpty = false)
    {
        if (DisposeToken.IsCancellationRequested)
            return;

        if (addIfEmpty)
        {
            _sendQueue.AddIfEmpty(item);
            return;
        }

        _sendQueue.Enqueue(item);
    }

    public async ValueTask HandleCommandAsync(string command, string commandParameter, BlittableJsonReaderArray commandParameters, CancellationToken token)
    {
        long.TryParse(commandParameter, out long commandParameterAsLong);

        if (Match(command, "watch-index"))
        {
            await WatchIndexAsync(commandParameter, token);
        }
        else if (Match(command, "unwatch-index"))
        {
            await UnwatchIndexAsync(commandParameter, token);
        }
        else if (Match(command, "watch-indexes"))
        {
            await WatchAllIndexesAsync(token);
        }
        else if (Match(command, "unwatch-indexes"))
        {
            await UnwatchAllIndexesAsync(token);
        }
        else if (Match(command, "watch-doc"))
        {
            await WatchDocumentAsync(commandParameter, token);
        }
        else if (Match(command, "unwatch-doc"))
        {
            await UnwatchDocumentAsync(commandParameter, token);
        }
        else if (Match(command, "watch-docs"))
        {
            await WatchAllDocumentsAsync(token);
        }
        else if (Match(command, "unwatch-docs"))
        {
            await UnwatchAllDocumentsAsync(token);
        }
        else if (Match(command, "watch-prefix"))
        {
            await WatchDocumentPrefixAsync(commandParameter, token);
        }
        else if (Equals(command, "unwatch-prefix"))
        {
            await UnwatchDocumentPrefixAsync(commandParameter, token);
        }
        else if (Match(command, "watch-collection"))
        {
            await WatchDocumentInCollectionAsync(commandParameter, token);
        }
        else if (Equals(command, "unwatch-collection"))
        {
            await UnwatchDocumentInCollectionAsync(commandParameter, token);
        }
        else if (Equals(command, "watch-operation"))
        {
            await WatchOperationAsync(commandParameterAsLong);
        }
        else if (Equals(command, "unwatch-operation"))
        {
            await UnwatchOperationAsync(commandParameterAsLong);
        }
        else if (Equals(command, "watch-operations"))
        {
            await WatchAllOperationsAsync();
        }
        else if (Equals(command, "unwatch-operations"))
        {
            await UnwatchAllOperationsAsync();
        }
        else if (Match(command, "watch-counters"))
        {
            await WatchAllCountersAsync(token);
        }
        else if (Match(command, "unwatch-counters"))
        {
            await UnwatchAllCountersAsync(token);
        }
        else if (Match(command, "watch-counter"))
        {
            await WatchCounterAsync(commandParameter, token);
        }
        else if (Match(command, "unwatch-counter"))
        {
            await UnwatchCounterAsync(commandParameter, token);
        }
        else if (Match(command, "watch-document-counters"))
        {
            await WatchDocumentCountersAsync(commandParameter, token);
        }
        else if (Match(command, "unwatch-document-counters"))
        {
            await UnwatchDocumentCountersAsync(commandParameter, token);
        }
        else if (Match(command, "watch-document-counter"))
        {
            await WatchDocumentCounterAsync(commandParameters, token);
        }
        else if (Match(command, "unwatch-document-counter"))
        {
            await UnwatchDocumentCounterAsync(commandParameters, token);
        }
        else if (Match(command, "watch-all-timeseries"))
        {
            await WatchAllTimeSeriesAsync(token);
        }
        else if (Match(command, "unwatch-all-timeseries"))
        {
            await UnwatchAllTimeSeriesAsync(token);
        }
        else if (Match(command, "watch-timeseries"))
        {
            await WatchTimeSeriesAsync(commandParameter, token);
        }
        else if (Match(command, "unwatch-timeseries"))
        {
            await UnwatchTimeSeriesAsync(commandParameter, token);
        }
        else if (Match(command, "watch-all-document-timeseries"))
        {
            await WatchAllDocumentTimeSeriesAsync(commandParameter, token);
        }
        else if (Match(command, "unwatch-all-document-timeseries"))
        {
            await UnwatchAllDocumentTimeSeriesAsync(commandParameter, token);
        }
        else if (Match(command, "watch-document-timeseries"))
        {
            await WatchDocumentTimeSeriesAsync(commandParameters, token);
        }
        else if (Match(command, "unwatch-document-timeseries"))
        {
            await UnwatchDocumentTimeSeriesAsync(commandParameters, token);
        }
        else if (Match(command, "watch-topology-change"))
        {
            await WatchTopologyAsync();
        }
        else if (Match(command, "watch-aggressive-caching"))
        {
            await WatchAggressiveCachingAsync(token);
        }
        else if (Match(command, "unwatch-aggressive-caching"))
        {
            await UnwatchAggressiveCachingAsync(token);
        }
        else
        {
            throw new ArgumentOutOfRangeException(nameof(command), "Command argument is not valid");
        }
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

    private static bool Match(string x, string y)
    {
        return string.Equals(x, y, StringComparison.OrdinalIgnoreCase);
    }

    public virtual DynamicJsonValue GetDebugInfo()
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
            ["WatchAllOperations"] = _watchAllOperations > 0
        };
    }

    public void LowMemory(LowMemorySeverity lowMemorySeverity)
    {
        _lowMemoryFlag.Raise();
    }

    public void LowMemoryOver()
    {
        _lowMemoryFlag.Lower();
    }

    protected static DynamicJsonValue CreateValueToSend(string type, DynamicJsonValue value)
    {
        return new DynamicJsonValue
        {
            ["Type"] = type,
            ["Value"] = value
        };
    }

    protected static DocumentIdAndNamePair GetParameters(BlittableJsonReaderArray parameters)
    {
        if (parameters == null)
            throw new ArgumentNullException(nameof(parameters));

        if (parameters.Length != 2)
            throw new InvalidOperationException("Expected to get 2 parameters, but got " + parameters.Length);

        return new DocumentIdAndNamePair(parameters[0].ToString(), parameters[1].ToString());
    }

    protected readonly struct DocumentIdAndNamePair
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

    protected internal sealed class SendQueueItem
    {
        public object ValueToSend;
        public bool AllowSkip;
    }

    protected readonly struct Message : IDisposable
    {
        public readonly object Value;
        private readonly Action<object> _onDispose;

        public Message(object value, Action<object> onDispose)
        {
            Value = value;
            _onDispose = onDispose;
        }

        public void Dispose() => _onDispose?.Invoke(Value);
    }

    protected abstract class DatabaseChangeFactory
    {
        public abstract DynamicJsonValue CreateJson();
    }
}
