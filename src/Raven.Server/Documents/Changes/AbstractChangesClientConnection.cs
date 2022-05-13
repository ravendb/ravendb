using System;
using System.Diagnostics;
using System.IO;
using System.Net.WebSockets;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Util;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Server.Collections;
using Sparrow.Threading;

namespace Raven.Server.Documents.Changes;

public abstract class AbstractChangesClientConnection<TOperationContext> : IDisposable
    where TOperationContext : JsonOperationContext
{
    private static long _counter;

    private readonly WebSocket _webSocket;
    private readonly AsyncQueue<SendQueueItem> _sendQueue = new();

    private readonly CancellationTokenSource _cts;
    private readonly CancellationToken _disposeToken;

    private readonly DateTime _startedAt;

    private object _skippedMessage;
    private DateTime _lastSendMessage;

    protected readonly JsonContextPoolBase<TOperationContext> ContextPool;
    private readonly bool _throttleConnection;

    protected AbstractChangesClientConnection(WebSocket webSocket, JsonContextPoolBase<TOperationContext> contextPool, CancellationToken databaseShutdown, bool throttleConnection, bool fromStudio)
    {
        IsChangesConnectionOriginatedFromStudio = fromStudio;
        ContextPool = contextPool;
        _throttleConnection = throttleConnection;
        _webSocket = webSocket;
        _startedAt = SystemTime.UtcNow;
        _cts = CancellationTokenSource.CreateLinkedTokenSource(databaseShutdown);
        _disposeToken = _cts.Token;
    }

    public long Id = Interlocked.Increment(ref _counter);

    public TimeSpan Age => SystemTime.UtcNow - _startedAt;

    protected abstract ValueTask WatchTopologyAsync();

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

    protected abstract ValueTask WatchOperationAsync(long operationId);

    protected abstract ValueTask UnwatchOperationAsync(long operationId);

    protected abstract ValueTask WatchAllOperationsAsync();

    protected abstract ValueTask UnwatchAllOperationsAsync();

    public async Task StartSendingNotificationsAsync()
    {
        using (ContextPool.AllocateOperationContext(out JsonOperationContext context))
        {
            await using (var ms = new MemoryStream())
            {
                var sp = Stopwatch.StartNew();
                while (true)
                {
                    if (_disposeToken.IsCancellationRequested)
                        break;

                    ms.SetLength(0);
                    context.Reset();
                    context.Renew();

                    await using (var writer = new AsyncBlittableJsonTextWriter(context, ms))
                    {
                        sp.Restart();

                        var first = true;
                        writer.WriteStartArray();

                        do
                        {
                            var value = await GetNextMessage();
                            if (value == null || _disposeToken.IsCancellationRequested)
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
                                    context.Write(writer, bjro);
                                    break;
                            }

                            await writer.FlushAsync(_disposeToken);

                            if (ms.Length > 16 * 1024)
                                break;
                        } while (_sendQueue.Count > 0 && sp.Elapsed < TimeSpan.FromSeconds(5));

                        writer.WriteEndArray();
                    }

                    if (_disposeToken.IsCancellationRequested)
                        break;

                    ms.TryGetBuffer(out ArraySegment<byte> bytes);
                    await _webSocket.SendAsync(bytes, WebSocketMessageType.Text, true, _disposeToken);
                }
            }
        }
    }

    private async ValueTask<object> GetNextMessage()
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
            return msg.ValueToSend;
        }
    }

    public bool IsChangesConnectionOriginatedFromStudio { get; }

    private readonly SingleUseFlag _isDisposed = new SingleUseFlag();

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
                ["TopologyChange"] = true
            },
            AllowSkip = false
        });
    }

    protected void AddToQueue(SendQueueItem item)
    {
        if (_disposeToken.IsCancellationRequested)
            return;

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
        else
        {
            throw new ArgumentOutOfRangeException(nameof(command), "Command argument is not valid");
        }
    }

    protected static bool Match(string x, string y)
    {
        return string.Equals(x, y, StringComparison.OrdinalIgnoreCase);
    }

    public virtual DynamicJsonValue GetDebugInfo()
    {
        return new DynamicJsonValue
        {
            ["Id"] = Id,
            ["State"] = _webSocket.State.ToString(),
            ["CloseStatus"] = _webSocket.CloseStatus,
            ["CloseStatusDescription"] = _webSocket.CloseStatusDescription,
            ["SubProtocol"] = _webSocket.SubProtocol,
            ["Age"] = Age
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

    protected class SendQueueItem
    {
        public object ValueToSend;
        public bool AllowSkip;
    }
}
