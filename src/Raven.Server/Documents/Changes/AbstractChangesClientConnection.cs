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

    protected abstract ValueTask WatchDocumentAsync(string docId);

    protected abstract ValueTask UnwatchDocumentAsync(string docId);

    protected abstract ValueTask WatchAllDocumentsAsync();

    protected abstract ValueTask UnwatchAllDocumentsAsync();

    protected abstract ValueTask WatchCounterAsync(string name);

    protected abstract ValueTask UnwatchCounterAsync(string name);

    protected abstract ValueTask WatchDocumentCountersAsync(string docId);

    protected abstract ValueTask UnwatchDocumentCountersAsync(string docId);

    protected abstract ValueTask WatchDocumentCounterAsync(BlittableJsonReaderArray parameters);

    protected abstract ValueTask UnwatchDocumentCounterAsync(BlittableJsonReaderArray parameters);

    protected abstract ValueTask WatchAllCountersAsync();

    protected abstract ValueTask UnwatchAllCountersAsync();

    protected abstract ValueTask WatchTimeSeriesAsync(string name);

    protected abstract ValueTask UnwatchTimeSeriesAsync(string name);

    protected abstract ValueTask WatchAllDocumentTimeSeriesAsync(string docId);

    protected abstract ValueTask UnwatchAllDocumentTimeSeriesAsync(string docId);

    protected abstract ValueTask WatchDocumentTimeSeriesAsync(BlittableJsonReaderArray parameters);

    protected abstract ValueTask UnwatchDocumentTimeSeriesAsync(BlittableJsonReaderArray parameters);

    protected abstract ValueTask WatchAllTimeSeriesAsync();

    protected abstract ValueTask UnwatchAllTimeSeriesAsync();

    protected abstract ValueTask WatchDocumentPrefixAsync(string name);

    protected abstract ValueTask UnwatchDocumentPrefixAsync(string name);

    protected abstract ValueTask WatchDocumentInCollectionAsync(string name);

    protected abstract ValueTask UnwatchDocumentInCollectionAsync(string name);

    protected abstract ValueTask WatchDocumentOfTypeAsync(string name);

    protected abstract ValueTask UnwatchDocumentOfTypeAsync(string name);

    protected abstract ValueTask WatchAllIndexesAsync();

    protected abstract ValueTask UnwatchAllIndexesAsync();

    protected abstract ValueTask WatchIndexAsync(string name);

    protected abstract ValueTask UnwatchIndexAsync(string name);

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

    public async ValueTask HandleCommandAsync(string command, string commandParameter, BlittableJsonReaderArray commandParameters)
    {
        long.TryParse(commandParameter, out long commandParameterAsLong);

        if (Match(command, "watch-index"))
        {
            await WatchIndexAsync(commandParameter);
        }
        else if (Match(command, "unwatch-index"))
        {
            await UnwatchIndexAsync(commandParameter);
        }
        else if (Match(command, "watch-indexes"))
        {
            await WatchAllIndexesAsync();
        }
        else if (Match(command, "unwatch-indexes"))
        {
            await UnwatchAllIndexesAsync();
        }
        else if (Match(command, "watch-doc"))
        {
            await WatchDocumentAsync(commandParameter);
        }
        else if (Match(command, "unwatch-doc"))
        {
            await UnwatchDocumentAsync(commandParameter);
        }
        else if (Match(command, "watch-docs"))
        {
            await WatchAllDocumentsAsync();
        }
        else if (Match(command, "unwatch-docs"))
        {
            await UnwatchAllDocumentsAsync();
        }
        else if (Match(command, "watch-prefix"))
        {
            await WatchDocumentPrefixAsync(commandParameter);
        }
        else if (Equals(command, "unwatch-prefix"))
        {
            await UnwatchDocumentPrefixAsync(commandParameter);
        }
        else if (Match(command, "watch-collection"))
        {
            await WatchDocumentInCollectionAsync(commandParameter);
        }
        else if (Equals(command, "unwatch-collection"))
        {
            await UnwatchDocumentInCollectionAsync(commandParameter);
        }
        else if (Match(command, "watch-type"))
        {
            await WatchDocumentOfTypeAsync(commandParameter);
        }
        else if (Equals(command, "unwatch-type"))
        {
            await UnwatchDocumentOfTypeAsync(commandParameter);
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
            await WatchAllCountersAsync();
        }
        else if (Match(command, "unwatch-counters"))
        {
            await UnwatchAllCountersAsync();
        }
        else if (Match(command, "watch-counter"))
        {
            await WatchCounterAsync(commandParameter);
        }
        else if (Match(command, "unwatch-counter"))
        {
            await UnwatchCounterAsync(commandParameter);
        }
        else if (Match(command, "watch-document-counters"))
        {
            await WatchDocumentCountersAsync(commandParameter);
        }
        else if (Match(command, "unwatch-document-counters"))
        {
            await UnwatchDocumentCountersAsync(commandParameter);
        }
        else if (Match(command, "watch-document-counter"))
        {
            await WatchDocumentCounterAsync(commandParameters);
        }
        else if (Match(command, "unwatch-document-counter"))
        {
            await UnwatchDocumentCounterAsync(commandParameters);
        }
        else if (Match(command, "watch-all-timeseries"))
        {
            await WatchAllTimeSeriesAsync();
        }
        else if (Match(command, "unwatch-all-timeseries"))
        {
            await UnwatchAllTimeSeriesAsync();
        }
        else if (Match(command, "watch-timeseries"))
        {
            await WatchTimeSeriesAsync(commandParameter);
        }
        else if (Match(command, "unwatch-timeseries"))
        {
            await UnwatchTimeSeriesAsync(commandParameter);
        }
        else if (Match(command, "watch-all-document-timeseries"))
        {
            await WatchAllDocumentTimeSeriesAsync(commandParameter);
        }
        else if (Match(command, "unwatch-all-document-timeseries"))
        {
            await UnwatchAllDocumentTimeSeriesAsync(commandParameter);
        }
        else if (Match(command, "watch-document-timeseries"))
        {
            await WatchDocumentTimeSeriesAsync(commandParameters);
        }
        else if (Match(command, "unwatch-document-timeseries"))
        {
            await UnwatchDocumentTimeSeriesAsync(commandParameters);
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

    public DynamicJsonValue GetDebugInfo()
    {
        return new DynamicJsonValue
        {
            ["Id"] = Id,
            ["State"] = _webSocket.State.ToString(),
            ["CloseStatus"] = _webSocket.CloseStatus,
            ["CloseStatusDescription"] = _webSocket.CloseStatusDescription,
            ["SubProtocol"] = _webSocket.SubProtocol,
            ["Age"] = Age,
            //["WatchAllDocuments"] = _watchAllDocuments > 0,
            //["WatchAllIndexes"] = _watchAllIndexes > 0,
            //["WatchAllCounters"] = _watchAllCounters > 0,
            //["WatchAllTimeSeries"] = _watchAllTimeSeries > 0,
            //["WatchAllOperations"] = _watchAllOperations > 0,
            //["WatchDocumentPrefixes"] = _matchingDocumentPrefixes.ToArray(),
            //["WatchDocumentsInCollection"] = _matchingDocumentsInCollection.ToArray(),
            //["WatchIndexes"] = _matchingIndexes.ToArray(),
            //["WatchDocuments"] = _matchingDocuments.ToArray(),
            //["WatchCounters"] = _matchingCounters.ToArray(),
            //["WatchCounterOfDocument"] = _matchingDocumentCounter.Select(x => x.ToJson()).ToArray(),
            //["WatchCountersOfDocument"] = _matchingDocumentCounters.ToArray(),
            //["WatchTimeSeries"] = _matchingTimeSeries.ToArray(),
            //["WatchTimeSeriesOfDocument"] = _matchingDocumentTimeSeries.Select(x => x.ToJson()).ToArray(),
            //["WatchAllTimeSeriesOfDocument"] = _matchingAllDocumentTimeSeries.ToArray()
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
