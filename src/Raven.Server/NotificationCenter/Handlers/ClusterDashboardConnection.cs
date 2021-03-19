// -----------------------------------------------------------------------
//  <copyright file="ClusterDashboardConnection.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Concurrent;
using System.IO;
using System.Net.WebSockets;
using System.Threading;
using System.Threading.Tasks;
using Raven.Server.ClusterDashboard;
using Raven.Server.ClusterDashboard.Widgets;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Logging;
using Sparrow.Server.Collections;
using Sparrow.Server.Json.Sync;

namespace Raven.Server.NotificationCenter.Handlers
{
    public class ClusterDashboardConnection : IDisposable
    {
        private static readonly Logger Logger = LoggingSource.Instance.GetLogger<ClusterDashboardConnection>("Server");

        private const string _watchCommand = "watch";
        private const string _unwatchCommand = "unwatch";

        private readonly JsonOperationContext _writeContext;
        private readonly JsonOperationContext _readContext;
        
        private readonly CancellationTokenSource _cts;
        private readonly CancellationToken _disposeToken;

        private readonly RavenServer _server;
        private readonly WebSocket _webSocket;
        private readonly AsyncQueue<DynamicJsonValue> _sendQueue = new AsyncQueue<DynamicJsonValue>();
        private readonly ConcurrentDictionary<int, Widget> _widgets = new ConcurrentDictionary<int, Widget>();

        private Task _receiveTask;
        private readonly MemoryStream _ms = new MemoryStream();
        
        public ClusterDashboardConnection(
            RavenServer server,
            WebSocket webSocket, 
            JsonOperationContext writeContext, 
            JsonOperationContext readContext, 
            CancellationToken token)
        {
            _server = server;
            _webSocket = webSocket;
            _writeContext = writeContext;
            _readContext = readContext;
            _cts = CancellationTokenSource.CreateLinkedTokenSource(token);
            _disposeToken = _cts.Token;
        }

        public async Task Handle()
        {
            _receiveTask = ListenForCommands();
            await StartSendingNotifications();
            await _receiveTask;
        }
        
        private async Task StartSendingNotifications()
        {
            try
            {
                while (_disposeToken.IsCancellationRequested == false)
                {
                    // we use this to detect client-initialized closure
                    if (_receiveTask != null && _receiveTask.IsCompleted)
                    {
                        break;
                    }

                    var tuple = await _sendQueue.TryDequeueAsync(TimeSpan.FromSeconds(5));
                    if (tuple.Item1 == false)
                    {
                        await SendHeartbeat();
                        continue;
                    }

                    await WriteToWebSocket(tuple.Item2);
                }
            }
            catch (OperationCanceledException)
            {
            }
        }
        
        private async Task SendHeartbeat()
        {
            await _webSocket.SendAsync(WebSocketHelper.Heartbeat, WebSocketMessageType.Text, true, _disposeToken);
        }

        private void WatchCommand(int widgetId, WidgetType type, BlittableJsonReaderObject configuration)
        {
            Widget widget;
            switch (type)
            {
                //TODO: refactor to avoid repeats?
                case WidgetType.CpuUsage:
                    widget = new CpuUsageWidget(widgetId, _server, msg => EnqueueMessage(widgetId, msg), _disposeToken);
                    break;
                case WidgetType.Traffic:
                    //TODO: widget = new TrafficWidget(widgetId, OnMessage, _disposeToken);
                    widget = null; //TODO:
                    break;
                case WidgetType.MemoryUsage:
                    widget = new MemoryUsageWidget(widgetId, _server, msg => EnqueueMessage(widgetId, msg), _disposeToken);
                    break;
                case WidgetType.Storage:
                    widget = new StorageWidget(widgetId, _server, msg => EnqueueMessage(widgetId, msg), _disposeToken);
                    break;
                case WidgetType.Debug:
                    // ignore this command
                    return;
                default:
                    throw new ArgumentOutOfRangeException();
            }

            widget.Start();

            if (_widgets.TryAdd(widgetId, widget) == false)
            {
                throw new ArgumentException($"Widget with id = {widgetId} already exists.");
            }
        }

        private void EnqueueMessage<T>(int widgetId, T data) where T : IDynamicJson
        {
            _sendQueue.Enqueue(new DynamicJsonValue
            {
                [nameof(WidgetMessage.Id)] = widgetId,
                [nameof(WidgetMessage.Data)] = data.ToJson()
            });
        }

        private void UnwatchCommand(int widgetId)
        {
            if (_widgets.TryRemove(widgetId, out var widget)) 
            {
                widget.Dispose();
            }
        }

        public void Dispose()
        {
            _cts.Cancel();
            using (_ms)
            using (_cts)
            {
            }

            foreach (Widget widget in _widgets.Values)
            {
                widget.Dispose();
            }
            _widgets.Clear();
        }

        private async Task ListenForCommands()
        {
            using (_readContext.GetMemoryBuffer(out JsonOperationContext.MemoryBuffer segment1))
            using (_readContext.GetMemoryBuffer(out JsonOperationContext.MemoryBuffer segment2))
            {
                try
                {
                    var segments = new[] {segment1, segment2};
                    int index = 0;
                    var receiveAsync = _webSocket.ReceiveAsync(segments[index].Memory.Memory, _disposeToken);
                    var jsonParserState = new JsonParserState();
                    using (var parser = new UnmanagedJsonParser(_readContext, jsonParserState, "cluster-dashboard"))
                    {
                        var result = await receiveAsync;
                        _disposeToken.ThrowIfCancellationRequested();

                        parser.SetBuffer(segments[index], 0, result.Count);
                        index++;
                        receiveAsync = _webSocket.ReceiveAsync(segments[index].Memory.Memory, _disposeToken);

                        while (true)
                        {
                            using (var builder =
                                new BlittableJsonDocumentBuilder(_readContext, BlittableJsonDocumentBuilder.UsageMode.None, "cluster-dashboard",
                                    parser, jsonParserState))
                            {
                                parser.NewDocument();
                                builder.ReadObjectDocument();

                                while (builder.Read() == false)
                                {
                                    result = await receiveAsync;
                                    _disposeToken.ThrowIfCancellationRequested();

                                    parser.SetBuffer(segments[index], 0, result.Count);
                                    if (++index >= segments.Length)
                                        index = 0;
                                    receiveAsync = _webSocket.ReceiveAsync(segments[index].Memory.Memory, _disposeToken);
                                }

                                builder.FinalizeDocument();

                                using (var reader = builder.CreateReader())
                                {
                                    HandleCommand(reader);
                                }
                            }
                        }
                    }
                }
                catch (IOException ex)
                {
                    /* Client was disconnected, write to log */
                    if (Logger.IsInfoEnabled)
                        Logger.Info("Client was disconnected", ex);
                }
                catch (Exception ex)
                {
                    // if we received close from the client, we want to ignore it and close the websocket (dispose does it)
                    if (ex is WebSocketException webSocketException
                        && webSocketException.WebSocketErrorCode == WebSocketError.InvalidState
                        && _webSocket.State == WebSocketState.CloseReceived)
                    {
                        // ignore
                    }
                    else
                    {
                        throw;
                    }
                }
            }

            _disposeToken.ThrowIfCancellationRequested();
        }

        private Task WriteToWebSocket(DynamicJsonValue notification)
        {
            _writeContext.Reset();
            _writeContext.Renew();

            _ms.SetLength(0);

            using (var writer = new BlittableJsonTextWriter(_writeContext, _ms))
            {
                _writeContext.Write(writer, notification);
            }

            _ms.TryGetBuffer(out ArraySegment<byte> bytes);

            return _webSocket.SendAsync(bytes, WebSocketMessageType.Text, true, _disposeToken);
        }

        private void HandleCommand(BlittableJsonReaderObject reader)
        {
            if (reader.TryGet(nameof(WidgetRequest.Command), out string command) == false)
                throw new ArgumentNullException(nameof(command), "Command argument is mandatory");
            if (reader.TryGet(nameof(WidgetRequest.Id), out int id) == false)
                throw new ArgumentNullException(nameof(command), "Id argument is mandatory");

            switch (command)
            {
                case _watchCommand:
                    if (reader.TryGet(nameof(WidgetRequest.Type), out WidgetType type) == false)
                        throw new ArgumentNullException(nameof(command), "Type argument is mandatory");
                    reader.TryGet(nameof(WidgetRequest.Config), out BlittableJsonReaderObject configuration);
                    WatchCommand(id, type, configuration);
                    break;
                case _unwatchCommand:
                    UnwatchCommand(id);
                    break;
                default:
                    throw new NotSupportedException($"Unhandled command: {command}");
            }
        }
    }
}
