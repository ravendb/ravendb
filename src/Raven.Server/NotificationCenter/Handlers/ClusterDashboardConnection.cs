using System;
using System.Collections.Concurrent;
using System.IO;
using System.Net.WebSockets;
using System.Threading;
using System.Threading.Tasks;
using Raven.Server.Dashboard;
using Raven.Server.Dashboard.Cluster;
using Raven.Server.Dashboard.Cluster.Notifications;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Logging;

namespace Raven.Server.NotificationCenter.Handlers
{
    public class ClusterDashboardConnection : NotificationCenterWebSocketWriter
    {
        private static readonly Logger Logger = LoggingSource.Instance.GetLogger<ClusterDashboardConnection>(nameof(ClusterDashboardConnection));

        private readonly CanAccessDatabase _canAccessDatabase;
        private readonly ClusterDashboardNotifications _clusterDashboardNotifications;
        private readonly JsonOperationContext _readContext;
        private readonly IDisposable _returnReadContext;
        private readonly ConcurrentDictionary<int, AbstractClusterDashboardNotificationSender> _activeNotificationSenders = new ConcurrentDictionary<int, AbstractClusterDashboardNotificationSender>();

        private Task _receiveTask;

        public ClusterDashboardConnection(WebSocket webSocket, CanAccessDatabase canAccessDatabase, ClusterDashboardNotifications clusterDashboardNotifications,
            IMemoryContextPool contextPool, CancellationToken resourceShutdown)
            : base(webSocket, clusterDashboardNotifications, contextPool, resourceShutdown)
        {
            _canAccessDatabase = canAccessDatabase;
            _clusterDashboardNotifications = clusterDashboardNotifications;
            _returnReadContext = contextPool.AllocateOperationContext(out _readContext);
        }

        public async Task Handle()
        {
            _receiveTask = ListenForCommands();

            await WriteToWebSocket(CreateInitialMessage());

            await WriteNotifications(_canAccessDatabase, taskHandlingReceiveOfData: _receiveTask);

            await _receiveTask;
        }

        private DynamicJsonValue CreateInitialMessage()
        {
            var serverTimePayload = new ServerTimePayload();
            var dataJson = serverTimePayload.ToJson();
            return new DynamicJsonValue
            {
                [nameof(WidgetMessage.Id)] = 0, // special id - for initial message
                [nameof(WidgetMessage.Data)] = dataJson
            };
        }

        private async Task ListenForCommands()
        {
            await _clusterDashboardNotifications.EnsureWatcher(); // in current impl cluster dashboard senders talk to a single watcher

            using (_readContext.GetMemoryBuffer(out JsonOperationContext.MemoryBuffer segment1))
            using (_readContext.GetMemoryBuffer(out JsonOperationContext.MemoryBuffer segment2))
            {
                try
                {
                    var segments = new[] { segment1, segment2 };
                    int index = 0;
                    var receiveAsync = _webSocket.ReceiveAsync(segments[index].Memory.Memory, _resourceShutdown);
                    var jsonParserState = new JsonParserState();
                    using (var parser = new UnmanagedJsonParser(_readContext, jsonParserState, "cluster-dashboard"))
                    {
                        var result = await receiveAsync;
                        _resourceShutdown.ThrowIfCancellationRequested();
                        
                        parser.SetBuffer(segments[index], 0, result.Count);
                        index++;
                        receiveAsync = _webSocket.ReceiveAsync(segments[index].Memory.Memory, _resourceShutdown);

                        while (true)
                        {
                            using (var builder = new BlittableJsonDocumentBuilder(_readContext, BlittableJsonDocumentBuilder.UsageMode.None, 
                                "cluster-dashboard", parser, jsonParserState))
                            {
                                parser.NewDocument();
                                builder.ReadObjectDocument();

                                while (builder.Read() == false)
                                {
                                    result = await receiveAsync;
                                    _resourceShutdown.ThrowIfCancellationRequested();
                                    
                                    parser.SetBuffer(segments[index], 0, result.Count);
                                    if (++index >= segments.Length)
                                        index = 0;

                                    receiveAsync = _webSocket.ReceiveAsync(segments[index].Memory.Memory, _resourceShutdown);
                                }

                                builder.FinalizeDocument();

                                using (var reader = builder.CreateReader())
                                {
                                    await HandleCommand(reader);
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
        }

        private async Task HandleCommand(BlittableJsonReaderObject reader)
        {
            if (reader.TryGet(nameof(WidgetRequest.Command), out string command) == false)
                throw new ArgumentNullException(nameof(command), "Command argument is mandatory");
            if (reader.TryGet(nameof(WidgetRequest.Id), out int id) == false)
                throw new ArgumentNullException(nameof(command), "Id argument is mandatory");

            switch (command.ToLower())
            {
                case "watch":
                    if (reader.TryGet(nameof(WidgetRequest.Type), out ClusterDashboardNotificationType type) == false)
                        throw new ArgumentNullException(nameof(command), "Type argument is mandatory");
                    reader.TryGet(nameof(WidgetRequest.Config), out BlittableJsonReaderObject configuration);
                    await WatchCommand(id, type, configuration);
                    break;
                case "unwatch":
                    UnwatchCommand(id);
                    break;
                default:
                    throw new NotSupportedException($"Unhandled command: {command}");
            }
        }

        private async Task WatchCommand(int widgetId, ClusterDashboardNotificationType type, BlittableJsonReaderObject configuration)
        {
            var notificationSender = await _clusterDashboardNotifications.CreateNotificationSender(widgetId, type);

            notificationSender.Start();

            if (_activeNotificationSenders.TryAdd(widgetId, notificationSender) == false)
            {
                throw new ArgumentException($"Widget with id = {widgetId} already exists.");
            }
        }

        private void UnwatchCommand(int widgetId)
        {
            if (_activeNotificationSenders.TryRemove(widgetId, out var widget))
            {
                widget.Dispose();
            }
        }

        public override void Dispose()
        {
            base.Dispose();

            foreach (AbstractClusterDashboardNotificationSender sender in _activeNotificationSenders.Values)
            {
                sender.Dispose();
            }

            _activeNotificationSenders.Clear();

            _returnReadContext.Dispose();
        }
    }
}
