using System;
using System.IO;
using System.Net.WebSockets;
using System.Threading;
using System.Threading.Tasks;
using Raven.Server.Dashboard;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Server.Collections;

namespace Raven.Server.NotificationCenter
{
    public class NotificationCenterWebSocketWriter : IWebsocketWriter, IDisposable
    {
        private readonly WebSocket _webSocket;
        private readonly NotificationsBase _notificationsBase;
        private readonly JsonOperationContext _context;
        private readonly CancellationToken _resourceShutdown;

        private readonly MemoryStream _ms = new MemoryStream();
        public Action AfterTrackActionsRegistration;
        private readonly IDisposable _returnContext;

        public NotificationCenterWebSocketWriter(WebSocket webSocket, NotificationsBase notificationsBase, IMemoryContextPool contextPool, CancellationToken resourceShutdown)
        {
            _webSocket = webSocket;
            _notificationsBase = notificationsBase;
            _returnContext = contextPool.AllocateOperationContext(out _context);
            _resourceShutdown = resourceShutdown;
        }

        public async Task WriteNotifications(CanAccessDatabase shouldWriteByDb)
        {
            var receiveBuffer = new ArraySegment<byte>(new byte[1024]);
            var receive = _webSocket.ReceiveAsync(receiveBuffer, _resourceShutdown);

            var asyncQueue = new AsyncQueue<DynamicJsonValue>();

            try
            {
                using (_notificationsBase.TrackActions(asyncQueue, this, shouldWriteByDb))
                {
                    AfterTrackActionsRegistration?.Invoke();

                    while (_resourceShutdown.IsCancellationRequested == false)
                    {
                        // we use this to detect client-initialized closure
                        if (receive.IsCompleted)
                        {
                            break;
                        }

                        var tuple = await asyncQueue.TryDequeueAsync(TimeSpan.FromSeconds(5));
                        if (tuple.Item1 == false)
                        {
                            await SendHeartbeat();
                            continue;
                        }

                        await WriteToWebSocket(tuple.Item2);
                    }
                }
            }
            catch (OperationCanceledException)
            {
            }
        }

        public async Task WriteToWebSocket<TNotification>(TNotification notification)
        {
            _context.Reset();
            _context.Renew();

            _ms.SetLength(0);

            await using (var writer = new AsyncBlittableJsonTextWriter(_context, _ms))
            {
                var notificationType = notification.GetType();

                if (notificationType == typeof(DynamicJsonValue))
                    _context.Write(writer, notification as DynamicJsonValue);
                else if (notificationType == typeof(BlittableJsonReaderObject))
                    _context.Write(writer, notification as BlittableJsonReaderObject);
                else
                    ThrowNotSupportedType(notification);
            }

            _ms.TryGetBuffer(out ArraySegment<byte> bytes);

            await _webSocket.SendAsync(bytes, WebSocketMessageType.Text, true, _resourceShutdown);
        }

        private async Task SendHeartbeat()
        {
            await _webSocket.SendAsync(WebSocketHelper.Heartbeat, WebSocketMessageType.Text, true, _resourceShutdown);
        }

        private static void ThrowNotSupportedType<TNotification>(TNotification notification)
        {
            throw new NotSupportedException($"Not supported notification type: {notification.GetType()}");
        }

        public void Dispose()
        {
            using (_returnContext)
            using (_ms)
            {
            }
        }
    }
}
