using System;
using System.IO;
using System.Net.WebSockets;
using System.Threading;
using System.Threading.Tasks;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Collections;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.NotificationCenter
{
    public class NotificationCenterWebSocketWriter : IWebsocketWriter, IDisposable
    {
        private readonly CancellationToken _resourceShutdown;
        private readonly NotificationsBase _notificationsBase;
        private readonly IMemoryContextPool _contextPool;
        private readonly WebSocket _webSocket;
        private readonly MemoryStream _ms = new MemoryStream();

        public NotificationCenterWebSocketWriter(WebSocket webSocket, NotificationsBase notificationsBase, IMemoryContextPool contextPool, CancellationToken resourceShutdown)
        {
            _notificationsBase = notificationsBase;
            _contextPool = contextPool;
            _resourceShutdown = resourceShutdown;
            _webSocket = webSocket;
        }

        public void Dispose()
        {
            _ms.Dispose();
        }

        public async Task WriteNotifications()
        {
            var receiveBuffer = new ArraySegment<byte>(new byte[1024]);
            var receive = _webSocket.ReceiveAsync(receiveBuffer, _resourceShutdown);

            var asyncQueue = new AsyncQueue<DynamicJsonValue>();

            try
            {
                using (_notificationsBase.TrackActions(asyncQueue, this))
                {
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
                            await _webSocket.SendAsync(WebSocketHelper.Heartbeat, WebSocketMessageType.Text, true, _resourceShutdown);
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

        public Task WriteToWebSocket<TNotification>(TNotification notification)
        {
            _ms.SetLength(0);

            using (_contextPool.AllocateOperationContext(out JsonOperationContext context))
            using (var writer = new BlittableJsonTextWriter(context, _ms))
            {
                var notificationType = notification.GetType();

                if (notificationType == typeof(DynamicJsonValue))
                    context.Write(writer, notification as DynamicJsonValue);
                else if (notificationType == typeof(BlittableJsonReaderObject))
                    context.Write(writer, notification as BlittableJsonReaderObject);
                else
                    ThrowNotSupportedType(notification);
            }

            _ms.TryGetBuffer(out ArraySegment<byte> bytes);

            return _webSocket.SendAsync(bytes, WebSocketMessageType.Text, true, _resourceShutdown);
        }

        private static void ThrowNotSupportedType<TNotification>(TNotification notification)
        {
            throw new NotSupportedException($"Not supported notification type: {notification.GetType()}");
        }
    }
}
