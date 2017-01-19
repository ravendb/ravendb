using System;
using System.IO;
using System.Net.WebSockets;
using System.Threading;
using System.Threading.Tasks;
using Raven.Server.ServerWide.Context;
using Sparrow.Collections;
using Sparrow.Json;
using Sparrow.Json.Parsing;

using Action = Raven.Server.NotificationCenter.Actions.Action;

namespace Raven.Server.NotificationCenter.Handlers
{
    public class NotificationCenterWebsocketWriter : IDisposable
    {
        private static readonly ArraySegment<byte> Heartbeat = new ArraySegment<byte>(new[] { (byte)'\r', (byte)'\n' });

        private readonly CancellationToken _resourceShutdown;
        private readonly NotificationCenter _notificationCenter;
        private readonly IMemoryContextPool _contextPool;
        private readonly WebSocket _webSocket;
        private readonly MemoryStream _ms = new MemoryStream();

        public NotificationCenterWebsocketWriter(WebSocket webSocket, NotificationCenter notificationCenter, IMemoryContextPool contextPool, CancellationToken resourceShutdown)
        {
            _notificationCenter = notificationCenter;
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
            var asyncQueue = new AsyncQueue<Action>();
            
            using (_notificationCenter.TrackActions(asyncQueue))
            {
                while (_resourceShutdown.IsCancellationRequested == false)
                {
                    var tuple = await asyncQueue.TryDequeueAsync(TimeSpan.FromSeconds(5));
                    if (tuple.Item1 == false)
                    {
                        await _webSocket.SendAsync(Heartbeat, WebSocketMessageType.Text, true, _resourceShutdown);
                        continue;
                    }

                    await WriteToWebSocket(tuple.Item2.ToJson());
                }
            }
        }

        public Task WriteToWebSocket<TNotification>(TNotification notification)
        {
            _ms.SetLength(0);

            JsonOperationContext context;
            using (_contextPool.AllocateOperationContext(out context))
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

            ArraySegment<byte> bytes;
            _ms.TryGetBuffer(out bytes);

            return _webSocket.SendAsync(bytes, WebSocketMessageType.Text, true, _resourceShutdown);
        }

        private static void ThrowNotSupportedType<TNotification>(TNotification notification)
        {
            throw new NotSupportedException($"Not supported notification type: {notification.GetType()}");
        }
    }
}