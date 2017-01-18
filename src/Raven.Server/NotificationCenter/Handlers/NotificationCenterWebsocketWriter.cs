using System;
using System.IO;
using System.Net.WebSockets;
using System.Threading;
using System.Threading.Tasks;
using Raven.Server.ServerWide.Context;
using Sparrow.Collections;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.NotificationCenter.Handlers
{
    public class NotificationCenterWebsocketWriter<T> : IDisposable where T : Actions.Action
    {
        private static readonly ArraySegment<byte> Heartbeat = new ArraySegment<byte>(new[] { (byte)'\r', (byte)'\n' });

        private readonly CancellationToken _resourceShutdown;
        private readonly NotificationCenter<T> _notificationCenter;
        private readonly IMemoryContextPool _contextPool;
        private readonly WebSocket _webSocket;
        private readonly MemoryStream _ms = new MemoryStream();

        public NotificationCenterWebsocketWriter(WebSocket webSocket, NotificationCenter<T> notificationCenter, IMemoryContextPool contextPool, CancellationToken resourceShutdown)
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
            var asyncQueue = new AsyncQueue<T>();

            using (await _notificationCenter.TrackActions(asyncQueue, existing => WriteToWebSocket(_ms, existing, _webSocket)))
            {
                while (_resourceShutdown.IsCancellationRequested == false)
                {
                    var tuple = await asyncQueue.TryDequeueAsync(TimeSpan.FromSeconds(5));
                    if (tuple.Item1 == false)
                    {
                        await _webSocket.SendAsync(Heartbeat, WebSocketMessageType.Text, true, _resourceShutdown);
                        continue;
                    }

                    await WriteToWebSocket(_ms, tuple.Item2.ToJson(), _webSocket);
                }
            }
        }

        private Task WriteToWebSocket(MemoryStream ms, object notification, WebSocket webSocket)
        {
            ms.SetLength(0);

            JsonOperationContext context;
            using (_contextPool.AllocateOperationContext(out context))
            using (var writer = new BlittableJsonTextWriter(context, ms))
            {
                if (notification is DynamicJsonValue)
                    context.Write(writer, (DynamicJsonValue)notification);
                else if (notification is BlittableJsonReaderObject)
                    context.Write(writer, (BlittableJsonReaderObject)notification);
                else
                    throw new InvalidOperationException($"Not supported notification type: {notification.GetType()}");
            }

            ArraySegment<byte> bytes;
            ms.TryGetBuffer(out bytes);

            return webSocket.SendAsync(bytes, WebSocketMessageType.Text, true, _resourceShutdown);
        }
    }
}