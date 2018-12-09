using System;
using System.Diagnostics;
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
        private readonly WebSocket _webSocket;
        private readonly NotificationsBase _notificationsBase;
        private readonly JsonOperationContext _context;
        private readonly CancellationToken _resourceShutdown;
        
        private readonly MemoryStream _ms = new MemoryStream();
        
        public NotificationCenterWebSocketWriter(WebSocket webSocket, NotificationsBase notificationsBase, IMemoryContextPool contextPool, CancellationToken resourceShutdown)
        {
            _webSocket = webSocket;
            _notificationsBase = notificationsBase;
            contextPool.AllocateOperationContext(out _context);
            _resourceShutdown = resourceShutdown;
        }

        public async Task WriteNotifications(Func<string, bool> shouldWriteByDb)
        {
            var receiveBuffer = new ArraySegment<byte>(new byte[1024]);
            var receive = _webSocket.ReceiveAsync(receiveBuffer, _resourceShutdown);

            var asyncQueue = new AsyncQueue<DynamicJsonValue>();

            try
            {
                var sp = shouldWriteByDb == null ? null : Stopwatch.StartNew();
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
                            await SendHeartbeat();
                            continue;
                        }

                        if (shouldWriteByDb != null &&
                            shouldWriteByDb((string)tuple.Item2["Database"]) == false)
                        {
                            if (sp.ElapsedMilliseconds > 5000)
                            {
                                sp.Restart();
                                await SendHeartbeat();
                            }
                            
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

            using (var writer = new BlittableJsonTextWriter(_context, _ms))
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

            return _webSocket.SendAsync(bytes, WebSocketMessageType.Text, true, _resourceShutdown);
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
            using (_ms)
            using (_context)
            {
            }
        }
    }
}
