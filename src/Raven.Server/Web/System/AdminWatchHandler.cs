using System;
using System.IO;
using System.Net.WebSockets;
using System.Threading.Tasks;
using Raven.Server.Alerts;
using Raven.Server.Documents.Handlers.Admin;
using Raven.Server.Routing;
using Sparrow.Collections;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Web.System
{
    public class AdminWatchHandler : AdminRequestHandler
    {
        [RavenAction("/admin/watch", "GET", "/admin/watch")]
        public async Task GetChanges()
        {
            var heartbeat = new ArraySegment<byte>(new[] { (byte)'\r', (byte)'\n' });
            var ms = new MemoryStream();
            using (var webSocket = await HttpContext.WebSockets.AcceptWebSocketAsync())
            {
                var asyncQueue = new AsyncQueue<GlobalAlertNotification>();

                using (ServerStore.TrackChanges(asyncQueue))
                {
                    while (ServerStore.ServerShutdown.IsCancellationRequested == false)
                    {
                        var tuple = await asyncQueue.TryDequeueAsync(TimeSpan.FromSeconds(5));
                        if (tuple.Item1 == false)
                        {
                            await
                                webSocket.SendAsync(heartbeat, WebSocketMessageType.Text, true,
                                    ServerStore.ServerShutdown);
                            continue;
                        }

                        ms.SetLength(0);

                        JsonOperationContext context;
                        using (ServerStore.ContextPool.AllocateOperationContext(out context))
                        using (var writer = new BlittableJsonTextWriter(context, ms))
                        {
                            context.Write(writer, tuple.Item2.ToJson());
                        }

                        ArraySegment<byte> bytes;
                        ms.TryGetBuffer(out bytes);

                        await webSocket.SendAsync(bytes, WebSocketMessageType.Text, true, ServerStore.ServerShutdown);
                    }
                }
            }
        }
    }
}