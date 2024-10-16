using System;
using System.IO;
using System.Net.WebSockets;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.Documents.Replication.Stats;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.Replication
{
    internal sealed class ReplicationHandlerProcessorForGetPulsesLive : AbstractReplicationHandlerProcessorForGetPulsesLive<DatabaseRequestHandler, DocumentsOperationContext>
    {
        public ReplicationHandlerProcessorForGetPulsesLive([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected override bool SupportsCurrentNode => true;

        protected override async ValueTask HandleCurrentNodeAsync(WebSocket webSocket, OperationCancelToken token)
        {
            var receiveBuffer = new ArraySegment<byte>(new byte[1024]);
            var receive = webSocket.ReceiveAsync(receiveBuffer, RequestHandler.Database.DatabaseShutdown);

            await using (var ms = RecyclableMemoryStreamFactory.GetRecyclableStream())
            using (var collector = new LiveReplicationPulsesCollector(RequestHandler.Database))
            {
                // 1. Send data to webSocket without making UI wait upon opening webSocket
                await SendPulsesOrHeartbeatToWebSocket(receive, webSocket, collector, ms, 100);

                // 2. Send data to webSocket when available
                while (RequestHandler.Database.DatabaseShutdown.IsCancellationRequested == false)
                {
                    if (await SendPulsesOrHeartbeatToWebSocket(receive, webSocket, collector, ms, 4000) == false)
                    {
                        break;
                    }
                }
            }
        }

        private async Task<bool> SendPulsesOrHeartbeatToWebSocket(Task<WebSocketReceiveResult> receive, WebSocket webSocket,
            LiveReplicationPulsesCollector collector, MemoryStream ms, int timeToWait)
        {
            if (receive.IsCompleted || webSocket.State != WebSocketState.Open)
                return false;

            var tuple = await collector.Pulses.TryDequeueAsync(TimeSpan.FromMilliseconds(timeToWait));
            if (tuple.Item1 == false)
            {
                await webSocket.SendAsync(WebSocketHelper.Heartbeat, WebSocketMessageType.Text, true, RequestHandler.Database.DatabaseShutdown);
                return true;
            }

            ms.SetLength(0);

            using (ContextPool.AllocateOperationContext(out JsonOperationContext context))
            await using (var writer = new AsyncBlittableJsonTextWriter(context, ms))
            {
                var pulse = tuple.Item2;
                context.Write(writer, pulse.ToJson());
            }

            ms.TryGetBuffer(out ArraySegment<byte> bytes);
            await webSocket.SendAsync(bytes, WebSocketMessageType.Text, true, RequestHandler.Database.DatabaseShutdown);

            return true;
        }
    }
}
