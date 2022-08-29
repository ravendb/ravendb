using System;
using System.IO;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.Documents.Replication.Stats;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Web.Http;

namespace Raven.Server.Documents.Handlers.Processors.Replication
{
    internal class ReplicationHandlerProcessorForGetPerformanceLive : AbstractReplicationHandlerProcessorForGetPerformanceLive<DatabaseRequestHandler, DocumentsOperationContext>
    {
        public ReplicationHandlerProcessorForGetPerformanceLive([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected override bool SupportsCurrentNode => true;

        protected override async ValueTask HandleCurrentNodeAsync()
        {
            using (var webSocket = await HttpContext.WebSockets.AcceptWebSocketAsync())
            {
                var receiveBuffer = new ArraySegment<byte>(new byte[1024]);
                var receive = webSocket.ReceiveAsync(receiveBuffer, RequestHandler.Database.DatabaseShutdown);

                await using (var ms = new MemoryStream())
                using (var collector = new LiveReplicationPerformanceCollector(RequestHandler.Database))
                {
                    // 1. Send data to webSocket without making UI wait upon opening webSocket
                    await collector.SendStatsOrHeartbeatToWebSocket(receive, webSocket, ContextPool, ms, 100);

                    // 2. Send data to webSocket when available
                    while (RequestHandler.Database.DatabaseShutdown.IsCancellationRequested == false)
                    {
                        if (await collector.SendStatsOrHeartbeatToWebSocket(receive, webSocket, ContextPool, ms, 4000) == false)
                        {
                            break;
                        }
                    }
                }
            }
        }

        protected override Task HandleRemoteNodeAsync(ProxyCommand<object> command, OperationCancelToken token) => RequestHandler.ExecuteRemoteAsync(command, token.Token);
    }
}
