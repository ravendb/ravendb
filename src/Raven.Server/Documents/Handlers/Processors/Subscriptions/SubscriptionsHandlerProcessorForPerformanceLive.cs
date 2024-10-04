using System;
using System.IO;
using System.Net.WebSockets;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.Documents.Subscriptions;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow;

namespace Raven.Server.Documents.Handlers.Processors.Subscriptions;

internal sealed class SubscriptionsHandlerProcessorForPerformanceLive : AbstractSubscriptionsHandlerProcessorForPerformanceLive<DatabaseRequestHandler, DocumentsOperationContext>
{
    public SubscriptionsHandlerProcessorForPerformanceLive([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected override bool SupportsCurrentNode => true;

    protected override async ValueTask HandleCurrentNodeAsync(WebSocket webSocket, OperationCancelToken token)
    {
        var receiveBuffer = new ArraySegment<byte>(new byte[1024]);
        var receive = webSocket.ReceiveAsync(receiveBuffer, token.Token);

        using (var ms = RecyclableMemoryStreamFactory.GetRecyclableStream())
        using (var collector = new LiveSubscriptionPerformanceCollector(RequestHandler.Database))
        {
            // 1. Send data to webSocket without making UI wait upon opening webSocket
            await collector.SendStatsOrHeartbeatToWebSocket(receive, webSocket, ContextPool, ms, 100);

            // 2. Send data to webSocket when available
            while (token.Token.IsCancellationRequested == false)
            {
                if (await collector.SendStatsOrHeartbeatToWebSocket(receive, webSocket, ContextPool, ms, 4000) == false)
                {
                    break;
                }
            }
        }
    }
}
