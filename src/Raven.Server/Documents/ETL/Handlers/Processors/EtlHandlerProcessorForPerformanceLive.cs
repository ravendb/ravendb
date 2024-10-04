using System;
using System.IO;
using System.Net.WebSockets;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow;

namespace Raven.Server.Documents.ETL.Handlers.Processors;

internal sealed class EtlHandlerProcessorForPerformanceLive : AbstractEtlHandlerProcessorForPerformanceLive<DatabaseRequestHandler, DocumentsOperationContext>
{
    public EtlHandlerProcessorForPerformanceLive([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected override bool SupportsCurrentNode => true;

    protected override async ValueTask HandleCurrentNodeAsync(WebSocket webSocket, OperationCancelToken token)
    {
        var names = RequestHandler.GetStringValuesQueryString("name", required: false);
        var etls = EtlHandlerProcessorForStats.GetProcessesToReportOn(RequestHandler.Database, names);

        try
        {
            var receiveBuffer = new ArraySegment<byte>(new byte[1024]);
            var receive = webSocket.ReceiveAsync(receiveBuffer, token.Token);

            await using (var ms = RecyclableMemoryStreamFactory.GetRecyclableStream())
            using (var collector = new LiveEtlPerformanceCollector(RequestHandler.Database, etls))
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
        catch (OperationCanceledException)
        {
            // disposing
        }
        catch (ObjectDisposedException)
        {
            // disposing
        }
    }
}
