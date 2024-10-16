using System;
using System.IO;
using System.Linq;
using System.Net.WebSockets;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils.IoMetrics;
using Sparrow;

namespace Raven.Server.Documents.Handlers.Processors.IoMetrics;

internal sealed class IoMetricsHandlerProcessorForLive : AbstractIoMetricsHandlerProcessorForLive<DatabaseRequestHandler, DocumentsOperationContext>
{
    public IoMetricsHandlerProcessorForLive([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected override bool SupportsCurrentNode => true;

    protected override async ValueTask HandleCurrentNodeAsync(WebSocket webSocket, OperationCancelToken token)
    {
        var receiveBuffer = new ArraySegment<byte>(new byte[1024]);
        var receive = webSocket.ReceiveAsync(receiveBuffer, token.Token);

        await using (var ms = RecyclableMemoryStreamFactory.GetRecyclableStream())
        using (var collector = new DatabaseLiveIoStatsCollector(RequestHandler.Database))
        {
            // 1. Send data to webSocket without making UI wait upon opening webSocket
            await collector.SendDataOrHeartbeatToWebSocket(receive, webSocket, ms, 100);

            // 2. Send data to webSocket when available
            while (token.Token.IsCancellationRequested == false)
            {
                if (await collector.SendDataOrHeartbeatToWebSocket(receive, webSocket, ms, 4000) == false)
                {
                    break;
                }
            }
        }
    }

    private sealed class DatabaseLiveIoStatsCollector : LiveIoStatsCollector<DocumentsOperationContext>
    {
        public DatabaseLiveIoStatsCollector(DocumentDatabase database) : base(database.IoChanges, database.GetAllStoragesEnvironment().ToList(), database.GetAllPerformanceMetrics(), database.DocumentsStorage.ContextPool, database.DatabaseShutdown)
        {
        }
    }
}
