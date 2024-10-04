using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.WebSockets;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client;
using Raven.Server.Documents.Indexes;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow;
using Index = Raven.Server.Documents.Indexes.Index;

namespace Raven.Server.Documents.Handlers.Processors.Indexes;

internal sealed class IndexHandlerProcessorForPerformanceLive : AbstractIndexHandlerProcessorForPerformanceLive<DatabaseRequestHandler, DocumentsOperationContext>
{
    public IndexHandlerProcessorForPerformanceLive([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected override bool SupportsCurrentNode => true;

    protected override async ValueTask HandleCurrentNodeAsync(WebSocket webSocket, OperationCancelToken token)
    {
        var indexNames = GetIndexesToReportOn().Select(x => x.Name).ToList();
        if (IncludeSideBySide)
        {
            // user requested to track side by side indexes as well
            // add extra names to indexNames list
            var complementaryIndexes = new HashSet<string>();
            foreach (var indexName in indexNames)
            {
                if (indexName.StartsWith(Constants.Documents.Indexing.SideBySideIndexNamePrefix, StringComparison.OrdinalIgnoreCase))
                    complementaryIndexes.Add(indexName.Substring(Constants.Documents.Indexing.SideBySideIndexNamePrefix.Length));
                else
                    complementaryIndexes.Add(Constants.Documents.Indexing.SideBySideIndexNamePrefix + indexName);
            }

            indexNames.AddRange(complementaryIndexes);
        }

        var receiveBuffer = new ArraySegment<byte>(new byte[1024]);
        var receive = webSocket.ReceiveAsync(receiveBuffer, token.Token);

        await using (var ms = RecyclableMemoryStreamFactory.GetRecyclableStream())
        using (var collector = new LiveIndexingPerformanceCollector(RequestHandler.Database, indexNames))
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

    private IEnumerable<Index> GetIndexesToReportOn()
    {
        var names = GetNames();

        var indexes = names.Count == 0
            ? RequestHandler.Database.IndexStore
                .GetIndexes()
            : RequestHandler.Database.IndexStore
                .GetIndexes()
                .Where(x => names.Contains(x.Name, StringComparer.OrdinalIgnoreCase));

        return indexes;
    }
}
