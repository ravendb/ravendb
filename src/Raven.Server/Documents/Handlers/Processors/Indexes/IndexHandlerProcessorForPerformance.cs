using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Indexes;
using Raven.Server.Json;
using Raven.Server.ServerWide.Context;
using Raven.Server.Web.Http;
using Sparrow.Json;
using Index = Raven.Server.Documents.Indexes.Index;

namespace Raven.Server.Documents.Handlers.Processors.Indexes;

internal class IndexHandlerProcessorForPerformance : AbstractIndexHandlerProcessorForPerformance<DatabaseRequestHandler, DocumentsOperationContext>
{
    public IndexHandlerProcessorForPerformance([NotNull] DatabaseRequestHandler requestHandler)
        : base(requestHandler, requestHandler.ContextPool)
    {
    }

    protected override bool SupportsCurrentNode => true;

    protected override ValueTask HandleCurrentNodeAsync()
    {
        var stats = GetIndexesToReportOn()
            .Select(x => new IndexPerformanceStats
            {
                Name = x.Name,
                Performance = x.GetIndexingPerformance()
            })
            .ToArray();

        return WriteResultAsync(stats);
    }

    protected override Task HandleRemoteNodeAsync(ProxyCommand<IndexPerformanceStats[]> command) => RequestHandler.ExecuteRemoteAsync(command);

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
    private async ValueTask WriteResultAsync(IndexPerformanceStats[] result)
    {
        using (ContextPool.AllocateOperationContext(out JsonOperationContext context))
        await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
        {
            writer.WritePerformanceStats(context, result);
        }
    }
}
