using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Indexes;
using Raven.Server.Documents.Commands.Indexes;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Web.Http;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Index = Raven.Server.Documents.Indexes.Index;

namespace Raven.Server.Documents.Handlers.Processors.Indexes;

internal class IndexHandlerProcessorForTotalTime : AbstractIndexHandlerProcessorForTotalTime<DatabaseRequestHandler, DocumentsOperationContext>
{
    public IndexHandlerProcessorForTotalTime([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected override bool SupportsCurrentNode => true;

    protected override async ValueTask HandleCurrentNodeAsync()
    {
        var indexes = GetIndexesToReportOn();
        using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
        await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
        {
            var dja = new DynamicJsonArray();

            foreach (var index in indexes)
            {
                DateTime baseLine = DateTime.MinValue;
                using (context.OpenReadTransaction())
                {
                    foreach (var collection in index.Collections)
                    {
                        switch (index.SourceType)
                        {
                            case IndexSourceType.Documents:
                                var etag = RequestHandler.Database.DocumentsStorage.GetLastDocumentEtag(context.Transaction.InnerTransaction, collection);
                                var document = RequestHandler.Database.DocumentsStorage.GetDocumentsFrom(context, collection, etag, 0, 1, DocumentFields.Default).FirstOrDefault();
                                if (document != null && document.LastModified > baseLine)
                                    baseLine = document.LastModified;
                                break;

                            case IndexSourceType.Counters:
                            case IndexSourceType.TimeSeries:
                                break;

                            default:
                                throw new NotSupportedException($"Index with source type '{index.SourceType}' is not supported.");
                        }
                    }
                }
                var createdTimestamp = index.GetStats().CreatedTimestamp;
                if (createdTimestamp > baseLine)
                    baseLine = createdTimestamp;

                var lastBatch = index.GetIndexingPerformance()
                    .LastOrDefault(x => x.Completed != null)
                    ?.Completed ?? DateTime.UtcNow;

                dja.Add(new DynamicJsonValue
                {
                    [nameof(GetIndexesTotalTimeCommand.IndexTotalTime.Name)] = index.Name,
                    [nameof(GetIndexesTotalTimeCommand.IndexTotalTime.TotalIndexingTime)] = index.TimeSpentIndexing.Elapsed,
                    [nameof(GetIndexesTotalTimeCommand.IndexTotalTime.LagTime)] = (lastBatch - baseLine)
                });
            }

            context.Write(writer, new DynamicJsonValue
            {
                [nameof(GetIndexesTotalTimeCommand.IndexesTotalTime.Results)] = dja
            });
        }
    }

    protected override Task HandleRemoteNodeAsync(ProxyCommand<GetIndexesTotalTimeCommand.IndexTotalTime[]> command, OperationCancelToken token) => RequestHandler.ExecuteRemoteAsync(command, token.Token);

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
