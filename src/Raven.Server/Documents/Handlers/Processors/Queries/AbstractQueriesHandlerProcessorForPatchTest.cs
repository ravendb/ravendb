using System.Net.Http;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.Documents.Queries;
using Raven.Server.TrafficWatch;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.Queries;

internal abstract class AbstractQueriesHandlerProcessorForPatchTest<TRequestHandler, TOperationContext> : AbstractQueriesHandlerProcessor<TRequestHandler, TOperationContext>
    where TOperationContext : JsonOperationContext
    where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
{
    protected AbstractQueriesHandlerProcessorForPatchTest([NotNull] TRequestHandler requestHandler, QueryMetadataCache queryMetadataCache) : base(requestHandler, queryMetadataCache)
    {
    }

    protected abstract ValueTask HandleDocumentPatchTestAsync(IndexQueryServerSide query, string docId, TOperationContext context);

    public override async ValueTask ExecuteAsync()
    {
        using (ContextPool.AllocateOperationContext(out TOperationContext context))
        {
            var docId = RequestHandler.GetQueryStringValueAndAssertIfSingleAndNotEmpty("id");

            var query = await GetIndexQueryAsync(context, HttpMethod.Patch, null);

            if (TrafficWatchManager.HasRegisteredClients)
                RequestHandler.TrafficWatchQuery(query);

            await HandleDocumentPatchTestAsync(query, docId, context);
        }
    }
}
