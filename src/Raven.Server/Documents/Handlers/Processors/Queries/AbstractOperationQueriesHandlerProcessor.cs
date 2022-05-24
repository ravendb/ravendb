using System;
using System.Net.Http;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Queries;
using Raven.Server.Documents.Queries;
using Raven.Server.Json;
using Raven.Server.NotificationCenter;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.Queries;

internal abstract class AbstractOperationQueriesHandlerProcessor<TRequestHandler, TOperationContext> : AbstractQueriesHandlerProcessor<TRequestHandler, TOperationContext>
    where TOperationContext : JsonOperationContext
    where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
{
    protected AbstractOperationQueriesHandlerProcessor([NotNull] TRequestHandler requestHandler, QueryMetadataCache queryMetadataCache) : base(requestHandler, queryMetadataCache)
    {
    }

    protected abstract HttpMethod OperationMethod { get; }

    protected abstract long GetNextOperationId();

    protected abstract IDisposable AllocateContextForAsyncOperation(out TOperationContext asyncOperationContext);

    protected abstract ValueTask ExecuteOperationAsync(TOperationContext asyncOperationContext, IndexQueryServerSide query, long operationId, QueryOperationOptions options);

    public override async ValueTask ExecuteAsync()
    {
        using (ContextPool.AllocateOperationContext(out JsonOperationContext context))
        using (var tracker = new RequestTimeTracker(HttpContext, Logger, null, "Query"))
        {
            var operationId = RequestHandler.GetLongQueryString("operationId", required: false) ?? GetNextOperationId();
            var options = GetQueryOperationOptions();

            var returnContext = AllocateContextForAsyncOperation(out var asyncOperationContext);

            try
            {
                var query = await GetIndexQueryAsync(asyncOperationContext, OperationMethod, tracker, addSpatialProperties: false);

                await ExecuteOperationAsync(asyncOperationContext, query, operationId, options);

                await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
                {
                    writer.WriteOperationIdAndNodeTag(context, operationId, ServerStore.NodeTag);
                }
            }
            catch
            {
                returnContext.Dispose();
                throw;
            }
        }
    }

    protected QueryOperationOptions GetQueryOperationOptions()
    {
        return new QueryOperationOptions
        {
            AllowStale = RequestHandler.GetBoolValueQueryString("allowStale", required: false) ?? false,
            MaxOpsPerSecond = RequestHandler.GetIntValueQueryString("maxOpsPerSec", required: false),
            StaleTimeout = RequestHandler.GetTimeSpanQueryString("staleTimeout", required: false),
            RetrieveDetails = RequestHandler.GetBoolValueQueryString("details", required: false) ?? false
        };
    }
}
