using System;
using System.Net.Http;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Queries;
using Raven.Server.Documents.Queries;
using Raven.Server.Json;
using Raven.Server.NotificationCenter;
using Raven.Server.TrafficWatch;
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

    protected abstract void ScheduleOperation(TOperationContext asyncOperationContext, IDisposable returnAsyncOperationContext, IndexQueryServerSide query, long operationId, QueryOperationOptions options);

    public override async ValueTask ExecuteAsync()
    {
        using (ContextPool.AllocateOperationContext(out JsonOperationContext context))
        using (var tracker = new RequestTimeTracker(HttpContext, Logger, null, "Query"))
        {
            var operationId = RequestHandler.GetLongQueryString("operationId", required: false) ?? GetNextOperationId();
            var options = GetQueryOperationOptions();

            var returnContext = AllocateContextForAsyncOperation(out var asyncOperationContext); // we don't dispose this as operation is async

            try
            {
                var query = await GetIndexQueryAsync(asyncOperationContext, OperationMethod, tracker, addSpatialProperties: false);

                query.DisableAutoIndexCreation = RequestHandler.GetBoolValueQueryString("disableAutoIndexCreation", false) ?? false;

                if (TrafficWatchManager.HasRegisteredClients)
                    RequestHandler.TrafficWatchQuery(query);

                ScheduleOperation(asyncOperationContext, returnContext, query, operationId, options);

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

    protected static string GetOperationDescription(IndexQueryServerSide query)
    {
        return query.Metadata.IsDynamic
            ? (query.Metadata.IsCollectionQuery ? "collection/" : "dynamic/") + query.Metadata.CollectionName
            : query.Metadata.IndexName;
    }

    protected static BulkOperationResult.OperationDetails GetDetailedDescription(IndexQueryServerSide query)
    {
        return new BulkOperationResult.OperationDetails
        {
            Query = query.Query
        };
    }
}
