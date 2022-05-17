using System.Net.Http;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations;
using Raven.Server.Documents.Handlers.Processors;
using Raven.Server.Documents.Operations;
using Raven.Server.Documents.Queries;
using Raven.Server.Json;
using Raven.Server.NotificationCenter;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.Queries;

internal class ShardedQueriesHandlerProcessorForPatch : AbstractDatabaseHandlerProcessor<ShardedDatabaseRequestHandler, TransactionOperationContext>
{
    public ShardedQueriesHandlerProcessorForPatch([NotNull] ShardedDatabaseRequestHandler requestHandler) : base(requestHandler)
    {
    }

    public override async ValueTask ExecuteAsync()
    {
        using (ContextPool.AllocateOperationContext(out JsonOperationContext context))
        using (var tracker = new RequestTimeTracker(HttpContext, Logger, null, "Query"))
        {
            var indexQueryReader = new IndexQueryReader(
                RequestHandler.GetStart(),
                RequestHandler.GetPageSize(),
                HttpContext,
                RequestHandler.RequestBodyStream(),
                RequestHandler.DatabaseContext.QueryMetadataCache,
                database: null,
                addSpatialProperties: false);

            var updateToQuery = await indexQueryReader.GetIndexQueryAsync(context, HttpMethod.Patch, tracker);

            var operationId = RequestHandler.GetLongQueryString("operationId", required: false) ?? RequestHandler.DatabaseContext.Operations.GetNextOperationId();

            _ = RequestHandler.DatabaseContext.Operations
                .AddRemoteOperation(
                    operationId,
                    OperationType.UpdateByQuery,
                    "Test description",
                    detailedDescription: null,
                    c => new PatchByQueryOperation.PatchByQueryCommand<BlittableJsonReaderObject>(DocumentConventions.DefaultForServer, c, updateToQuery), RequestHandler.CreateOperationToken());

            await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
            {
                writer.WriteOperationIdAndNodeTag(context, operationId, ServerStore.NodeTag);
            }
        }
    }
}
