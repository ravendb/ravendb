using System;
using System.Net.Http;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Queries;
using Raven.Server.Documents.Handlers.Processors.Queries;
using Raven.Server.Documents.Operations;
using Raven.Server.Documents.Queries;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.Queries;

internal class ShardedQueriesHandlerProcessorForPatch : AbstractOperationQueriesHandlerProcessor<ShardedDatabaseRequestHandler, TransactionOperationContext>
{
    public ShardedQueriesHandlerProcessorForPatch([NotNull] ShardedDatabaseRequestHandler requestHandler) : base(requestHandler, requestHandler.DatabaseContext.QueryMetadataCache)
    {
    }
    protected override HttpMethod OperationMethod => HttpMethod.Patch;

    protected override long GetNextOperationId()
    {
        return RequestHandler.DatabaseContext.Operations.GetNextOperationId();
    }

    protected override IDisposable AllocateContextForAsyncOperation(out TransactionOperationContext asyncOperationContext)
    {
        return ContextPool.AllocateOperationContext(out asyncOperationContext);
    }

    protected override async ValueTask ExecuteOperationAsync(TransactionOperationContext asyncOperationContext, IndexQueryServerSide query, long operationId, QueryOperationOptions options)
    {
        using (asyncOperationContext)
        {
            await RequestHandler.DatabaseContext.Operations
                .AddRemoteOperation(
                    operationId,
                    OperationType.UpdateByQuery,
                    "Test description",
                    detailedDescription: null,
                    c => new PatchByQueryOperation.PatchByQueryCommand<BlittableJsonReaderObject>(DocumentConventions.DefaultForServer, c, query, options, operationId),
                    RequestHandler.CreateOperationToken());
        }
    }

}
