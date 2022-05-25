using System;
using JetBrains.Annotations;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Queries;
using Raven.Client.Http;
using Raven.Server.Documents.Handlers.Processors.Queries;
using Raven.Server.Documents.Operations;
using Raven.Server.Documents.Queries;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.Queries;

internal abstract class AbstractShardedOperationQueriesHandlerProcessor : AbstractOperationQueriesHandlerProcessor<ShardedDatabaseRequestHandler, TransactionOperationContext>
{
    public AbstractShardedOperationQueriesHandlerProcessor([NotNull] ShardedDatabaseRequestHandler requestHandler, QueryMetadataCache queryMetadataCache) : base(requestHandler, queryMetadataCache)
    {
    }

    protected override long GetNextOperationId()
    {
        return RequestHandler.DatabaseContext.Operations.GetNextOperationId();
    }

    protected override IDisposable AllocateContextForAsyncOperation(out TransactionOperationContext asyncOperationContext)
    {
        return ContextPool.AllocateOperationContext(out asyncOperationContext);
    }

    protected abstract (Func<JsonOperationContext, RavenCommand<OperationIdResult>> CommandFactory, OperationType Type) GetOperation(IndexQueryServerSide query, long operationId, QueryOperationOptions options);

    protected override void ScheduleOperation(TransactionOperationContext asyncOperationContext, IDisposable returnAsyncOperationContext, IndexQueryServerSide query, long operationId, QueryOperationOptions options)
    {
        var token = RequestHandler.CreateOperationToken();

        var op = GetOperation(query, operationId, options);

        var task = RequestHandler.DatabaseContext.Operations
            .AddRemoteOperation(
                operationId,
                op.Type,
                GetOperationDescription(query),
                detailedDescription: GetDetailedDescription(query),
                op.CommandFactory,
                token);

        _ = task.ContinueWith(_ =>
        {
            using (returnAsyncOperationContext)
                token.Dispose();
        });
    }
}
