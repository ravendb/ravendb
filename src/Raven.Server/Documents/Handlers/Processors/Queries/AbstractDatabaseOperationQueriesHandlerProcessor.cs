using System;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Queries;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.Operations;
using Raven.Server.Documents.Queries;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Handlers.Processors.Queries;

internal abstract class AbstractDatabaseOperationQueriesHandlerProcessor<TRequestHandler> : AbstractOperationQueriesHandlerProcessor<TRequestHandler, DocumentsOperationContext>
    where TRequestHandler : DatabaseRequestHandler
{
    protected AbstractDatabaseOperationQueriesHandlerProcessor([NotNull] TRequestHandler requestHandler) : base(requestHandler, requestHandler.Database.QueryMetadataCache)
    {
    }

    protected override long GetNextOperationId()
    {
        return RequestHandler.Database.Operations.GetNextOperationId();
    }

    protected void ExecuteQueryOperation(IndexQueryServerSide query, long operationId, QueryOperationOptions options,
        Func<QueryRunner,
            QueryOperationOptions,
            Action<IOperationProgress>, OperationCancelToken,
            Task<IOperationResult>> operation,
        IDisposable returnContextToPool,
        OperationType operationType)
    {
        var token = RequestHandler.CreateTimeLimitedQueryOperationToken();

        var indexName = query.Metadata.IsDynamic
            ? (query.Metadata.IsCollectionQuery ? "collection/" : "dynamic/") + query.Metadata.CollectionName
            : query.Metadata.IndexName;

        var details = new BulkOperationResult.OperationDetails
        {
            Query = query.Query
        };

        var task = RequestHandler.Database.Operations.AddLocalOperation(operationId,
            operationType,
            indexName,
            details,
            onProgress => operation(RequestHandler.Database.QueryRunner, options, onProgress, token),
            token);

        _ = task.ContinueWith(_ =>
        {
            using (returnContextToPool)
                token.Dispose();
        });
    }
}
