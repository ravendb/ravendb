using System;
using System.Diagnostics;
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

internal abstract class AbstractDatabaseOperationQueriesHandlerProcessor : AbstractOperationQueriesHandlerProcessor<QueriesHandler, DocumentsOperationContext>
{
    protected delegate Task<IOperationResult> QueryOperationFunction(QueryRunner runner, QueryOperationOptions options, Action<IOperationProgress> progress, OperationCancelToken token);

    protected QueryOperationContext QueryOperationContext;

    protected AbstractDatabaseOperationQueriesHandlerProcessor([NotNull] QueriesHandler requestHandler) : base(requestHandler, requestHandler.Database.QueryMetadataCache)
    {
    }

    protected override long GetNextOperationId()
    {
        return RequestHandler.Database.Operations.GetNextOperationId();
    }

    protected override IDisposable AllocateContextForAsyncOperation(out DocumentsOperationContext asyncOperationContext)
    {
        QueryOperationContext = QueryOperationContext.Allocate(RequestHandler.Database);

        asyncOperationContext = QueryOperationContext.Documents;

        return QueryOperationContext;
    }

    protected abstract (QueryOperationFunction Action, OperationType Type) GetOperation(IndexQueryServerSide query);

    protected override void ScheduleOperation(DocumentsOperationContext asyncOperationContext, IDisposable returnAsyncOperationContext, IndexQueryServerSide query, long operationId, QueryOperationOptions options)
    {
        Debug.Assert(ReferenceEquals(asyncOperationContext, QueryOperationContext.Documents), "asyncOperationContext == _queryOperationContext.Documents");
        Debug.Assert(ReferenceEquals(returnAsyncOperationContext, QueryOperationContext), "returnAsyncOperationContext == _queryOperationContext");

        var op = GetOperation(query);

        ExecuteQueryOperation(query, operationId, options, op.Action,  QueryOperationContext, op.Type);
    }

    private void ExecuteQueryOperation(IndexQueryServerSide query, long operationId, QueryOperationOptions options,
        QueryOperationFunction operation,
        IDisposable returnContextToPool,
        OperationType operationType)
    {
        var token = RequestHandler.CreateTimeLimitedQueryOperationToken();

        var description = GetOperationDescription(query);

        var details = GetDetailedDescription(query);

        var task = RequestHandler.Database.Operations.AddLocalOperation(operationId,
            operationType,
            description,
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
