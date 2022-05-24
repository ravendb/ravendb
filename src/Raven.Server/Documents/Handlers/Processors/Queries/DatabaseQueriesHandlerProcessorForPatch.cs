using System;
using System.Net.Http;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Queries;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.Operations;
using Raven.Server.Documents.Patch;
using Raven.Server.Documents.Queries;
using Raven.Server.ServerWide.Context;
using Raven.Server.TrafficWatch;

namespace Raven.Server.Documents.Handlers.Processors.Queries;

internal class DatabaseQueriesHandlerProcessorForPatch: AbstractDatabaseOperationQueriesHandlerProcessor<QueriesHandler>
{
    public DatabaseQueriesHandlerProcessorForPatch([NotNull] QueriesHandler requestHandler) : base(requestHandler)
    {
    }

    protected override HttpMethod OperationMethod => HttpMethod.Patch;

    protected override IDisposable AllocateContextForAsyncOperation(out DocumentsOperationContext asyncOperationContext)
    {
        return ContextPool.AllocateOperationContext(out asyncOperationContext);
    }

    protected override ValueTask ExecuteOperationAsync(DocumentsOperationContext asyncOperationContext, IndexQueryServerSide query, long operationId, QueryOperationOptions options)
    {
        var queryContext = QueryOperationContext.Allocate(RequestHandler.Database, asyncOperationContext, releaseDocumentsContext: true); // we don't dispose this as operation is async

        try
        {
            query.DisableAutoIndexCreation = RequestHandler.GetBoolValueQueryString("disableAutoIndexCreation", false) ?? false;

            if (TrafficWatchManager.HasRegisteredClients)
                RequestHandler.TrafficWatchQuery(query);

            var patch = new PatchRequest(query.Metadata.GetUpdateBody(query.QueryParameters), PatchRequestType.Patch, query.Metadata.DeclaredFunctions);

            ExecuteQueryOperation(query, operationId, options,
                (runner, options, onProgress, token) => runner.ExecutePatchQuery(
                    query, options, patch, query.QueryParameters, queryContext, onProgress, token),
                queryContext, OperationType.UpdateByQuery);

            return ValueTask.CompletedTask;
        }
        catch
        {
            queryContext.Dispose();
            throw;
        }
    }
}
