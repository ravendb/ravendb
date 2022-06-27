using System.Net.Http;
using JetBrains.Annotations;
using Raven.Server.Documents.Operations;
using Raven.Server.Documents.Patch;
using Raven.Server.Documents.Queries;
using PatchRequest = Raven.Server.Documents.Patch.PatchRequest;

namespace Raven.Server.Documents.Handlers.Processors.Queries;

internal class DatabaseQueriesHandlerProcessorForPatch : AbstractDatabaseOperationQueriesHandlerProcessor
{

    public DatabaseQueriesHandlerProcessorForPatch([NotNull] QueriesHandler requestHandler) : base(requestHandler)
    {
    }

    protected override HttpMethod QueryMethod => HttpMethod.Patch;

    protected override (QueryOperationFunction Action, OperationType Type) GetOperation(IndexQueryServerSide query)
    {
        var patch = new PatchRequest(query.Metadata.GetUpdateBody(query.QueryParameters), PatchRequestType.Patch, query.Metadata.DeclaredFunctions);

        return ((runner, o, onProgress, token) => runner.ExecutePatchQuery(
            query, o, patch, query.QueryParameters, QueryOperationContext, onProgress, token), OperationType.UpdateByQuery);
    }
}
