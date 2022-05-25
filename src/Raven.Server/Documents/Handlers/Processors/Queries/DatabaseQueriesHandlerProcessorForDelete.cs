using System.Net.Http;
using JetBrains.Annotations;
using Raven.Server.Documents.Operations;
using Raven.Server.Documents.Queries;

namespace Raven.Server.Documents.Handlers.Processors.Queries;

internal class DatabaseQueriesHandlerProcessorForDelete : AbstractDatabaseOperationQueriesHandlerProcessor
{
    public DatabaseQueriesHandlerProcessorForDelete([NotNull] QueriesHandler requestHandler) : base(requestHandler)
    {
    }

    protected override HttpMethod OperationMethod => HttpMethod.Delete;

    protected override (QueryOperationFunction Action, OperationType Type) GetOperation(IndexQueryServerSide query)
    {
        return ((runner, o, onProgress, token) => runner.ExecuteDeleteQuery(query, o, QueryOperationContext, onProgress, token), OperationType.DeleteByQuery);
    }
}
