using JetBrains.Annotations;
using Raven.Server.Documents.Queries;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Handlers.Debugging.Processors;

internal sealed class QueriesDebugHandlerProcessorForRunningQueries : AbstractQueriesDebugHandlerProcessorForRunningQueries<DatabaseRequestHandler, DocumentsOperationContext>
{
    public QueriesDebugHandlerProcessorForRunningQueries([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected override AbstractQueryRunner GetQueryRunner() => RequestHandler.Database.QueryRunner;
}
