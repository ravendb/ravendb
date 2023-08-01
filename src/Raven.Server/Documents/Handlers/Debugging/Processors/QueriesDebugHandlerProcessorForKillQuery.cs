using JetBrains.Annotations;
using Raven.Server.Documents.Queries;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Debugging.Processors;

internal sealed class QueriesDebugHandlerProcessorForKillQuery : AbstractQueriesDebugHandlerProcessorForKillQuery<DatabaseRequestHandler, DocumentsOperationContext>
{
    public QueriesDebugHandlerProcessorForKillQuery([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected override AbstractQueryRunner GetQueryRunner() => RequestHandler.Database.QueryRunner;
}
