using JetBrains.Annotations;
using Raven.Server.Documents.Queries;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Handlers.Debugging.Processors;

internal sealed class QueriesDebugHandlerProcessorForQueriesCacheList : AbstractQueriesDebugHandlerProcessorForQueriesCacheList<DatabaseRequestHandler, DocumentsOperationContext>
{
    public QueriesDebugHandlerProcessorForQueriesCacheList([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected override QueryMetadataCache GetQueryMetadataCache() => RequestHandler.Database.QueryMetadataCache;
}
