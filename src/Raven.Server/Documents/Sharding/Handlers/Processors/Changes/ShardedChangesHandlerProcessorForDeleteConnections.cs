using JetBrains.Annotations;
using Raven.Server.Documents.Handlers.Processors.Changes;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.Changes;

internal class ShardedChangesHandlerProcessorForDeleteConnections : AbstractChangesHandlerProcessorForDeleteConnections<ShardedDatabaseRequestHandler, TransactionOperationContext>
{
    public ShardedChangesHandlerProcessorForDeleteConnections([NotNull] ShardedDatabaseRequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected override void Disconnect(long connectionId)
    {
        RequestHandler.DatabaseContext.Changes.Disconnect(connectionId);
    }
}
