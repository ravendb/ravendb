using System.Collections.Concurrent;
using JetBrains.Annotations;
using Raven.Server.Documents.Handlers.Processors.Changes;
using Raven.Server.Documents.Sharding.Changes;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.Changes;

internal class ShardedChangesHandlerProcessorForGetConnectionsDebugInfo : AbstractChangesHandlerProcessorForGetConnectionsDebugInfo<ShardedDatabaseRequestHandler, TransactionOperationContext, ShardedChangesClientConnection>
{
    public ShardedChangesHandlerProcessorForGetConnectionsDebugInfo([NotNull] ShardedDatabaseRequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected override ConcurrentDictionary<long, ShardedChangesClientConnection> GetConnections() => RequestHandler.DatabaseContext.Changes.Connections;
}
