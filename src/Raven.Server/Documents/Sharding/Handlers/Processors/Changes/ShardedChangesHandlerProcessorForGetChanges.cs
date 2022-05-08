using System.Net.WebSockets;
using JetBrains.Annotations;
using Raven.Server.Documents.Handlers.Processors.Changes;
using Raven.Server.Documents.Sharding.Changes;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.Changes;

internal class ShardedChangesHandlerProcessorForGetChanges : AbstractChangesHandlerProcessorForGetChanges<ShardedDatabaseRequestHandler, TransactionOperationContext, ShardedChangesClientConnection>
{
    public ShardedChangesHandlerProcessorForGetChanges([NotNull] ShardedDatabaseRequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected override ShardedChangesClientConnection CreateChangesClientConnection(WebSocket webSocket, bool throttleConnection, bool fromStudio) => new(webSocket, RequestHandler.ServerStore, RequestHandler.DatabaseContext, throttleConnection, fromStudio);

    protected override void Connect(ShardedChangesClientConnection connection)
    {
        RequestHandler.DatabaseContext.Changes.Connect(connection);
    }

    protected override void Disconnect(ShardedChangesClientConnection connection)
    {
        RequestHandler.DatabaseContext.Changes.Disconnect(connection.Id);
    }
}
