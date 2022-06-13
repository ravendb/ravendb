using JetBrains.Annotations;
using Raven.Server.Documents.Handlers.Processors.Tcp;
using Raven.Server.Documents.TcpHandlers;
using Raven.Server.ServerWide.Context;
using Sparrow.Collections;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.Tcp;

internal class ShardedTcpManagementHandlerProcessorForGetAll : AbstractTcpManagementHandlerProcessorForGetAll<ShardedDatabaseRequestHandler, TransactionOperationContext>
{
    public ShardedTcpManagementHandlerProcessorForGetAll([NotNull] ShardedDatabaseRequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected override ConcurrentSet<TcpConnectionOptions> GetRunningTcpConnections() => RequestHandler.DatabaseContext.RunningTcpConnections;
}
