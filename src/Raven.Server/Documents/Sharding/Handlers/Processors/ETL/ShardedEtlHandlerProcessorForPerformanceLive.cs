using System;
using System.Net.WebSockets;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.Documents.ETL.Handlers.Processors;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.ETL;

internal class ShardedEtlHandlerProcessorForPerformanceLive : AbstractEtlHandlerProcessorForPerformanceLive<ShardedDatabaseRequestHandler, TransactionOperationContext>
{
    public ShardedEtlHandlerProcessorForPerformanceLive([NotNull] ShardedDatabaseRequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected override bool SupportsCurrentNode => false;

    protected override string GetDatabaseName()
    {
        var shardNumber = GetShardNumber();

        return ShardHelper.ToShardName(RequestHandler.DatabaseName, shardNumber);
    }

    protected override ValueTask HandleCurrentNodeAsync(WebSocket webSocket, OperationCancelToken token) => throw new NotSupportedException();
}
