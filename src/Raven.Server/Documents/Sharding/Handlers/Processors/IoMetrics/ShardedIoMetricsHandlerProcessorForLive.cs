using System;
using System.Net.WebSockets;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.Documents.Handlers.Processors.IoMetrics;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.IoMetrics;

internal class ShardedIoMetricsHandlerProcessorForLive : AbstractIoMetricsHandlerProcessorForLive<ShardedDatabaseRequestHandler, TransactionOperationContext>
{
    public ShardedIoMetricsHandlerProcessorForLive([NotNull] ShardedDatabaseRequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected override string GetDatabaseName()
    {
        var shardNumber = GetShardNumber();

        return ShardHelper.ToShardName(RequestHandler.DatabaseName, shardNumber);
    }

    protected override bool SupportsCurrentNode => false;

    protected override ValueTask HandleCurrentNodeAsync(WebSocket webSocket, OperationCancelToken token) => throw new NotSupportedException();
}
