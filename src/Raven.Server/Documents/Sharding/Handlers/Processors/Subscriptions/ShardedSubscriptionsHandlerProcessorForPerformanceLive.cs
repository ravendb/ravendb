﻿using System;
using System.Net.WebSockets;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.Documents.Handlers.Processors.Subscriptions;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.Subscriptions;

internal sealed class ShardedSubscriptionsHandlerProcessorForPerformanceLive : AbstractSubscriptionsHandlerProcessorForPerformanceLive<ShardedDatabaseRequestHandler, TransactionOperationContext>
{
    public ShardedSubscriptionsHandlerProcessorForPerformanceLive([NotNull] ShardedDatabaseRequestHandler requestHandler) : base(requestHandler)
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
