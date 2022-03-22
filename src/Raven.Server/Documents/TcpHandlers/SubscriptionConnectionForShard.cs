using System;
using Raven.Server.Documents.Subscriptions;
using Raven.Server.ServerWide;
using Sparrow.Json;

namespace Raven.Server.Documents.TcpHandlers;

public class SubscriptionConnectionForShard : SubscriptionConnection
{
    public readonly string ShardName;

    public SubscriptionConnectionForShard(ServerStore serverStore, TcpConnectionOptions tcpConnection, IDisposable tcpConnectionDisposable, JsonOperationContext.MemoryBuffer bufferToCopy, string database) : 
        base(serverStore, tcpConnection, tcpConnectionDisposable, bufferToCopy, database)
    {
        ShardName = tcpConnection.DocumentDatabase.Name;
    }

    protected override StatusMessageDetails GetDefault()
    {
        return new StatusMessageDetails
        {
            DatabaseName = $"for shard '{ShardName}'",
            ClientType = "'sharded worker'",
            SubscriptionType = "sharded subscription"
        };
    }

    protected override SubscriptionConnectionsState GetSubscriptionConnectionState()
    {
        var subscriptions = TcpConnection.DocumentDatabase.SubscriptionStorage.Subscriptions;
        return subscriptions.GetOrAdd(SubscriptionId, subId => new SubscriptionConnectionsStateForShard(subId, TcpConnection.DocumentDatabase.SubscriptionStorage));
    }
}
