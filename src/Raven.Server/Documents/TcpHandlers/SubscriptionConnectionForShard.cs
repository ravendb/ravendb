using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.ServerWide;
using Raven.Server.Documents.Sharding;
using Raven.Server.Documents.Subscriptions;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Utils;

namespace Raven.Server.Documents.TcpHandlers;

public class SubscriptionConnectionForShard : SubscriptionConnection
{
    public readonly string ShardName;
    private readonly ShardedDocumentDatabase _shardedDatabase;
    private readonly HashSet<string> _dbIdToRemove;

    public SubscriptionConnectionForShard(ServerStore serverStore, TcpConnectionOptions tcpConnection, IDisposable tcpConnectionDisposable, JsonOperationContext.MemoryBuffer bufferToCopy, string database) : 
        base(serverStore, tcpConnection, tcpConnectionDisposable, bufferToCopy, database)
    {
        _shardedDatabase = tcpConnection.DocumentDatabase as ShardedDocumentDatabase;
        ShardName = tcpConnection.DocumentDatabase.Name;
        _dbIdToRemove = new HashSet<string>() { _shardedDatabase.ShardedDatabaseId };
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
    
    protected override DynamicJsonValue AcceptMessage()
    {
        DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Karmel, DevelopmentHelper.Severity.Normal, "need to ensure the sharded workers has the same sub definition. by sending my raft index?");
        return base.AcceptMessage();
    }

    protected override RawDatabaseRecord GetRecord(TransactionOperationContext context) => _serverStore.Cluster.ReadRawDatabaseRecord(context, ShardName);

    protected override string SetLastChangeVectorInThisBatch(IChangeVectorOperationContext context, string currentLast, Document sentDocument)
    {
        if (sentDocument.Etag == 0) // got this document from resend
            return currentLast;

        var vector = context.GetChangeVector(sentDocument.ChangeVector);

        return ChangeVectorUtils.MergeVectors(
            currentLast,
            ChangeVectorUtils.NewChangeVector(_serverStore.NodeTag, sentDocument.Etag,_shardedDatabase.DbBase64Id),
            vector.Order);
    }

    protected override Task UpdateStateAfterBatchSentAsync(IChangeVectorOperationContext context, string lastChangeVectorSentInThisBatch)
    {
        var vector = context.GetChangeVector(lastChangeVectorSentInThisBatch);
        vector.TryRemoveIds(_dbIdToRemove, context, out vector);
        
        return base.UpdateStateAfterBatchSentAsync(context, vector.Order);
    }

    public SubscriptionConnectionsState GetSubscriptionConnectionStateForShard()
    {
        var subscriptions = TcpConnection.DocumentDatabase.SubscriptionStorage.Subscriptions;
        var state = subscriptions.GetOrAdd(SubscriptionId, subId => new SubscriptionConnectionsStateForShard(DatabaseName, subId, TcpConnection.DocumentDatabase.SubscriptionStorage));
        _subscriptionConnectionsState = state;
        return state;
    }

    protected override string WhosTaskIsIt(DatabaseTopology topology, SubscriptionState subscriptionState) => 
        topology.WhoseTaskIsIt(_serverStore.Engine.CurrentState, subscriptionState, () =>
        {
            subscriptionState.SubscriptionShardingState.NodeTagPerShard.TryGetValue(ShardName, out var tag);
            return tag;
        });
}
