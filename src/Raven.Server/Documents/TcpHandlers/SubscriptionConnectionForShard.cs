using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.ServerWide;
using Raven.Server.Documents.Includes;
using Raven.Server.Documents.Sharding;
using Raven.Server.Documents.Sharding.Subscriptions;
using Raven.Server.Documents.Subscriptions;
using Raven.Server.Documents.Subscriptions.Sharding;
using Raven.Server.Documents.Subscriptions.SubscriptionProcessor;
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
    private SubscriptionConnectionsStateForShard _state;

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
        DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Karmel, DevelopmentHelper.Severity.Normal, "RavenDB-19085 need to ensure the sharded workers has the same sub definition. by sending my raft index?");
        return base.AcceptMessage();
    }

    protected override RawDatabaseRecord GetRecord(ClusterOperationContext context) => _serverStore.Cluster.ReadRawDatabaseRecord(context, ShardName);

    protected override string SetLastChangeVectorInThisBatch(IChangeVectorOperationContext context, string currentLast, Document sentDocument)
    {
        if (sentDocument.Etag == 0) // got this document from resend
            return currentLast;

        var vector = context.GetChangeVector(sentDocument.ChangeVector);

        var result = ChangeVectorUtils.MergeVectors(
            currentLast,
            ChangeVectorUtils.NewChangeVector(_serverStore.NodeTag, sentDocument.Etag, _shardedDatabase.DbBase64Id),
            vector.Order);

        return result;
    }

    public override AbstractSubscriptionProcessor<DatabaseIncludesCommandImpl> CreateProcessor(SubscriptionConnectionBase<DatabaseIncludesCommandImpl> connection)
    {
        if (connection is SubscriptionConnectionForShard shardConnection)
        {
            var database = connection.TcpConnection.DocumentDatabase as ShardedDocumentDatabase;
            var server = database.ServerStore;

            if (connection.Subscription.Revisions)
            {
                return new ShardedRevisionsDatabaseSubscriptionProcessor(server, database, shardConnection);
            }

            return new ShardedDocumentsDatabaseSubscriptionProcessor(server, database, shardConnection);
        }

        throw new InvalidOperationException($"Expected to create a processor for '{nameof(SubscriptionConnectionForShard)}', but got: '{connection.GetType().Name}'.");
    }

    protected override async Task UpdateStateAfterBatchSentAsync(IChangeVectorOperationContext context, string lastChangeVectorSentInThisBatch)
    {
        var vector = context.GetChangeVector(lastChangeVectorSentInThisBatch);
        vector.TryRemoveIds(_dbIdToRemove, context, out vector);

        await base.UpdateStateAfterBatchSentAsync(context, vector.Order);

        var p = Processor as ShardedDocumentsDatabaseSubscriptionProcessor;
        if (p.Skipped != null)
        {
            for (int i = CurrentBatch.Count - 1; i >= 0; i--)
            {
                var item = CurrentBatch[i];
                if (p.Skipped.Contains(item.Document.Id))
                {
                    CurrentBatch.RemoveAt(i);
                    item.Document.Dispose();
                }
            }
        }
    }

    protected override bool FoundAboutMoreDocs()
    {
        if (base.FoundAboutMoreDocs())
            return true;

        if (_state.HasDocumentFromResend())
            return true;

        return false;
    }

    public SubscriptionConnectionsState GetSubscriptionConnectionStateForShard()
    {
        var subscriptions = TcpConnection.DocumentDatabase.SubscriptionStorage.Subscriptions;
        var state = subscriptions.GetOrAdd(SubscriptionId, subId => new SubscriptionConnectionsStateForShard(DatabaseName, subId, TcpConnection.DocumentDatabase.SubscriptionStorage));
        State = state;
        _state = (SubscriptionConnectionsStateForShard)state;
        return state;
    }

    protected override void FillIncludedDocuments(DatabaseIncludesCommandImpl includeDocumentsCommand, List<Document> includes)
    {
        includeDocumentsCommand.IncludeDocumentsCommand.Fill(includes, includeMissingAsNull: true);
    }
}
