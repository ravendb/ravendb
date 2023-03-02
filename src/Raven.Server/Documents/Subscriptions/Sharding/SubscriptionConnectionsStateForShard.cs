using System.Collections.Generic;
using System.Threading.Tasks;
using Raven.Server.Documents.Sharding;
using Raven.Server.Documents.TcpHandlers;
using Raven.Server.ServerWide.Commands.Subscriptions;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;

namespace Raven.Server.Documents.Subscriptions.Sharding;

public class SubscriptionConnectionsStateForShard : SubscriptionConnectionsState
{
    public ShardedDocumentDatabase ShardedDocumentDatabase;

    public SubscriptionConnectionsStateForShard(string databaseName, long subscriptionId, SubscriptionStorage storage) : base(databaseName, subscriptionId, storage)
    {
        ShardedDocumentDatabase = (ShardedDocumentDatabase)DocumentDatabase;
    }

    protected override void SetLastChangeVectorSent(SubscriptionConnection connection)
    {
        if (connection.SubscriptionState.ShardingState.ChangeVectorForNextBatchStartingPointPerShard == null ||
            connection.SubscriptionState.ShardingState.ChangeVectorForNextBatchStartingPointPerShard.TryGetValue(DocumentDatabase.Name, out string cv) == false)
        {
            LastChangeVectorSent = null;
        }
        else
        {
            LastChangeVectorSent = cv;
        }
    }

    public override Task UpdateClientConnectionTime()
    {
        // the orchestrator will update the client connection time
        return Task.CompletedTask;
    }

    protected override AcknowledgeSubscriptionBatchCommand GetAcknowledgeSubscriptionBatchCommand(string changeVector, long batchId, List<DocumentRecord> docsToResend)
    {
        var cmd = base.GetAcknowledgeSubscriptionBatchCommand(changeVector, batchId, docsToResend);
        cmd.ShardName = ShardedDocumentDatabase.Name;
        cmd.DatabaseName = ShardedDocumentDatabase.ShardedDatabaseName;
        return cmd;
    }

    protected override void ValidateTakeOver(SubscriptionConnection currentConnection)
    {
        // we let take over to override even an existing subscription with take over
    }

    protected override Task<(long Index, object Skipped)> RecordBatchInternal(RecordBatchSubscriptionDocumentsCommand command)
    {
        command.DatabaseName = ShardedDocumentDatabase.ShardedDatabaseName;
        command.ShardName = ShardedDocumentDatabase.Name;
        command.ActiveBatchesFromSender = GetActiveBatches();
        return base.RecordBatchInternal(command);
    }


    public bool HasDocumentFromResend()
    {
        using (_server.Engine.ContextPool.AllocateOperationContext(out ClusterOperationContext context))
        using (context.OpenReadTransaction())
        {
            foreach (var item in GetDocumentsFromResend(context, GetActiveBatches()))
            {
                var shard = ShardHelper.GetShardNumberFor(ShardedDocumentDatabase.ShardingConfiguration, context, item.DocumentId);
                if (shard == ShardedDocumentDatabase.ShardNumber)
                    return true;
            }
        }

        return false;
    }
}
