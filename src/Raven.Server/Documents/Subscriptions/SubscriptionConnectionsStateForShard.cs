using System.Collections.Generic;
using System.Threading.Tasks;
using Raven.Server.Documents.Sharding;
using Raven.Server.Documents.TcpHandlers;
using Raven.Server.ServerWide.Commands.Subscriptions;

namespace Raven.Server.Documents.Subscriptions;

public class SubscriptionConnectionsStateForShard : SubscriptionConnectionsState
{
    public ShardedDocumentDatabase ShardedDocumentDatabase;

    public SubscriptionConnectionsStateForShard(long subscriptionId, SubscriptionStorage storage) : base(subscriptionId, storage)
    {
        ShardedDocumentDatabase = (ShardedDocumentDatabase)DocumentDatabase;
    }

    protected override void SetLastChangeVectorSent(SubscriptionConnection connection)
    {
        if (connection.SubscriptionState.ChangeVectorForNextBatchStartingPointPerShard == null ||
            connection.SubscriptionState.ChangeVectorForNextBatchStartingPointPerShard.TryGetValue(DocumentDatabase.Name, out string cv) == false)
        {
            LastChangeVectorSent = null;
        }
        else
        {
            LastChangeVectorSent = cv;
        }
    }

    protected override AcknowledgeSubscriptionBatchCommand GetAcknowledgeSubscriptionBatchCommand(string changeVector, long? batchId, List<DocumentRecord> docsToResend)
    {
        var cmd = base.GetAcknowledgeSubscriptionBatchCommand(changeVector, batchId, docsToResend);
        cmd.ShardName = ShardedDocumentDatabase.Name;
        cmd.DatabaseName = ShardedDocumentDatabase.ShardedDatabaseName;
        return cmd;
    }

    protected override UpdateSubscriptionClientConnectionTime GetUpdateSubscriptionClientConnectionTime()
    {
        var cmd = base.GetUpdateSubscriptionClientConnectionTime();
        cmd.ShardName = ShardedDocumentDatabase.Name;
        cmd.DatabaseName = ShardedDocumentDatabase.ShardedDatabaseName;
        return cmd;
    }

    protected override Task<long> RecordBatchInternal(RecordBatchSubscriptionDocumentsCommand command)
    {
        command.DatabaseName = ShardedDocumentDatabase.ShardedDatabaseName;
        command.ShardName = ShardedDocumentDatabase.Name;
        return base.RecordBatchInternal(command);
    }
}
