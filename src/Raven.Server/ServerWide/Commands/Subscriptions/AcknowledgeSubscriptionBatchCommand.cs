using System;
using Raven.Client.Documents;
using Raven.Client.Documents.Replication.Messages;
using Raven.Client.Documents.Subscriptions;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands.Subscriptions
{
    public class AcknowledgeSubscriptionBatchCommand: UpdateValueForDatabaseCommand, IDatabaseTask
    {
        public ChangeVectorEntry[] ChangeVector;
        public long SubscriptionId;
        public string NodeTag;

        // for serializtion
        private AcknowledgeSubscriptionBatchCommand() : base(null){}

        public AcknowledgeSubscriptionBatchCommand(string databaseName) : base(databaseName)
        {
        }

        public override string GetItemId() => SubscriptionRaftState.GenerateSubscriptionItemName(DatabaseName, SubscriptionId);
        
        public override DynamicJsonValue GetUpdatedValue(long index, DatabaseRecord record, BlittableJsonReaderObject existingValue)
        {
            if (existingValue == null)
                throw new InvalidOperationException($"Subscription with id {SubscriptionId} does not exist");


            if (record.Topology.WhoseTaskIsIt(this) != NodeTag)
                throw new InvalidOperationException($"Can't update subscription with id {SubscriptionId} by node {NodeTag}, because it's not it's task to update this subscription");
            
            // todo: implement change vector comparison here, need to move some extention methods from server to client first
            return new DynamicJsonValue(existingValue)
            {
                [nameof(SubscriptionRaftState.ChangeVector)] = ChangeVector.ToJson(),
                [nameof(SubscriptionRaftState.TimeOfLastClientActivity)] = DateTime.UtcNow
            };
        }

        public override void FillJson(DynamicJsonValue json)
        {
            json[nameof(ChangeVector)] = ChangeVector?.ToJson();
            json[nameof(SubscriptionId)] = SubscriptionId;
            json[nameof(NodeTag)] = NodeTag;
        }

        public ulong GetTaskKey()
        {
            return (ulong)SubscriptionId;
        }
    }
}
