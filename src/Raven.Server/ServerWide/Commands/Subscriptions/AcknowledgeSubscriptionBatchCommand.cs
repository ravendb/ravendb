using System;
using System.Collections.Generic;
using Raven.Client.Documents;
using Raven.Client.Documents.Replication.Messages;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.Server;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands.Subscriptions
{
    public class AcknowledgeSubscriptionBatchCommand: UpdateValueForDatabaseCommand, IDatabaseTask
    {
        public ChangeVectorEntry[] ChangeVector;
        public long SubscriptionId;
        public string NodeTag;
        public Guid DbId;
        public long LastEtagInDbId;

        // for serializtion
        private AcknowledgeSubscriptionBatchCommand() : base(null){}

        public AcknowledgeSubscriptionBatchCommand(string databaseName) : base(databaseName)
        {
        }

        public override string GetItemId() => SubscriptionState.GenerateSubscriptionItemName(DatabaseName, SubscriptionId);
        public override BlittableJsonReaderObject GetUpdatedValue(long index, DatabaseRecord record, JsonOperationContext context, BlittableJsonReaderObject existingValue)
        {
            if (existingValue == null)
                throw new InvalidOperationException($"Subscription with id {SubscriptionId} does not exist");


            if (record.Topology.WhoseTaskIsIt(this) != NodeTag)
                throw new InvalidOperationException($"Can't update subscription with id {SubscriptionId} by node {NodeTag}, because it's not it's task to update this subscription");

            var subscripiton = new SubscriptionState();
            subscripiton.FillFromBlittableJson(existingValue);
            if (subscripiton.LastEtagReachedInServer == null)
                subscripiton.LastEtagReachedInServer = new Dictionary<Guid, long>();
            subscripiton.LastEtagReachedInServer[DbId] = this.LastEtagInDbId;
            subscripiton.ChangeVector = ChangeVector;
            subscripiton.TimeOfLastClientActivity = DateTime.UtcNow;

            // todo: implement change vector comparison here, need to move some extention methods from server to client first
            
            return context.ReadObject(subscripiton.ToJson(), GetItemId());
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
