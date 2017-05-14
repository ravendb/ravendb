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
        public string SubscriptionId;
        public string NodeTag;
        public Guid DbId;
        public long LastDocumentEtagAckedInNode;

        // for serializtion
        private AcknowledgeSubscriptionBatchCommand() : base(null){}

        public AcknowledgeSubscriptionBatchCommand(string databaseName) : base(databaseName)
        {
        }

        public override string GetItemId() => SubscriptionId;
        public override BlittableJsonReaderObject GetUpdatedValue(long index, DatabaseRecord record, JsonOperationContext context, BlittableJsonReaderObject existingValue)
        {
            if (existingValue == null)
                throw new InvalidOperationException($"Subscription with id {SubscriptionId} does not exist");


            if (record.Topology.WhoseTaskIsIt(this) != NodeTag)
                throw new InvalidOperationException($"Can't update subscription with id {SubscriptionId} by node {NodeTag}, because it's not it's task to update this subscription");

            var subscripiton = new SubscriptionState();
            subscripiton.FillFromBlittableJson(existingValue);
            if (subscripiton.LastEtagReachedPedNode == null)
                subscripiton.LastEtagReachedPedNode = new Dictionary<Guid, long>();
            subscripiton.LastEtagReachedPedNode[DbId] = this.LastDocumentEtagAckedInNode;
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

        private ulong? taskKey;

        public ulong GetTaskKey()
        {
            if (taskKey.HasValue == false)
            {
                var lastSlashIndex = SubscriptionId.LastIndexOf("/");
                taskKey = ulong.Parse(SubscriptionId.Substring(lastSlashIndex + 1));
                return taskKey.Value;
            }
            return taskKey.Value;
        }
    }
}
