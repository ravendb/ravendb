using System;
using System.Collections.Generic;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.ServerWide;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands.Subscriptions
{
    public class AcknowledgeSubscriptionBatchCommand : UpdateValueForDatabaseCommand, IDatabaseTask
    {
        public string ChangeVector;
        public long SubscriptionId;
        public string SubscriptionName;
        public string NodeTag;
        public Guid DbId;
        public long LastDocumentEtagAckedInNode;

        // for serializtion
        private AcknowledgeSubscriptionBatchCommand() : base(null) { }

        public AcknowledgeSubscriptionBatchCommand(string databaseName) : base(databaseName)
        {
        }

        public override string GetItemId() => SubscriptionState.GenerateSubscriptionItemKeyName(DatabaseName, SubscriptionName);

        protected override BlittableJsonReaderObject GetUpdatedValue(long index, DatabaseRecord record, JsonOperationContext context, BlittableJsonReaderObject existingValue, bool isPassive)
        {

            var itemId = GetItemId();
            if (existingValue == null)
                throw new InvalidOperationException($"Subscription with id {itemId} does not exist");

            if (record.Topology.WhoseTaskIsIt(this, isPassive) != NodeTag)
                throw new InvalidOperationException($"Can't update subscription with name {itemId} by node {NodeTag}, because it's not it's task to update this subscription");

            var subscription = JsonDeserializationCluster.SubscriptionState(existingValue);
            
            if (subscription.LastEtagReachedInServer == null)
                subscription.LastEtagReachedInServer = new Dictionary<string, long>();
            subscription.LastEtagReachedInServer[DbId.ToString()] = LastDocumentEtagAckedInNode;
            subscription.ChangeVector = ChangeVector;
            subscription.TimeOfLastClientActivity = DateTime.UtcNow;

            return context.ReadObject(subscription.ToJson(), itemId);
        }

        public override void FillJson(DynamicJsonValue json)
        {
            json[nameof(ChangeVector)] = ChangeVector;
            json[nameof(SubscriptionId)] = SubscriptionId;
            json[nameof(SubscriptionName)] = SubscriptionName;
            json[nameof(NodeTag)] = NodeTag;
        }

        public ulong GetTaskKey()
        {
            return (ulong)SubscriptionId;
        }
    }
}
