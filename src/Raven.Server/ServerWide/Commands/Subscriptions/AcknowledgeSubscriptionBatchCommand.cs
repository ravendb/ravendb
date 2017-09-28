using System;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.Exceptions;
using Raven.Client.ServerWide;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands.Subscriptions
{
    public class AcknowledgeSubscriptionBatchCommand : UpdateValueForDatabaseCommand
    {
        public string ChangeVector;
        public string LastKnownSubscriptionChangeVector;
        public long SubscriptionId;
        public string SubscriptionName;
        public string NodeTag;
        public DateTime LastTimeServerMadeProgressWithDocuments;

        // for serializtion
        private AcknowledgeSubscriptionBatchCommand() : base(null) { }

        public AcknowledgeSubscriptionBatchCommand(string databaseName) : base(databaseName)
        {
        }

        public override string GetItemId() => SubscriptionState.GenerateSubscriptionItemKeyName(DatabaseName, SubscriptionName);

        protected override BlittableJsonReaderObject GetUpdatedValue(long index, DatabaseRecord record, JsonOperationContext context, BlittableJsonReaderObject existingValue, bool isPassive)
        {
            var subscriptionName = SubscriptionName;
            if (string.IsNullOrEmpty(subscriptionName))
            {
                subscriptionName = SubscriptionId.ToString();
            }

            if (existingValue == null)
                throw new InvalidOperationException($"Subscription with name {subscriptionName} does not exist");

            var subscription = JsonDeserializationCluster.SubscriptionState(existingValue);

            if (record.Topology.WhoseTaskIsIt(subscription, isPassive) != NodeTag)
                throw new InvalidOperationException($"Can't update subscription with name {subscriptionName} by node {NodeTag}, because it's not it's task to update this subscription");


            if (LastKnownSubscriptionChangeVector != subscription.ChangeVectorForNextBatchStartingPoint)
                throw new ConcurrencyException($"Can't acknowledge subscription with name {subscriptionName} due to inconsistency in change vector progress. Probably there was an admin intervention that changed the change vector value");

            subscription.ChangeVectorForNextBatchStartingPoint = ChangeVector;

            subscription.LastTimeServerMadeProgressWithDocuments = LastTimeServerMadeProgressWithDocuments;

            return context.ReadObject(subscription.ToJson(), subscriptionName);
        }

        public override void FillJson(DynamicJsonValue json)
        {
            json[nameof(ChangeVector)] = ChangeVector;
            json[nameof(SubscriptionId)] = SubscriptionId;
            json[nameof(SubscriptionName)] = SubscriptionName;
            json[nameof(NodeTag)] = NodeTag;
            json[nameof(LastTimeServerMadeProgressWithDocuments)] = LastTimeServerMadeProgressWithDocuments;
            json[nameof(LastKnownSubscriptionChangeVector)] = LastKnownSubscriptionChangeVector;
        }
    }
}
