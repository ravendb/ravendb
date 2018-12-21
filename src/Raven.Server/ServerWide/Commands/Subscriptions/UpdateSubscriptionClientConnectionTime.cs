using System;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.ServerWide;
using Raven.Server.Rachis;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands.Subscriptions
{
    public class UpdateSubscriptionClientConnectionTime : UpdateValueForDatabaseCommand {
        public string SubscriptionName;
        public string NodeTag;
        public bool HasHighlyAvailableTasks;
        public DateTime LastClientConnectionTime;

        private UpdateSubscriptionClientConnectionTime():base(null){}

        public UpdateSubscriptionClientConnectionTime(string databaseName) : base(databaseName)
        {
        }

        public override string GetItemId() => SubscriptionState.GenerateSubscriptionItemKeyName(DatabaseName, SubscriptionName);

        protected override BlittableJsonReaderObject GetUpdatedValue(long index, DatabaseRecord record, JsonOperationContext context, BlittableJsonReaderObject existingValue, RachisState state)
        {
            var itemId = GetItemId();
            if (existingValue == null)
                throw new RachisApplyException($"Subscription with id {itemId} does not exist");

            var subscription = JsonDeserializationCluster.SubscriptionState(existingValue);

            var lastResponsibleNode = AcknowledgeSubscriptionBatchCommand.GetLastResponsibleNode(HasHighlyAvailableTasks, record.Topology, NodeTag);
            if (record.Topology.WhoseTaskIsIt(state, subscription, lastResponsibleNode) != NodeTag)
                throw new RachisApplyException($"Can't update subscription with name {itemId} by node {NodeTag}, because it's not it's task to update this subscription");

            subscription.LastClientConnectionTime = LastClientConnectionTime;
            subscription.NodeTag = NodeTag;

            return context.ReadObject(subscription.ToJson(), itemId);
        }

        public override void FillJson(DynamicJsonValue json)
        {
            json[nameof(SubscriptionName)] = SubscriptionName;
            json[nameof(NodeTag)] = NodeTag;
            json[nameof(HasHighlyAvailableTasks)] = HasHighlyAvailableTasks;
            json[nameof(LastClientConnectionTime)] = LastClientConnectionTime;
        }
    }
}
