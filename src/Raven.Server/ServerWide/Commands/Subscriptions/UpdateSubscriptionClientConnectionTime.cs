using System;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.Exceptions.Database;
using Raven.Client.Exceptions.Documents.Subscriptions;
using Raven.Client.ServerWide;
using Raven.Server.Rachis;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands.Subscriptions
{
    public class UpdateSubscriptionClientConnectionTime : UpdateValueForDatabaseCommand
    {
        public string SubscriptionName;
        public string NodeTag;
        public bool HasHighlyAvailableTasks;
        public DateTime LastClientConnectionTime;

        private UpdateSubscriptionClientConnectionTime()
        {
        }

        public UpdateSubscriptionClientConnectionTime(string databaseName, string uniqueRequestId) : base(databaseName, uniqueRequestId)
        {
        }

        public override string GetItemId() => SubscriptionState.GenerateSubscriptionItemKeyName(DatabaseName, SubscriptionName);

        protected override BlittableJsonReaderObject GetUpdatedValue(long index, RawDatabaseRecord record, JsonOperationContext context, BlittableJsonReaderObject existingValue)
        {
            var itemId = GetItemId();
            if (existingValue == null)
                throw new SubscriptionDoesNotExistException($"Subscription with id '{itemId}' does not exist");

            var subscription = JsonDeserializationCluster.SubscriptionState(existingValue);

            var topology = record.Topology;
            var lastResponsibleNode = AcknowledgeSubscriptionBatchCommand.GetLastResponsibleNode(HasHighlyAvailableTasks, topology, NodeTag);
            var appropriateNode = topology.WhoseTaskIsIt(RachisState.Follower, subscription, lastResponsibleNode);

            if (appropriateNode == null && record.DeletionInProgress.ContainsKey(NodeTag))
                throw new DatabaseDoesNotExistException(
                    $"Stopping subscription '{SubscriptionName}' on node {NodeTag}, because database '{DatabaseName}' is being deleted.");

            if (appropriateNode != NodeTag)
                throw new SubscriptionDoesNotBelongToNodeException(
                    $"Can't update subscription with name {itemId} by node {NodeTag}, because it's not its task to update this subscription");

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
