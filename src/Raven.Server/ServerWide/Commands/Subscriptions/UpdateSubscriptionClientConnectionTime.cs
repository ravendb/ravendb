using System;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.ServerWide;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands.Subscriptions
{
    public class UpdateSubscriptionClientConnectionTime:UpdateValueForDatabaseCommand    {
        public string SubscriptionName;
        public string NodeTag;
        public DateTime LastClientConnectionTime;

        private UpdateSubscriptionClientConnectionTime():base(null){}

        public UpdateSubscriptionClientConnectionTime(string databaseName) : base(databaseName)
        {
        }

        public override string GetItemId() => SubscriptionState.GenerateSubscriptionItemKeyName(DatabaseName, SubscriptionName);

        protected override BlittableJsonReaderObject GetUpdatedValue(long index, DatabaseRecord record, JsonOperationContext context, BlittableJsonReaderObject existingValue, bool isPassive)
        {
            var itemId = GetItemId();
            if (existingValue == null)
                throw new InvalidOperationException($"Subscription with id {itemId} does not exist");

            var subscription = JsonDeserializationCluster.SubscriptionState(existingValue);

            if (record.Topology.WhoseTaskIsIt(subscription, isPassive) != NodeTag)
                throw new InvalidOperationException($"Can't update subscription with name {itemId} by node {NodeTag}, because it's not it's task to update this subscription");


            subscription.LastClientConnectionTime = LastClientConnectionTime;

            return context.ReadObject(subscription.ToJson(), itemId);
        }

        public override void FillJson(DynamicJsonValue json)
        {
            json[nameof(SubscriptionName)] = SubscriptionName;
            json[nameof(NodeTag)] = NodeTag;
            json[nameof(LastClientConnectionTime)] = LastClientConnectionTime;
        }
    }
}
