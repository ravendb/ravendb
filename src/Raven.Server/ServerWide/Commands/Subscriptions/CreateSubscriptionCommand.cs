using System;
using System.Collections.Generic;
using System.Text;
using Raven.Client.Documents;
using Raven.Client.Documents.Replication.Messages;
using Raven.Client.Documents.Subscriptions;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands.Subscriptions
{
    public class CreateSubscriptionCommand: UpdateValueForDatabaseCommand
    {
        public SubscriptionCriteria Criteria;
        public ChangeVectorEntry[] InitialChangeVector;

        private long? _subscriptionId;
        // for serialization
        private CreateSubscriptionCommand():base(null){}

        public CreateSubscriptionCommand(string databaseName) : base(databaseName)
        {
        }

        public override string GetItemId()
        {
            if (_subscriptionId.HasValue)
                return SubscriptionRaftState.GenerateSubscriptionItemName(DatabaseName, _subscriptionId.Value);
            return $"noValue";
        }

        public override DynamicJsonValue GetUpdatedValue(long index, DatabaseRecord record, BlittableJsonReaderObject existingValue)
        {
            if (existingValue != null)
                throw new InvalidOperationException(); // todo: should not happen
            _subscriptionId = index;
            var rafValue = new SubscriptionRaftState()
            {
                Criteria = Criteria,
                ChangeVector = InitialChangeVector,
                SubscriptionId = index
            };

            return rafValue.ToJson();

        }

        public override void FillJson(DynamicJsonValue json)
        {
            json[nameof(Criteria)] = new DynamicJsonValue()
            {
                [nameof(SubscriptionCriteria.Collection)] = Criteria.Collection,
                [nameof(SubscriptionCriteria.FilterJavaScript)] = Criteria.FilterJavaScript
            };
            json[nameof(InitialChangeVector)] = InitialChangeVector?.ToJson();
        }
    }
}
