using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;
using Raven.Client.Documents;
using Raven.Client.Documents.Replication.Messages;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.Server;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands.Subscriptions
{
    public class CreateSubscriptionCommand : UpdateValueForDatabaseCommand
    {
        public SubscriptionCriteria Criteria;
        public ChangeVectorEntry[] InitialChangeVector;

        private long? _subscriptionId;
        // for serialization
        private CreateSubscriptionCommand() : base(null) { }

        public CreateSubscriptionCommand(string databaseName) : base(databaseName)
        {
        }

        public override string GetItemId()
        {
            if (_subscriptionId.HasValue)
                return SubscriptionState.GenerateSubscriptionItemName(DatabaseName, _subscriptionId.Value);
            return "does/not/exists/" + Guid.NewGuid(); // missing value
        }

        public override BlittableJsonReaderObject GetUpdatedValue(long index, DatabaseRecord record, JsonOperationContext context, BlittableJsonReaderObject existingValue)
        {
            Debug.Assert(existingValue == null);

            _subscriptionId = index;
            var rafValue = new SubscriptionState
            {
                Criteria = Criteria,
                ChangeVector = InitialChangeVector,
                SubscriptionId = SubscriptionState.GenerateSubscriptionItemName(record.DatabaseName, index)
            };

            return context.ReadObject(rafValue.ToJson(), GetItemId());
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
