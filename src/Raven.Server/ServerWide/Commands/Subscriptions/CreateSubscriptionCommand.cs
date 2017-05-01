using System;
using System.Collections.Generic;
using System.Text;
using Raven.Client.Documents;
using Raven.Client.Documents.Replication.Messages;
using Raven.Client.Documents.Subscriptions;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands.Subscriptions
{
    public class CreateSubscriptionCommand: UpdateDatabaseCommand
    {
        public SubscriptionCriteria Criteria;
        public ChangeVectorEntry[] InitialChangeVector;

        // for serialization
        private CreateSubscriptionCommand():base(null){}

        public CreateSubscriptionCommand(string databaseName) : base(databaseName)
        {
        }

        public override void UpdateDatabaseRecord(DatabaseRecord record, long etag)
        {
            record.CreateSubscription(Criteria,etag, InitialChangeVector);
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
