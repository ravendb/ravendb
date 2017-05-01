using System;
using System.Collections.Generic;
using System.Text;
using Raven.Client.Documents;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands.Subscriptions
{
    public class DeleteSubscriptionCommand:UpdateDatabaseCommand
    {
        public long SubscriptionEtag;

        // for serialization
        private DeleteSubscriptionCommand():base(null){}

        public DeleteSubscriptionCommand(string databaseName) : base(databaseName)
        {
        }

        public override void UpdateDatabaseRecord(DatabaseRecord record, long etag)
        {
            record.DeleteSubscription(SubscriptionEtag);
        }

        public override void FillJson(DynamicJsonValue json)
        {
            json[nameof(SubscriptionEtag)] = SubscriptionEtag;
        }
    }
}
