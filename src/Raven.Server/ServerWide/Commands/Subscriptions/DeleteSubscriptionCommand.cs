using System;
using System.Collections.Generic;
using System.Text;
using Raven.Client.Documents;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.Server;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands.Subscriptions
{
    public class DeleteSubscriptionCommand:UpdateValueForDatabaseCommand
    {
        public long SubscriptionId;

        // for serialization
        private DeleteSubscriptionCommand():base(null){}

        public DeleteSubscriptionCommand(string databaseName) : base(databaseName)
        {
        }

        public override string GetItemId() => SubscriptionRaftState.GenerateSubscriptionItemName(DatabaseName, SubscriptionId);

        public override DynamicJsonValue GetUpdatedValue(long idnex, DatabaseRecord record, BlittableJsonReaderObject existingValue)
        {
            if (existingValue == null)
            {
                throw new InvalidOperationException($"Cannot delete subscription with Id {SubscriptionId}, it was not found");
            }

            return null;
        }

        public override void FillJson(DynamicJsonValue json)
        {
            json[nameof(SubscriptionId)] = SubscriptionId;
        }
    }
}
