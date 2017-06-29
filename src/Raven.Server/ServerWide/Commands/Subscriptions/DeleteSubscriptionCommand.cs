using System;
using System.Collections.Generic;
using System.Text;
using Raven.Client.Documents;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.Json.Converters;
using Raven.Client.Server;
using Raven.Server.Json;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Voron;
using Voron.Data.Tables;

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


        public override string GetItemId()
        {
            throw new NotImplementedException();
        }

        public override unsafe void Execute(TransactionOperationContext context, Table items, long index, DatabaseRecord record, bool isPassive)
        {
            var itemKey = SubscriptionState.GenerateSubscriptionItemNameFromId(DatabaseName, index);
            using (Slice.From(context.Allocator, itemKey.ToLowerInvariant(), out Slice valueNameLowered))
            {
                if (items.ReadByKey(valueNameLowered, out TableValueReader tvr) == false)
                {
                    return; // nothing to do
                }

                var json = new BlittableJsonReaderObject(tvr.Pointer, tvr.Size, context);
                var subscriptionState = JsonDeserializationClient.SubscriptionState(json);
                
                items.DeleteByKey(valueNameLowered);
                
                if (string.IsNullOrEmpty(subscriptionState.SubscriptionName) == false)
                {
                    itemKey = SubscriptionState.GenerateSubscriptionItemKeyName(DatabaseName, subscriptionState.SubscriptionName);
                    using (Slice.From(context.Allocator, itemKey.ToLowerInvariant(), out valueNameLowered))
                    {
                        items.DeleteByKey(valueNameLowered);
                    }
                }
            }
        }

        public override void FillJson(DynamicJsonValue json)
        {
            json[nameof(SubscriptionId)] = SubscriptionId;
        }
    }
}
