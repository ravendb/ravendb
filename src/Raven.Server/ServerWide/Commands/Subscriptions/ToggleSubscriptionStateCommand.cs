using System;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.Json.Converters;
using Raven.Client.Server;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Voron;
using Voron.Data.Tables;

namespace Raven.Server.ServerWide.Commands.Subscriptions
{
    public class ToggleSubscriptionStateCommand:UpdateValueForDatabaseCommand
    {
        public long SubscriptionId;
        public bool Disable;

        // for serialization
        private ToggleSubscriptionStateCommand():base(null){}

        public ToggleSubscriptionStateCommand(long subscriptionId, bool disable,string databaseName) : base(databaseName)
        {
            SubscriptionId = subscriptionId;
            Disable = disable;
        }


        public override string GetItemId()
        {
            throw new NotImplementedException();
        }

        public override unsafe void Execute(TransactionOperationContext context, Table items, long index, DatabaseRecord record, bool isPassive)
        {
            var itemKey = SubscriptionState.GenerateSubscriptionItemNameFromId(DatabaseName, SubscriptionId);
            using (Slice.From(context.Allocator, itemKey.ToLowerInvariant(), out Slice valueNameLowered))
            using (Slice.From(context.Allocator, itemKey, out Slice valueName))
            {
                if (items.ReadByKey(valueNameLowered, out TableValueReader tvr) == false)
                {
                    throw new InvalidOperationException("Cannot find subscription " + index);
                }

                var ptr = tvr.Read(2, out int size);
                var doc = new BlittableJsonReaderObject(ptr, size, context);

                var subscriptionState = JsonDeserializationClient.SubscriptionState(doc);
                subscriptionState.Disabled = Disable;
                using(var obj = context.ReadObject(subscriptionState.ToJson(), "subscription"))
                {
                    ClusterStateMachine.UpdateValue(index, items, valueNameLowered, valueName, obj);
                }
            }
        }

        public override void FillJson(DynamicJsonValue json)
        {
            json[nameof(SubscriptionId)] = SubscriptionId;
        }
    }
}