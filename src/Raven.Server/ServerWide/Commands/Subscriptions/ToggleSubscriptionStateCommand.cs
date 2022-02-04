using System;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.Exceptions.Documents.Subscriptions;
using Raven.Client.Json.Serialization;
using Raven.Client.ServerWide;
using Raven.Server.Rachis;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Voron;
using Voron.Data.Tables;

namespace Raven.Server.ServerWide.Commands.Subscriptions
{
    public class ToggleSubscriptionStateCommand : UpdateValueForDatabaseCommand
    {
        public string SubscriptionName;
        public bool Disable;

        // for serialization
        private ToggleSubscriptionStateCommand() { }

        public ToggleSubscriptionStateCommand(string subscriptionName, bool disable, string databaseName, string uniqueRequestId) : base(databaseName, uniqueRequestId)
        {
            if (string.IsNullOrEmpty(subscriptionName))
                throw new RachisApplyException($"Value cannot be null or empty. Param: {nameof(subscriptionName)}");
            if (string.IsNullOrEmpty(databaseName))
                throw new RachisApplyException($"Value cannot be null or empty. Param: {nameof(databaseName)}");

            SubscriptionName = subscriptionName;
            Disable = disable;
        }

        public override string GetItemId()
        {
            throw new NotImplementedException();
        }

        protected override BlittableJsonReaderObject GetUpdatedValue(long index, RawDatabaseRecord record, JsonOperationContext context, BlittableJsonReaderObject existingValue)
        {
            throw new NotImplementedException();
        }

        public override unsafe void Execute(ClusterOperationContext context, Table items, long index, RawDatabaseRecord record, RachisState state, out object result)
        {
            result = null;

            var itemKey = SubscriptionState.GenerateSubscriptionItemKeyName(DatabaseName, SubscriptionName);
            using (Slice.From(context.Allocator, itemKey.ToLowerInvariant(), out Slice valueNameLowered))
            using (Slice.From(context.Allocator, itemKey, out Slice valueName))
            {
                if (items.ReadByKey(valueNameLowered, out var tvr) == false)
                {
                    throw new SubscriptionDoesNotExistException($"Cannot find subscription {SubscriptionName} @ {DatabaseName}");
                }

                var ptr = tvr.Read(2, out int size);
                var doc = new BlittableJsonReaderObject(ptr, size, context);

                var subscriptionState = JsonDeserializationClient.SubscriptionState(doc);
                subscriptionState.Disabled = Disable;
                using (var obj = context.ReadObject(subscriptionState.ToJson(), "subscription"))
                {
                    ClusterStateMachine.UpdateValue(index, items, valueNameLowered, valueName, obj);
                }
            }
        }

        public override void FillJson(DynamicJsonValue json)
        {
            json[nameof(SubscriptionName)] = SubscriptionName;
            json[nameof(Disable)] = Disable;
        }
    }
}
