using System;
using JetBrains.Annotations;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.Json.Converters;
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
        private ToggleSubscriptionStateCommand() : base(null) { }

        public ToggleSubscriptionStateCommand([NotNull] string subscriptionName, bool disable, [NotNull] string databaseName) : base(databaseName)
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

        protected override BlittableJsonReaderObject GetUpdatedValue(long index, DatabaseRecord record, JsonOperationContext context, BlittableJsonReaderObject existingValue, RachisState state)
        {
            throw new NotImplementedException();
        }

        public override unsafe void Execute(TransactionOperationContext context, Table items, long index, DatabaseRecord record, RachisState state, out object result)
        {
            result = null;

            var itemKey = SubscriptionState.GenerateSubscriptionItemKeyName(DatabaseName, SubscriptionName);
            using (Slice.From(context.Allocator, itemKey.ToLowerInvariant(), out Slice valueNameLowered))
            using (Slice.From(context.Allocator, itemKey, out Slice valueName))
            {
                if (items.ReadByKey(valueNameLowered, out var tvr) == false)
                {
                    throw new RachisApplyException($"Cannot find subscription {index}");
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
