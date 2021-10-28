using System;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.Json.Serialization;
using Raven.Client.ServerWide;
using Raven.Server.Documents.Subscriptions;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Voron;
using Voron.Data.Tables;

namespace Raven.Server.ServerWide.Commands.Subscriptions
{
    public class DeleteSubscriptionCommand : UpdateValueForDatabaseCommand
    {
        public string SubscriptionName;

        // for serialization
        private DeleteSubscriptionCommand() { }

        public DeleteSubscriptionCommand(string databaseName, string subscriptionName, string uniqueRequestId) : base(databaseName, uniqueRequestId)
        {
            if (string.IsNullOrEmpty(subscriptionName))
                throw new ArgumentNullException(nameof(subscriptionName));
            SubscriptionName = subscriptionName;
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
            {
                if (items.ReadByKey(valueNameLowered, out TableValueReader tvr) == false)
                {
                    return; // nothing to do
                }

                var ptr = tvr.Read(2, out int size);
                var doc = new BlittableJsonReaderObject(ptr, size, context);

                var subscriptionState = JsonDeserializationClient.SubscriptionState(doc);
                items.DeleteByKey(valueNameLowered);

                if (string.IsNullOrEmpty(subscriptionState.SubscriptionName) == false)
                {
                    itemKey = SubscriptionState.GenerateSubscriptionItemKeyName(DatabaseName, subscriptionState.SubscriptionName);
                    using (Slice.From(context.Allocator, itemKey.ToLowerInvariant(), out valueNameLowered))
                    {
                        items.DeleteByKey(valueNameLowered);
                    }

                    using (SubscriptionConnectionsState.GetDatabaseAndSubscriptionPrefix(context, DatabaseName, subscriptionState.SubscriptionId, out var prefix))
                    {
                        var subscriptionStateTable = context.Transaction.InnerTransaction.OpenTable(ClusterStateMachine.SubscriptionStateSchema, ClusterStateMachine.SubscriptionState);
                        using var _ = Slice.External(context.Allocator, prefix, out var prefixSlice);
                        subscriptionStateTable.DeleteByPrimaryKeyPrefix(prefixSlice);
                    }
                }
            }
        }

        public override void FillJson(DynamicJsonValue json)
        {
            json[nameof(SubscriptionName)] = SubscriptionName;
        }
    }
}
