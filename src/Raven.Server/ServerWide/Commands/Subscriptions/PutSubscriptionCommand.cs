using System;
using Raven.Client;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.Json.Converters;
using Raven.Client.ServerWide;
using Raven.Server.Documents.TcpHandlers;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Voron;
using Voron.Data.Tables;

namespace Raven.Server.ServerWide.Commands.Subscriptions
{
    public class PutSubscriptionCommand : UpdateValueForDatabaseCommand
    {
        public string Query;
        public string InitialChangeVector;
        public long? SubscriptionId;
        public string SubscriptionName;
        public bool Disabled;

        // for serialization
        private PutSubscriptionCommand() : base(null) { }

        public PutSubscriptionCommand(string databaseName, string query) : base(databaseName)
        {
            Query = query;
            // this verifies that the query is a valid subscription query
            SubscriptionConnection.ParseSubscriptionQuery(query);
        }

        protected override BlittableJsonReaderObject GetUpdatedValue(long index, DatabaseRecord record, JsonOperationContext context, BlittableJsonReaderObject existingValue, bool isPassive)
        {
            throw new NotImplementedException();
        }

        public override unsafe void Execute(TransactionOperationContext context, Table items, long index, DatabaseRecord record, bool isPassive, out object result)
        {
            result = null;
            var subscriptionId = SubscriptionId ?? index;
            SubscriptionName = string.IsNullOrEmpty(SubscriptionName) ? subscriptionId.ToString() : SubscriptionName;
            var receivedSubscriptionState = context.ReadObject(new SubscriptionState
            {
                Query = Query,
                ChangeVectorForNextBatchStartingPoint = InitialChangeVector,
                SubscriptionId = subscriptionId,
                SubscriptionName = SubscriptionName,
                LastTimeServerMadeProgressWithDocuments = DateTime.UtcNow,
                Disabled = Disabled,
                LastClientConnectionTime = DateTime.Now
            }.ToJson(), SubscriptionName);
            BlittableJsonReaderObject modifiedSubscriptionState = null;
            try
            {
                string subscriptionItemName = SubscriptionState.GenerateSubscriptionItemKeyName(DatabaseName, SubscriptionName);

                using (Slice.From(context.Allocator, subscriptionItemName, out Slice valueName))
                using (Slice.From(context.Allocator, subscriptionItemName.ToLowerInvariant(), out Slice valueNameLowered))
                {
                    if (items.ReadByKey(valueNameLowered, out TableValueReader tvr))
                    {
                        var ptr = tvr.Read(2, out int size);
                        var doc = new BlittableJsonReaderObject(ptr, size, context);

                        var existingSubscriptionState = JsonDeserializationClient.SubscriptionState(doc);

                        if (SubscriptionId != existingSubscriptionState.SubscriptionId)
                            throw new InvalidOperationException("A subscription could not be modified because the name '" + subscriptionItemName +
                                                                "' is already in use in a subscription with different Id.");

                        if (Enum.TryParse(InitialChangeVector, out Constants.Documents.SubscriptionChangeVectorSpecialStates changeVectorState) && changeVectorState == Constants.Documents.SubscriptionChangeVectorSpecialStates.DoNotChange)
                        {
                            if (receivedSubscriptionState.Modifications == null)
                                receivedSubscriptionState.Modifications = new DynamicJsonValue();

                            receivedSubscriptionState.Modifications[nameof(SubscriptionState.ChangeVectorForNextBatchStartingPoint)] = existingSubscriptionState.ChangeVectorForNextBatchStartingPoint;
                            modifiedSubscriptionState = context.ReadObject(receivedSubscriptionState, SubscriptionName);
                        }
                    }

                    ClusterStateMachine.UpdateValue(subscriptionId, items, valueNameLowered, valueName, modifiedSubscriptionState ?? receivedSubscriptionState);
                }
            }
            finally
            {
                receivedSubscriptionState.Dispose();
                modifiedSubscriptionState?.Dispose();
            }
        }

        public override string GetItemId()
        {
            throw new NotImplementedException();
        }

        public override void FillJson(DynamicJsonValue json)
        {
            json[nameof(Query)] = Query;
            json[nameof(InitialChangeVector)] = InitialChangeVector;
            json[nameof(SubscriptionName)] = SubscriptionName;
            json[nameof(SubscriptionId)] = SubscriptionId;
            json[nameof(Disabled)] = Disabled;

        }
    }
}
