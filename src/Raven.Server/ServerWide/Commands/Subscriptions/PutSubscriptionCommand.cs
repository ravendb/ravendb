using System;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.Json.Converters;
using Raven.Client.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Voron;
using Voron.Data.Tables;

namespace Raven.Server.ServerWide.Commands.Subscriptions
{
    public class PutSubscriptionCommand : UpdateValueForDatabaseCommand
    {
        public SubscriptionCriteria Criteria;
        public string InitialChangeVector;
        public long? SubscriptionId; 
        public string SubscriptionName;
        public bool Disabled;
        
        // for serialization
        private PutSubscriptionCommand() : base(null) { }

        public PutSubscriptionCommand(string databaseName) : base(databaseName)
        {
        }

        public override unsafe void Execute(TransactionOperationContext context, Table items, long index, DatabaseRecord record, bool isPassive)
        {
            var subscriptionId = SubscriptionId ?? index;
            SubscriptionName = string.IsNullOrEmpty(SubscriptionName) ? subscriptionId.ToString() : SubscriptionName;
            var receivedSubscriptionState = context.ReadObject(new SubscriptionState {
                                Criteria = Criteria,
                                ChangeVector = InitialChangeVector,
                                SubscriptionId = subscriptionId,
                                SubscriptionName = SubscriptionName,
                                TimeOfLastClientActivity = DateTime.UtcNow,
                                Disabled = Disabled
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

                        if (InitialChangeVector == Raven.Client.Constants.Documents.UnchangedSubscriptionsChangeVecotr)
                        {
                            if (receivedSubscriptionState.Modifications == null)
                                receivedSubscriptionState.Modifications = new DynamicJsonValue();

                            receivedSubscriptionState.Modifications[nameof(SubscriptionState.ChangeVector)] = existingSubscriptionState.ChangeVector;
                            modifiedSubscriptionState = context.ReadObject(receivedSubscriptionState, SubscriptionName);
                        }
                    }

                    ClusterStateMachine.UpdateValue(subscriptionId, items, valueNameLowered, valueName, modifiedSubscriptionState??receivedSubscriptionState);
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
            json[nameof(Criteria)] = new DynamicJsonValue()
            {
                [nameof(SubscriptionCriteria.Collection)] = Criteria.Collection,
                [nameof(SubscriptionCriteria.Script)] = Criteria.Script,
                [nameof(SubscriptionCriteria.IncludeRevisions)] = Criteria.IncludeRevisions
            };
            json[nameof(InitialChangeVector)] = InitialChangeVector;
            json[nameof(SubscriptionName)] = SubscriptionName;
            json[nameof(SubscriptionId)] = SubscriptionId;
            json[nameof(Disabled)] = Disabled;

        }
    }
}
