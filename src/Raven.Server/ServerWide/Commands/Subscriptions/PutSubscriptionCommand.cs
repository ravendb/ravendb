using System;
using Raven.Client.Documents.Replication.Messages;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.Extensions;
using Raven.Client.Json.Converters;
using Raven.Client.Server;
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
            var obj = context.ReadObject(new SubscriptionState {
                                Criteria = Criteria,
                                ChangeVector = InitialChangeVector,
                                SubscriptionId = subscriptionId,
                                SubscriptionName = SubscriptionName,
                                TimeOfLastClientActivity = DateTime.UtcNow,
                                Disabled = Disabled
            }.ToJson(), SubscriptionName);
            using (obj)
            {
                string subscriptionItemName = SubscriptionState.GenerateSubscriptionItemKeyName(DatabaseName, SubscriptionName);

                using (Slice.From(context.Allocator, subscriptionItemName, out Slice valueName))
                using (Slice.From(context.Allocator, subscriptionItemName.ToLowerInvariant(), out Slice valueNameLowered))
                {
                    if (items.ReadByKey(valueNameLowered, out TableValueReader tvr))
                    {
                        var ptr = tvr.Read(2, out int size);
                        var doc = new BlittableJsonReaderObject(ptr, size, context);

                        var subscriptionState = JsonDeserializationClient.SubscriptionState(doc);

                        if (SubscriptionId != subscriptionState.SubscriptionId)
                            throw new InvalidOperationException("A subscription could not be modified because the name '" + subscriptionItemName + "' is already in use in a subscription with different Id.");
                        
                    }

                    ClusterStateMachine.UpdateValue(subscriptionId, items, valueNameLowered, valueName, obj);
                }
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
