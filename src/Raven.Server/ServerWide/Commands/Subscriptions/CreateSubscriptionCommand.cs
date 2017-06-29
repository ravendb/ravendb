using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;
using Raven.Client.Documents;
using Raven.Client.Documents.Replication.Messages;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.Server;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Voron;
using Voron.Data.Tables;

namespace Raven.Server.ServerWide.Commands.Subscriptions
{
    public class CreateSubscriptionCommand : UpdateValueForDatabaseCommand
    {
        public SubscriptionCriteria Criteria;
        public ChangeVectorEntry[] InitialChangeVector;

        public string SubscriptionName;
        
        // for serialization
        private CreateSubscriptionCommand() : base(null) { }

        public CreateSubscriptionCommand(string databaseName) : base(databaseName)
        {
        }

        public override void Execute(TransactionOperationContext context, Table items, long index, DatabaseRecord record, bool isPassive)
        {
            if (string.IsNullOrEmpty(SubscriptionName) == false)
            {
                var itemKey = SubscriptionState.GenerateSubscriptionItemKeyName(DatabaseName, SubscriptionName);
                using(var obj = context.ReadObject(new DynamicJsonValue
                {
                    ["SubscriptionId"] = index
                }, SubscriptionName))
                using (Slice.From(context.Allocator, itemKey, out Slice valueName))
                using (Slice.From(context.Allocator, itemKey.ToLowerInvariant(), out Slice valueNameLowered))
                {
                    if (items.ReadByKey(valueNameLowered, out TableValueReader _))
                    {
                        throw new InvalidOperationException("A subscription could not be created because the name '" + SubscriptionName + "' is already in use.");
                    }
                    ClusterStateMachine.UpdateValue(index, items, valueNameLowered, valueName, obj);
                }
            }
            
            var id = SubscriptionState.GenerateSubscriptionItemNameFromId(DatabaseName, index);
            using(var obj = context.ReadObject(new SubscriptionState
            {
                Criteria = Criteria,
                ChangeVector = InitialChangeVector,
                SubscriptionId = index,
                SubscriptionName = SubscriptionName,
                TimeOfLastClientActivity = DateTime.UtcNow
            }.ToJson(), SubscriptionName))
            using (Slice.From(context.Allocator, id, out Slice valueName))
            using (Slice.From(context.Allocator, id.ToLowerInvariant(), out Slice valueNameLowered))
            {
                ClusterStateMachine.UpdateValue(index, items, valueNameLowered, valueName, obj);
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
                [nameof(SubscriptionCriteria.IsVersioned)] = Criteria.IsVersioned
            };
            json[nameof(InitialChangeVector)] = InitialChangeVector?.ToJson();
            json[nameof(SubscriptionName)] = SubscriptionName;

        }
    }
}
