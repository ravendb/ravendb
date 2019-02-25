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
using Raven.Server.Documents.Replication;
using Raven.Server.Rachis;

namespace Raven.Server.ServerWide.Commands.Subscriptions
{
    public class PutSubscriptionCommand : UpdateValueForDatabaseCommand
    {
        public string Query;
        public string InitialChangeVector;
        public long? SubscriptionId;
        public string SubscriptionName;
        public bool Disabled;
        public string MentorNode;

        // for serialization
        private PutSubscriptionCommand() : base(null) { }

        public PutSubscriptionCommand(string databaseName, string query, string mentor) : base(databaseName)
        {
            Query = query;
            MentorNode = mentor;
            // this verifies that the query is a valid subscription query
            SubscriptionConnection.ParseSubscriptionQuery(query);
        }

        protected override BlittableJsonReaderObject GetUpdatedValue(long index, DatabaseRecord record, JsonOperationContext context, BlittableJsonReaderObject existingValue, RachisState state)
        {
            throw new NotImplementedException();
        }

        public override unsafe void Execute(TransactionOperationContext context, Table items, long index, DatabaseRecord record, RachisState state, out object result)
        {
            long i = 1;
            var originalName = SubscriptionName;
            var tryToSetName = true;
            result = null;
            var subscriptionId = SubscriptionId ?? index;
            SubscriptionName = string.IsNullOrEmpty(SubscriptionName) ? subscriptionId.ToString() : SubscriptionName;
            var baseName = SubscriptionName;

            while (tryToSetName)
            {
                var subscriptionItemName = SubscriptionState.GenerateSubscriptionItemKeyName(DatabaseName, SubscriptionName);
                using (Slice.From(context.Allocator, subscriptionItemName, out Slice valueName))
                using (Slice.From(context.Allocator, subscriptionItemName.ToLowerInvariant(), out Slice valueNameLowered))
                {
                    if (items.ReadByKey(valueNameLowered, out TableValueReader tvr))
                    {
                        var ptr = tvr.Read(2, out int size);
                        var doc = new BlittableJsonReaderObject(ptr, size, context);

                        var existingSubscriptionState = JsonDeserializationClient.SubscriptionState(doc);

                        if (SubscriptionId != existingSubscriptionState.SubscriptionId)
                        {
                            if (string.IsNullOrEmpty(originalName))
                            {
                                SubscriptionName = $"{baseName}.{i}";
                                i++;
                                continue;
                            }
                            throw new RachisApplyException("A subscription could not be modified because the name '" + subscriptionItemName +
                                                           "' is already in use in a subscription with different Id.");
                        }

                        if (string.IsNullOrEmpty(InitialChangeVector) == false && InitialChangeVector == nameof(Constants.Documents.SubscriptionChangeVectorSpecialStates.DoNotChange))
                        {
                            InitialChangeVector = existingSubscriptionState.ChangeVectorForNextBatchStartingPoint;
                        }
                        else
                        {
                            AssertValidChangeVector();
                        }
                    }
                    else
                    {
                        AssertValidChangeVector();
                    }

                    using (var receivedSubscriptionState = context.ReadObject(new SubscriptionState
                    {
                        Query = Query,
                        ChangeVectorForNextBatchStartingPoint = InitialChangeVector,
                        SubscriptionId = subscriptionId,
                        SubscriptionName = SubscriptionName,
                        LastBatchAckTime = null,
                        Disabled = Disabled,
                        MentorNode = MentorNode,
                        LastClientConnectionTime = null
                    }.ToJson(), SubscriptionName))
                    {
                        ClusterStateMachine.UpdateValue(subscriptionId, items, valueNameLowered, valueName, receivedSubscriptionState);
                    }
                    tryToSetName = false;
                }
            }
        }

        private void AssertValidChangeVector()
        {
            try
            {
                InitialChangeVector.ToChangeVector();
            }
            catch (Exception e)
            {
                throw new RachisApplyException(
                    $"Received change vector {InitialChangeVector} is not in a valid format, therefore request cannot be processed.", e);
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
            json[nameof(MentorNode)] = MentorNode;
        }
    }
}
