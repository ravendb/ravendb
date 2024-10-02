using System;
using Raven.Client;
using Raven.Client.Documents.DataArchival;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.Exceptions.Documents.Subscriptions;
using Raven.Client.Json.Serialization;
using Raven.Client.ServerWide;
using Raven.Server.Documents;
using Raven.Server.Documents.Replication;
using Raven.Server.Documents.Subscriptions;
using Raven.Server.Documents.TcpHandlers;
using Raven.Server.Rachis;
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
        public string MentorNode;
        public bool PinToMentorNode;
        public ArchivedDataProcessingBehavior? ArchivedDataProcessingBehavior;

        // for serialization
        protected PutSubscriptionCommand() { }

        public PutSubscriptionCommand(string databaseName, string query, string mentor, string uniqueRequestId) : base(databaseName, uniqueRequestId)
        {
            Query = query;
            MentorNode = mentor;
            // this verifies that the query is a valid subscription query
            SubscriptionConnection.ParseSubscriptionQuery(query);
        }

        protected override UpdatedValue GetUpdatedValue(long index, RawDatabaseRecord record, ClusterOperationContext context, BlittableJsonReaderObject existingValue)
        {
            throw new NotImplementedException();
        }

        public override unsafe void Execute(ClusterOperationContext context, Table items, long index, RawDatabaseRecord record, RachisState state, out object result)
        {
            long i = 1;
            var originalName = SubscriptionName;
            var tryToSetName = true;
            result = null;
            var subscriptionId = SubscriptionId ?? index;

            SubscriptionName = string.IsNullOrEmpty(SubscriptionName) ? subscriptionId.ToString() : SubscriptionName;
            var baseName = SubscriptionName;
            if (SubscriptionName.Length > DocumentIdWorker.MaxIdSize)
                throw new SubscriptionNameException($"Subscription Name is too long, must be at most {DocumentIdWorker.MaxIdSize} bytes");

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

                        HandleChangeVectorOnUpdate(context, existingSubscriptionState, subscriptionId);
                    }
                    else
                    {
                        AssertValidChangeVector();
                    }

                    using (var receivedSubscriptionState = context.ReadObject(CreateSubscriptionStateAsJson(subscriptionId, index), SubscriptionName))
                    {
                        ClusterStateMachine.UpdateValue(index, items, valueNameLowered, valueName, receivedSubscriptionState);
                    }
                    
                    tryToSetName = false;
                }
            }
        }

        protected virtual DynamicJsonValue CreateSubscriptionStateAsJson(long subscriptionId, long index)
        {
            var json = new SubscriptionState
            {
                Query = Query,
                ChangeVectorForNextBatchStartingPoint = InitialChangeVector,
                SubscriptionId = subscriptionId,
                SubscriptionName = SubscriptionName,
                LastBatchAckTime = null,
                Disabled = Disabled,
                MentorNode = MentorNode,
                PinToMentorNode = PinToMentorNode,
                LastClientConnectionTime = null,
                ArchivedDataProcessingBehavior = ArchivedDataProcessingBehavior,
                RaftCommandIndex = index
            }.ToJson();
            return json;
        }

        protected virtual void HandleChangeVectorOnUpdate(ClusterOperationContext context, SubscriptionState existingSubscriptionState, long subscriptionId)
        {
            if (InitialChangeVector == nameof(Constants.Documents.SubscriptionChangeVectorSpecialStates.DoNotChange))
            {
                InitialChangeVector = existingSubscriptionState.ChangeVectorForNextBatchStartingPoint;
            }
            else
            {
                AssertValidChangeVector();
                if (InitialChangeVector != existingSubscriptionState.ChangeVectorForNextBatchStartingPoint)
                {
                    // modified by the admin
                    RemoveSubscriptionStateFromStorage(context, subscriptionId);
                }
            }
        }

        protected void RemoveSubscriptionStateFromStorage(ClusterOperationContext context, long subscriptionId)
        {
            var subscriptionStateTable = context.Transaction.InnerTransaction.OpenTable(ClusterStateMachine.SubscriptionStateSchema, ClusterStateMachine.SubscriptionState);
            using (AbstractSubscriptionConnectionsState.GetDatabaseAndSubscriptionPrefix(context, DatabaseName, subscriptionId, out var prefix))
            {
                using var _ = Slice.External(context.Allocator, prefix, out var prefixSlice);
                subscriptionStateTable.DeleteByPrimaryKeyPrefix(prefixSlice);
            }
        }

        public long FindFreeId(ClusterOperationContext context, long subscriptionId)
        {
            if (SubscriptionId.HasValue)
                return SubscriptionId.Value;

            bool idTaken;
            do
            {
                idTaken = false;
                foreach (var keyValue in ClusterStateMachine.ReadValuesStartingWith(context,
                    SubscriptionState.SubscriptionPrefix(DatabaseName)))
                {
                    if (keyValue.Value.TryGet(nameof(SubscriptionState.SubscriptionId), out long id) == false)
                        continue;

                    if (id == subscriptionId)
                    {
                        subscriptionId--; //  we don't care if this end up as a negative value, we need only to be unique
                        idTaken = true;
                        break;
                    }
                }
            } while (idTaken);

            return subscriptionId;
        }

        protected virtual void AssertValidChangeVector()
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

        public bool IncludesRevisions() => Query.Contains(DocumentSubscriptions.IncludeRevisionsRQL);

        public override void FillJson(DynamicJsonValue json)
        {
            json[nameof(Query)] = Query;
            json[nameof(InitialChangeVector)] = InitialChangeVector;
            json[nameof(SubscriptionName)] = SubscriptionName;
            json[nameof(SubscriptionId)] = SubscriptionId;
            json[nameof(Disabled)] = Disabled;
            json[nameof(MentorNode)] = MentorNode;
            json[nameof(PinToMentorNode)] = PinToMentorNode;
            json[nameof(ArchivedDataProcessingBehavior)] = ArchivedDataProcessingBehavior;
        }
    }
}
