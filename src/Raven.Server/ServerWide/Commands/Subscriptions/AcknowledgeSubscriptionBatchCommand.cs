using System;
using System.Collections;
using System.Collections.Generic;
using Raven.Client;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.Exceptions.Database;
using Raven.Client.Exceptions.Documents.Subscriptions;
using Raven.Client.ServerWide;
using Raven.Server.Documents.Subscriptions;
using Raven.Server.Documents.TcpHandlers;
using Raven.Server.Rachis;
using Raven.Server.ServerWide.Context;
using Sparrow.Binary;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Server.Utils;
using Voron;
using Voron.Data.Tables;

namespace Raven.Server.ServerWide.Commands.Subscriptions
{
    public class AcknowledgeSubscriptionBatchCommand : UpdateValueForDatabaseCommand
    {
        public string ChangeVector;
        public string LastKnownSubscriptionChangeVector; // Now this used only for backward compatibility
        public long SubscriptionId;
        public string SubscriptionName;
        public string NodeTag;
        public bool HasHighlyAvailableTasks;
        public DateTime LastTimeServerMadeProgressWithDocuments;

        public long? BatchId;
        public List<DocumentRecord> DocumentsToResend; // documents that were updated while this batch was processing 

        // for serialization
        private AcknowledgeSubscriptionBatchCommand() { }

        public AcknowledgeSubscriptionBatchCommand(string databaseName, string uniqueRequestId) : base(databaseName, uniqueRequestId)
        {
        }

        public override string GetItemId() => SubscriptionState.GenerateSubscriptionItemKeyName(DatabaseName, SubscriptionName);

        protected override BlittableJsonReaderObject GetUpdatedValue(long index, RawDatabaseRecord record, JsonOperationContext context, BlittableJsonReaderObject existingValue)
        {
            var subscriptionName = SubscriptionName;
            if (string.IsNullOrEmpty(subscriptionName))
            {
                subscriptionName = SubscriptionId.ToString();
            }

            if (existingValue == null)
                throw new SubscriptionDoesNotExistException($"Subscription with name '{subscriptionName}' does not exist");

            var subscription = JsonDeserializationCluster.SubscriptionState(existingValue);
            var topology = record.Topology;
            var lastResponsibleNode = GetLastResponsibleNode(HasHighlyAvailableTasks, topology, NodeTag);
            var appropriateNode = topology.WhoseTaskIsIt(RachisState.Follower, subscription, lastResponsibleNode);
            if (appropriateNode == null && record.DeletionInProgress.ContainsKey(NodeTag))
                throw new DatabaseDoesNotExistException($"Stopping subscription '{subscriptionName}' on node {NodeTag}, because database '{DatabaseName}' is being deleted.");

            if (appropriateNode != NodeTag)
            {
                throw new SubscriptionDoesNotBelongToNodeException(
                    $"Cannot apply {nameof(AcknowledgeSubscriptionBatchCommand)} for subscription '{subscriptionName}' with id '{SubscriptionId}', on database '{DatabaseName}', on node '{NodeTag}'," +
                    $" because the subscription task belongs to '{appropriateNode ?? "N/A"}'.")
                { AppropriateNode = appropriateNode };
            }

            if (ChangeVector == nameof(Constants.Documents.SubscriptionChangeVectorSpecialStates.DoNotChange))
            {
                return context.ReadObject(existingValue, SubscriptionName);
            }

            if (IsLegacyCommand())
            {
                if (LastKnownSubscriptionChangeVector != subscription.ChangeVectorForNextBatchStartingPoint)
                    throw new SubscriptionChangeVectorUpdateConcurrencyException($"Can't acknowledge subscription with name {subscriptionName} due to inconsistency in change vector progress. Probably there was an admin intervention that changed the change vector value. Stored value: {subscription.ChangeVectorForNextBatchStartingPoint}, received value: {LastKnownSubscriptionChangeVector}");

                subscription.ChangeVectorForNextBatchStartingPoint = ChangeVector;
            }
            
            subscription.NodeTag = NodeTag;
            subscription.LastBatchAckTime = LastTimeServerMadeProgressWithDocuments;
            
            return context.ReadObject(subscription.ToJson(), subscriptionName);
        }

        private bool IsLegacyCommand()
        {
            return BatchId == null || // from old node
                   BatchId == SubscriptionConnection.NonExistentBatch; // from new node, but has lower cluster version
        }

        public override void Execute(ClusterOperationContext context, Table items, long index, RawDatabaseRecord record, RachisState state, out object result)
        {
            base.Execute(context, items, index, record, state, out result);

            if (IsLegacyCommand()) 
                return;

            ExecuteAcknowledgeSubscriptionBatch(context, index);
        }

        private unsafe void ExecuteAcknowledgeSubscriptionBatch(ClusterOperationContext context, long index)
        {
            if (SubscriptionId == default)
            {
                throw new RachisApplyException(
                    $"'{nameof(SubscriptionId)}' is missing in '{nameof(AcknowledgeSubscriptionBatchCommand)}'.");
            }

            if (DatabaseName == default)
            {
                throw new RachisApplyException($"'{nameof(DatabaseName)}' is missing in '{nameof(AcknowledgeSubscriptionBatchCommand)}'.");
            }

            if (BatchId == null)
            {
                throw new RachisApplyException($"'{nameof(BatchId)}' is missing in '{nameof(AcknowledgeSubscriptionBatchCommand)}'.");
            }
            
            var subscriptionStateTable = context.Transaction.InnerTransaction.OpenTable(ClusterStateMachine.SubscriptionStateSchema, ClusterStateMachine.SubscriptionState);
            var bigEndBatchId = Bits.SwapBytes(BatchId ?? 0);
            using var _ = Slice.External(context.Allocator, (byte*)&bigEndBatchId, sizeof(long), out var batchIdSlice);
            subscriptionStateTable.DeleteForwardFrom(ClusterStateMachine.SubscriptionStateSchema.Indexes[ClusterStateMachine.SubscriptionStateByBatchIdSlice], batchIdSlice, 
                false, long.MaxValue, shouldAbort: tvh =>
            {
                var recordBatchId = Bits.SwapBytes(*(long*)tvh.Reader.Read((int)ClusterStateMachine.SubscriptionStateTable.BatchId, out var size));
                return recordBatchId != BatchId;
            });

            if (DocumentsToResend == null)
                return;

            foreach (var r in DocumentsToResend)
            {
                using (SubscriptionConnectionsState.GetDatabaseAndSubscriptionAndDocumentKey(context, DatabaseName, SubscriptionId, r.DocumentId, out var key))
                using (subscriptionStateTable.Allocate(out var tvb))
                {
                    using var __ = Slice.External(context.Allocator, key, out var keySlice);
                    using var ___ = Slice.From(context.Allocator, r.ChangeVector, out var changeVectorSlice);

                    tvb.Add(keySlice);
                    tvb.Add(changeVectorSlice);
                    tvb.Add(SwappedNonExistentBatch); // batch id

                    subscriptionStateTable.Set(tvb);
                }
            }
        }

        private static readonly long SwappedNonExistentBatch = Bits.SwapBytes(SubscriptionConnection.NonExistentBatch);

        public static Func<string> GetLastResponsibleNode(
            bool hasHighlyAvailableTasks,
            DatabaseTopology topology,
            string nodeTag)
        {
            return () =>
            {
                if (hasHighlyAvailableTasks)
                    return null;

                if (topology.Members.Contains(nodeTag) == false)
                    return null;

                return nodeTag;
            };
        }

        public override void FillJson(DynamicJsonValue json)
        {
            json[nameof(ChangeVector)] = ChangeVector;
            json[nameof(SubscriptionId)] = SubscriptionId;
            json[nameof(SubscriptionName)] = SubscriptionName;
            json[nameof(NodeTag)] = NodeTag;
            json[nameof(HasHighlyAvailableTasks)] = HasHighlyAvailableTasks;
            json[nameof(LastTimeServerMadeProgressWithDocuments)] = LastTimeServerMadeProgressWithDocuments;
            json[nameof(LastKnownSubscriptionChangeVector)] = LastKnownSubscriptionChangeVector;
            json[nameof(BatchId)] = BatchId;
            if (DocumentsToResend != null)
                json[nameof(DocumentsToResend)] = new DynamicJsonArray(DocumentsToResend);
        }

        public override string AdditionalDebugInformation(Exception exception)
        {
            var msg = $"Got 'Ack' for id={SubscriptionId}, name={SubscriptionName}, CV={ChangeVector}, Tag={NodeTag}, lastProgressTime={LastTimeServerMadeProgressWithDocuments}" +
                $"lastKnownCV={LastKnownSubscriptionChangeVector}, HasHighlyAvailableTasks={HasHighlyAvailableTasks}.";
            if (exception != null)
            {
                msg += $" Exception = {exception}.";
            }

            return msg;
        }
    }
}
