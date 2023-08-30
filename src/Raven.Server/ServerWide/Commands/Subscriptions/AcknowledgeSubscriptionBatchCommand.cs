using System;
using System.Collections.Generic;
using Raven.Client;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.Exceptions.Database;
using Raven.Client.Exceptions.Documents.Subscriptions;
using Raven.Client.ServerWide;
using Raven.Server.Documents.Subscriptions;
using Raven.Server.Rachis;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Binary;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Voron;
using Voron.Data.Tables;

namespace Raven.Server.ServerWide.Commands.Subscriptions
{
    public sealed class AcknowledgeSubscriptionBatchCommand : UpdateValueForDatabaseCommand
    {
        public string ChangeVector;

        // in regular subscription LastKnownSubscriptionChangeVector used only for backward compatibility
        // in sharing subscription LastKnownSubscriptionChangeVector used to save orchestrator cv
        public string LastKnownSubscriptionChangeVector;
        public long SubscriptionId;
        public string SubscriptionName;
        public string NodeTag;
        public bool HasHighlyAvailableTasks;
        public DateTime LastTimeServerMadeProgressWithDocuments;

        public long? BatchId;
        public List<DocumentRecord> DocumentsToResend; // documents that were updated while this batch was processing 

        public string ShardName;

        // for serialization
        private AcknowledgeSubscriptionBatchCommand() { }

        public AcknowledgeSubscriptionBatchCommand(string databaseName, string uniqueRequestId) : base(databaseName, uniqueRequestId)
        {
        }

        public override string GetItemId() => SubscriptionState.GenerateSubscriptionItemKeyName(DatabaseName, SubscriptionName);

        protected override UpdatedValue GetUpdatedValue(long index, RawDatabaseRecord record, ClusterOperationContext context, BlittableJsonReaderObject existingValue)
        {
            var subscriptionName = SubscriptionName;
            if (string.IsNullOrEmpty(subscriptionName))
            {
                subscriptionName = SubscriptionId.ToString();
            }

            if (existingValue == null)
                throw new SubscriptionDoesNotExistException($"Subscription with name '{subscriptionName}' does not exist");

            var subscription = JsonDeserializationCluster.SubscriptionState(existingValue);
            AssertSubscriptionState(record, subscription, subscriptionName);

            if (ChangeVector == nameof(Constants.Documents.SubscriptionChangeVectorSpecialStates.DoNotChange))
            {
                return new UpdatedValue(UpdatedValueActionType.Noop, value: null);
            }

            if (IsLegacyCommand())
            {
                if (LastKnownSubscriptionChangeVector != subscription.ChangeVectorForNextBatchStartingPoint)
                    throw new SubscriptionChangeVectorUpdateConcurrencyException($"Can't acknowledge subscription with name {subscriptionName} due to inconsistency in change vector progress. Probably there was an admin intervention that changed the change vector value. Stored value: {subscription.ChangeVectorForNextBatchStartingPoint}, received value: {LastKnownSubscriptionChangeVector}");

                subscription.ChangeVectorForNextBatchStartingPoint = ChangeVector;
            }

            if (string.IsNullOrEmpty(ShardName))
            {
                subscription.NodeTag = NodeTag;
            }
            else
            {
                var changeVector = context.GetChangeVector(ChangeVector);
                subscription.ShardingState.NodeTagPerShard[ShardName] = NodeTag;
                subscription.ChangeVectorForNextBatchStartingPoint =
                    ChangeVectorUtils.MergeVectors(changeVector.Order.StripMoveTag(context), subscription.ChangeVectorForNextBatchStartingPoint);

                if (string.IsNullOrEmpty(LastKnownSubscriptionChangeVector) == false)
                {
                    var orchestratorCv = context.GetChangeVector(LastKnownSubscriptionChangeVector);
                    subscription.ShardingState.ChangeVectorForNextBatchStartingPointForOrchestrator =
                        ChangeVectorUtils.MergeVectors(orchestratorCv.Order.StripMoveTag(context), subscription.ShardingState.ChangeVectorForNextBatchStartingPointForOrchestrator);
                }
            }

            subscription.LastBatchAckTime = LastTimeServerMadeProgressWithDocuments;

            return new UpdatedValue(UpdatedValueActionType.Update, context.ReadObject(subscription.ToJson(), subscriptionName));
        }

        private void AssertSubscriptionState(RawDatabaseRecord record, SubscriptionState subscription, string subscriptionName)
        {
            var appropriateNode = AbstractSubscriptionStorage.GetSubscriptionResponsibleNodeForProgress(record, ShardName, subscription, HasHighlyAvailableTasks);
            var deletionKey = DatabaseRecord.GetKeyForDeletionInProgress(NodeTag, ShardName);

            if (appropriateNode == null && record.DeletionInProgress.ContainsKey(deletionKey))
                throw new DatabaseDoesNotExistException($"Stopping subscription '{subscriptionName}' on node {NodeTag}, because database '{DatabaseName}' is being deleted.");

            if (appropriateNode != NodeTag)
            {
                throw new SubscriptionDoesNotBelongToNodeException(
                    $"Cannot apply {nameof(AcknowledgeSubscriptionBatchCommand)} for subscription '{subscriptionName}' with id '{SubscriptionId}', on database '{DatabaseName}', on node '{NodeTag}'," +
                    $" because the subscription task belongs to '{appropriateNode ?? "N/A"}'.")
                { AppropriateNode = appropriateNode };
            }
        }

        private bool IsLegacyCommand()
        {
            return BatchId == null || // from an old version CSM
                   BatchId == ISubscriptionConnection.NonExistentBatch; // from noop ack
        }

        public override void Execute(ClusterOperationContext context, Table items, long index, RawDatabaseRecord record, RachisState state, out object result)
        {
            base.Execute(context, items, index, record, state, out result);

            if (IsLegacyCommand())
                return;

            ExecuteAcknowledgeSubscriptionBatch(context, items, index);
        }

        private unsafe void ExecuteAcknowledgeSubscriptionBatch(ClusterOperationContext context, Table items, long index)
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
            var bigEndBatchId = Bits.SwapBytes(BatchId.Value);
            using var _ = Slice.External(context.Allocator, (byte*)&bigEndBatchId, sizeof(long), out var batchIdSlice);

            subscriptionStateTable.DeleteForwardFrom(ClusterStateMachine.SubscriptionStateSchema.Indexes[ClusterStateMachine.SubscriptionStateByBatchIdSlice],
                batchIdSlice,
                false, long.MaxValue, shouldAbort: tvh =>
                {
                    var recordBatchId = Bits.SwapBytes(*(long*)tvh.Reader.Read((int)ClusterStateMachine.SubscriptionStateTable.BatchId, out var size));
                    return recordBatchId != BatchId;
                });

            if (DocumentsToResend == null)
                return;

            foreach (var r in DocumentsToResend)
            {
                using (AbstractSubscriptionConnectionsState.GetDatabaseAndSubscriptionAndDocumentKey(context, DatabaseName, SubscriptionId, r.DocumentId, out var key))
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

        public static readonly long SwappedNonExistentBatch = Bits.SwapBytes(ISubscriptionConnection.NonExistentBatch);

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
            json[nameof(ShardName)] = ShardName;
            if (DocumentsToResend != null)
                json[nameof(DocumentsToResend)] = new DynamicJsonArray(DocumentsToResend);
        }

        public override string AdditionalDebugInformation(Exception exception)
        {
            var msg = $"Got 'Ack' for id={SubscriptionId}, name={SubscriptionName}, CV={ChangeVector}, Tag={NodeTag}, lastProgressTime={LastTimeServerMadeProgressWithDocuments}" +
                $"lastKnownCV={LastKnownSubscriptionChangeVector}, HasHighlyAvailableTasks={HasHighlyAvailableTasks}.";

            if (ShardName != null)
            {
                msg += $" for shard {ShardName}.";
            }

            if (exception != null)
            {
                msg += $" Exception = {exception}.";
            }

            return msg;
        }
    }
}
