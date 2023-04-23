using System;
using System.Collections.Generic;
using System.Text;
using Raven.Client;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.Exceptions.Database;
using Raven.Client.Exceptions.Documents.Subscriptions;
using Raven.Client.Json.Serialization;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Sharding;
using Raven.Server.Documents;
using Raven.Server.Documents.Subscriptions;
using Raven.Server.Rachis;
using Raven.Server.ServerWide.Context;
using Raven.Server.ServerWide.Sharding;
using Raven.Server.Utils;
using Sparrow.Binary;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Utils;
using Voron;
using Voron.Data.Tables;

namespace Raven.Server.ServerWide.Commands.Subscriptions
{
    public class RecordBatchSubscriptionDocumentsCommand :  UpdateValueForDatabaseCommand
    {
        public long SubscriptionId;
        public string SubscriptionName;
        public string CurrentChangeVector;
        public string PreviouslyRecordedChangeVector;
        public string NodeTag;
        public bool HasHighlyAvailableTasks;
        public List<DocumentRecord> Documents;
        public List<string> Deleted;
        public List<RevisionRecord> Revisions;

        // for sharding
        public HashSet<long> ActiveBatchesFromSender;
        public string ShardName;

        public RecordBatchSubscriptionDocumentsCommand()
        {
        }

        public RecordBatchSubscriptionDocumentsCommand(string databaseName, long subscriptionId, string subscriptionName, List<DocumentRecord> documents, string previouslyRecordedChangeVector, string currentChangeVector, string nodeTag, bool hasHighlyAvailableTasks, string uniqueRaftId) : base(databaseName, uniqueRaftId)
        {
            SubscriptionId = subscriptionId;
            SubscriptionName = subscriptionName;
            Documents = documents;
            CurrentChangeVector = currentChangeVector;
            PreviouslyRecordedChangeVector = previouslyRecordedChangeVector;
            NodeTag = nodeTag;
            HasHighlyAvailableTasks = hasHighlyAvailableTasks;
        }

        public RecordBatchSubscriptionDocumentsCommand(string databaseName, long subscriptionId, string subscriptionName, List<RevisionRecord> revisions, string previouslyRecordedChangeVector, string currentChangeVector, string nodeTag, bool hasHighlyAvailableTasks, string uniqueRaftId) : base(databaseName, uniqueRaftId)
        {
            SubscriptionId = subscriptionId;
            SubscriptionName = subscriptionName;
            Revisions = revisions;
            CurrentChangeVector = currentChangeVector;
            PreviouslyRecordedChangeVector = previouslyRecordedChangeVector;
            NodeTag = nodeTag;
            HasHighlyAvailableTasks = hasHighlyAvailableTasks;
        }

        public override object FromRemote(object remoteResult)
        {
            var rc = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            var obj = remoteResult as BlittableJsonReaderArray;

            if (obj == null)
            {
                // this is an error as we expect BlittableJsonReaderArray, but we will pass the object value to get later appropriate exception
                return base.FromRemote(remoteResult);
            }

            foreach (var o in obj)
            {
                rc.Add(o.ToString());
            }
            return rc;
        }

        public override unsafe void Execute(ClusterOperationContext context, Table items, long index, RawDatabaseRecord record, RachisState state, out object result)
        {
            result = null;
            var shouldUpdateChangeVector = true;

            var subscriptionName = SubscriptionName;
            if (string.IsNullOrEmpty(subscriptionName))
            {
                subscriptionName = SubscriptionId.ToString();
            }

            //insert all docs to voron table. If exists, then batchId will be replaced
            var subscriptionStateTable = context.Transaction.InnerTransaction.OpenTable(ClusterStateMachine.SubscriptionStateSchema, ClusterStateMachine.SubscriptionState);
            
            var itemKey = SubscriptionState.GenerateSubscriptionItemKeyName(DatabaseName, subscriptionName);
            using (Slice.From(context.Allocator, itemKey.ToLowerInvariant(), out Slice valueNameLowered))
            using (Slice.From(context.Allocator, itemKey, out Slice valueName))
            {
                if (items.ReadByKey(valueNameLowered, out var tvr) == false)
                {
                    throw new RachisApplyException($"Cannot find subscription {subscriptionName} @ {DatabaseName}");
                }

                var ptr = tvr.Read(2, out int size);
                var existingValue = new BlittableJsonReaderObject(ptr, size, context);

                if (existingValue == null)
                    throw new SubscriptionDoesNotExistException($"Subscription with name '{subscriptionName}' does not exist in database '{DatabaseName}'");

                var subscriptionState = JsonDeserializationClient.SubscriptionState(existingValue);
                
                var appropriateNode = AbstractSubscriptionStorage.GetSubscriptionResponsibleNodeForProgress(record, ShardName, subscriptionState, HasHighlyAvailableTasks);
                var deletionKey = DatabaseRecord.GetKeyForDeletionInProgress(NodeTag, ShardName);

                if (appropriateNode == null && record.DeletionInProgress.ContainsKey(deletionKey))
                    throw new DatabaseDoesNotExistException(
                        $"Stopping subscription '{subscriptionName}' on node {NodeTag}, because database '{DatabaseName}' is being deleted.");

                if (appropriateNode != NodeTag)
                {
                    throw new SubscriptionDoesNotBelongToNodeException(
                        $"Cannot apply {nameof(AcknowledgeSubscriptionBatchCommand)} for subscription '{subscriptionName}' with id '{SubscriptionId}', on database '{DatabaseName}', on node '{NodeTag}'," +
                        $" because the subscription task belongs to '{appropriateNode ?? "N/A"}'.") { AppropriateNode = appropriateNode };
                }

                if (CurrentChangeVector == nameof(Constants.Documents.SubscriptionChangeVectorSpecialStates.DoNotChange))
                {
                    context.ReadObject(existingValue, subscriptionName);
                    shouldUpdateChangeVector = false;
                }

                CheckConcurrencyForBatchCv(subscriptionState, subscriptionName);

                foreach (var deletedId in Deleted)
                {
                    if (subscriptionState.ShardingState != null)
                    {
                        if (IsFromProperShard(context, record, deletedId) == false)
                            continue;
                    }

                    using (AbstractSubscriptionConnectionsState.GetDatabaseAndSubscriptionAndDocumentKey(context, DatabaseName, SubscriptionId, deletedId, out var key))
                    {
                        using var _ = Slice.External(context.Allocator, key, out var keySlice);
                        subscriptionStateTable.DeleteByKey(keySlice);
                    }
                }

                var skipped = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

                foreach (var documentRecord in Documents)
                {
                    using (AbstractSubscriptionConnectionsState.GetDatabaseAndSubscriptionAndDocumentKey(context, DatabaseName, SubscriptionId, documentRecord.DocumentId,
                               out var key))
                    {
                        using var _ = Slice.External(context.Allocator, key, out var keySlice);
                        using var __ = Slice.From(context.Allocator, documentRecord.ChangeVector, out var changeVectorSlice);

                        var batchId = index;
                        if (subscriptionState.ShardingState != null)
                        {
                            var exists = TryGetExisting(context, keySlice, out var currentBatch, out var currentChangeVector);
                            var owner = IsFromProperShard(context, record, documentRecord.DocumentId);

                            if (exists)
                            {
                                if (owner)
                                {
                                    if (ActiveBatchesFromSender.Contains(currentBatch))
                                    {
                                        // item has been picked up by someone else
                                        skipped.Add(documentRecord.DocumentId);
                                        continue;
                                    }
                                }
                                else
                                {
                                    // it is not mine, cannot send it
                                    skipped.Add(documentRecord.DocumentId);
                                    continue;
                                }
                            }
                            else
                            {
                                if (IsAlreadyProcessed(context, record.Sharding, subscriptionState.ShardingState, documentRecord.DocumentId,
                                        documentRecord.ChangeVector))
                                {
                                    // item was already processed
                                    skipped.Add(documentRecord.DocumentId);
                                    continue;
                                }
                            }

                            var vector = context.GetChangeVector(documentRecord.ChangeVector);
                            var bucket = ShardHelper.GetBucketFor(record.Sharding.MaterializedConfiguration, context.Allocator, documentRecord.DocumentId);

                            if (IsBucketUnderActiveMigration(record, bucket))
                            {
                                batchId = ISubscriptionConnection.NonExistentBatch;
                                skipped.Add(documentRecord.DocumentId);
                            } 
                            else if (owner == false)
                            {
                                batchId = ISubscriptionConnection.NonExistentBatch;
                                skipped.Add(documentRecord.DocumentId);
                            }

                            if (batchId > ISubscriptionConnection.NonExistentBatch)
                            {
                                if (subscriptionState.ShardingState.ProcessedChangeVectorPerBucket.TryGetValue(bucket, out var current))
                                {
                                    subscriptionState.ShardingState.ProcessedChangeVectorPerBucket[bucket] = ChangeVectorUtils.MergeVectors(current, vector.Version);
                                }
                            }
                        }

                        using (subscriptionStateTable.Allocate(out var tvb))
                        {
                            tvb.Add(keySlice);
                            tvb.Add(changeVectorSlice);
                            tvb.Add(Bits.SwapBytes(batchId)); // batch id

                            subscriptionStateTable.Set(tvb);
                        }
                    }
                }

                foreach (var revisionRecord in Revisions)
                {
                    using (AbstractSubscriptionConnectionsState.GetDatabaseAndSubscriptionAndRevisionKey(context, DatabaseName, SubscriptionId, revisionRecord.DocumentId, revisionRecord.Current,
                               out var key))
                    using (subscriptionStateTable.Allocate(out var tvb))
                    {
                        using var _ = Slice.External(context.Allocator, key, out var keySlice);
                        using var __ = Slice.From(context.Allocator, revisionRecord.Previous ?? string.Empty, out var changeVectorSlice);

                        tvb.Add(keySlice);
                        tvb.Add(changeVectorSlice); //prev change vector
                        tvb.Add(Bits.SwapBytes(index)); // batch id

                        subscriptionStateTable.Set(tvb);
                    }
                }

                if (shouldUpdateChangeVector)
                {
                    if (string.IsNullOrEmpty(ShardName))
                    {
                        subscriptionState.ChangeVectorForNextBatchStartingPoint =
                            ChangeVectorUtils.MergeVectors(CurrentChangeVector, subscriptionState.ChangeVectorForNextBatchStartingPoint);
                        subscriptionState.NodeTag = NodeTag;
                    }
                    else
                    {
                        var changeVector = context.GetChangeVector(CurrentChangeVector);
                        subscriptionState.ShardingState.ChangeVectorForNextBatchStartingPointPerShard.TryGetValue(ShardName, out string current);
                        subscriptionState.ShardingState.ChangeVectorForNextBatchStartingPointPerShard[ShardName] =
                            changeVector.Order.MergeWith(current, context);
                        subscriptionState.ShardingState.NodeTagPerShard[ShardName] = NodeTag;
                    }
                }

                using (var obj = context.ReadObject(subscriptionState.ToJson(), "subscription"))
                {
                    ClusterStateMachine.UpdateValue(index, items, valueNameLowered, valueName, obj);
                }

                result = skipped;
            }
        }

        private bool TryGetExisting(ClusterOperationContext context, Slice keySlice, out long batchId, out ChangeVector changeVector)
        {
            batchId = -2;
            changeVector = null;
            
            var subscriptionStateTable = context.Transaction.InnerTransaction.OpenTable(ClusterStateMachine.SubscriptionStateSchema, ClusterStateMachine.SubscriptionState);
            if (subscriptionStateTable.ReadByKey(keySlice, out var current))
            {
                
                    batchId = DocumentsStorage.TableValueToEtag((int)ClusterStateMachine.SubscriptionStateTable.BatchId, ref current);
                    var changeVectorAsString =
                        DocumentsStorage.TableValueToChangeVector(context, (int)ClusterStateMachine.SubscriptionStateTable.ChangeVector, ref current);
                    changeVector = context.GetChangeVector(changeVectorAsString);
                    return true;
            }

            return false;
        }

        private bool IsAlreadyProcessed(ClusterOperationContext context, RawShardingConfiguration configuration,
            SubscriptionShardingState subscriptionShardingState, string id,
            string changeVector)
        {
            var vector = context.GetChangeVector(changeVector);
            var bucket = ShardHelper.GetBucketFor(configuration.MaterializedConfiguration, context.Allocator, id);

            if (subscriptionShardingState.ProcessedChangeVectorPerBucket.TryGetValue(bucket, out var processedChangeVector))
            {
                var status = ChangeVectorUtils.GetConflictStatus(vector.Version, processedChangeVector);

                if (status == ConflictStatus.AlreadyMerged)
                {
                    return true;    
                }
            }

            return false;
        }

        private bool IsBucketUnderActiveMigration(RawDatabaseRecord record, int bucket)
        {
            if (record.Sharding.BucketMigrations.TryGetValue(bucket, out var migration) == false)
                return false;

            return migration.IsActive;
        }

        private bool IsFromProperShard(ClusterOperationContext context, RawDatabaseRecord record, string id)
        {
            var expected = ShardHelper.GetShardNumberFor(record.Sharding, context, id);
            var actual = ShardHelper.GetShardNumberFromDatabaseName(ShardName);

            return expected == actual;
        }

        private void CheckConcurrencyForBatchCv(SubscriptionState state, string subscriptionName)
        {
            if (string.IsNullOrEmpty(ShardName))
            {
                var subscriptionStateChangeVectorForNextBatchStartingPoint = state.ChangeVectorForNextBatchStartingPoint;
                if (subscriptionStateChangeVectorForNextBatchStartingPoint != PreviouslyRecordedChangeVector)
                {
                    throw new SubscriptionChangeVectorUpdateConcurrencyException($"Can't record subscription with name '{subscriptionName}' due to inconsistency in change vector progress. Probably there was an admin intervention that changed the change vector value. Stored value: {subscriptionStateChangeVectorForNextBatchStartingPoint}, received value: {PreviouslyRecordedChangeVector}.");
                }
                return;
            }

            state.ShardingState.ChangeVectorForNextBatchStartingPointPerShard.TryGetValue(ShardName, out string cvInStorage);
            if (cvInStorage != PreviouslyRecordedChangeVector)
            {
                throw new SubscriptionChangeVectorUpdateConcurrencyException($"Can't record sharded subscription with name '{subscriptionName}' due to inconsistency in change vector progress. Probably there was an admin intervention that changed the change vector value. Stored value: {cvInStorage}, received value: {PreviouslyRecordedChangeVector}.");
            }
        }

        public override string GetItemId()
        {
            throw new System.NotImplementedException();
        }

        public override void FillJson(DynamicJsonValue json)
        {
            json[nameof(SubscriptionId)] = SubscriptionId;
            json[nameof(SubscriptionName)] = SubscriptionName;
            json[nameof(CurrentChangeVector)] = CurrentChangeVector;
            json[nameof(PreviouslyRecordedChangeVector)] = PreviouslyRecordedChangeVector;
            json[nameof(NodeTag)] = NodeTag;
            json[nameof(HasHighlyAvailableTasks)] = HasHighlyAvailableTasks;
            json[nameof(ShardName)] = ShardName;

            if (ActiveBatchesFromSender != null)
                json[nameof(ActiveBatchesFromSender)] = new DynamicJsonArray(ActiveBatchesFromSender);
            if (Documents != null)
                json[nameof(Documents)] = new DynamicJsonArray(Documents);
            if(Revisions != null)
                json[nameof(Revisions)] = new DynamicJsonArray(Revisions);
            if(Deleted != null)
                json[nameof(Deleted)] = new DynamicJsonArray(Deleted);
        }

        protected override BlittableJsonReaderObject GetUpdatedValue(long index, RawDatabaseRecord record, ClusterOperationContext context,
            BlittableJsonReaderObject existingValue)
        {
            throw new System.NotImplementedException();
        }
    }

    public abstract class SubscriptionRecord
    {

    }

    public class DocumentRecord : SubscriptionRecord, IDynamicJsonValueConvertible
    {
        public string DocumentId;
        public string ChangeVector;

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue()
            {
                [nameof(DocumentId)] = DocumentId,
                [nameof(ChangeVector)] = ChangeVector
            };
        }
    }

    public class RevisionRecord : SubscriptionRecord, IDynamicJsonValueConvertible
    {
        public string DocumentId;
        public string Previous;
        public string Current;
        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue()
            {
                [nameof(DocumentId)] = DocumentId,
                [nameof(Previous)] = Previous,
                [nameof(Current)] = Current
            };
        }
    }

    public enum SubscriptionType : byte
    {
        None = 0,
        Document = 1,
        Revision = 2
    }
}
