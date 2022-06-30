using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents.Changes;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.Exceptions.Documents.Subscriptions;
using Raven.Client.Util;
using Raven.Server.Documents.Sharding;
using Raven.Server.Documents.TcpHandlers;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Commands.Subscriptions;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Binary;
using Voron;

namespace Raven.Server.Documents.Subscriptions
{
    public class SubscriptionConnectionsState : SubscriptionConnectionsStateBase<SubscriptionConnection>
    {
        private readonly SubscriptionStorage _subscriptionStorage;
        public DocumentDatabase DocumentDatabase => _subscriptionStorage._db;
        private IDisposable _disposableNotificationsRegistration;
        public string LastChangeVectorSent;
        public string PreviouslyRecordedChangeVector;

        public SubscriptionConnectionsState(string databaseName, long subscriptionId, SubscriptionStorage storage) : base(storage._db.ServerStore, databaseName, subscriptionId, storage._db.DatabaseShutdown)
        {
            _subscriptionStorage = storage;
        }

        public Task GetSubscriptionInUseAwaiter => Task.WhenAll(_connections.Select(c => c.SubscriptionConnectionTask));

        public override void DropSubscription(SubscriptionException e)
        {
            _subscriptionStorage.DropSubscriptionConnections(SubscriptionId, e);
        }
        
        public override async Task UpdateClientConnectionTime()
        {
            var command = GetUpdateSubscriptionClientConnectionTime();
            var (etag, _) = await _server.SendToLeaderAsync(command);
            await WaitForIndexNotificationAsync(etag);
        }

        public override Task WaitForIndexNotificationAsync(long index) => 
            DocumentDatabase.RachisLogIndexNotifications.WaitForIndexNotification(index, _server.Engine.OperationTimeout);

        public virtual long GetLastEtagSent() => ChangeVectorUtils.GetEtagById(LastChangeVectorSent, DocumentDatabase.DbBase64Id);

        public IDisposable RegisterForNotificationOnNewDocuments(SubscriptionConnection connection)
        {
            void RegisterNotification(DocumentChange notification)
            {
                if (Client.Constants.Documents.Collections.AllDocumentsCollection.Equals(connection.Subscription.Collection, StringComparison.OrdinalIgnoreCase) ||
                    notification.CollectionName.Equals(connection.Subscription.Collection, StringComparison.OrdinalIgnoreCase))
                {
                    NotifyHasMoreDocs();
                }
            }

            DocumentDatabase.Changes.OnDocumentChange += RegisterNotification;

            return new DisposableAction(() =>
            {
                DocumentDatabase.Changes.OnDocumentChange -= RegisterNotification;
            });
        }

        public override void Initialize(SubscriptionConnection connection, bool afterSubscribe = false)
        {
            base.Initialize(connection, afterSubscribe);

            // update the subscription data only on new concurrent connection or regular connection
            if (afterSubscribe && _connections.Count == 1)
            {
                // update the subscription data only on new concurrent connection or regular connection
                SetLastChangeVectorSent(connection);

                PreviouslyRecordedChangeVector = LastChangeVectorSent;
                
                using (var old = _disposableNotificationsRegistration)
                {
                    _disposableNotificationsRegistration = RegisterForNotificationOnNewDocuments(connection);
                }
            }
        }

        protected virtual void SetLastChangeVectorSent(SubscriptionConnection connection) => LastChangeVectorSent = connection.SubscriptionState.ChangeVectorForNextBatchStartingPoint;

        public HashSet<long> GetActiveBatches()
        {
            var set = new HashSet<long>();
            
            foreach (var connection in _connections)
            {
                var batch = connection.CurrentBatchId;
                if (batch == SubscriptionConnectionBase.NonExistentBatch)
                    continue;

                set.Add(batch);
            }

            return set;
        }

        public Task AcknowledgeBatch(SubscriptionConnection connection, long batchId, List<DocumentRecord> addDocumentsToResend)
        {
            return AcknowledgeBatchProcessed(
                connection.LastSentChangeVectorInThisConnection ?? nameof(Client.Constants.Documents.SubscriptionChangeVectorSpecialStates.DoNotChange),
                batchId,
                addDocumentsToResend);
        }
        
        public IEnumerable<RevisionRecord> GetRevisionsFromResend(ClusterOperationContext context, HashSet<long> activeBatches)
        {
            var subscriptionState = context.Transaction.InnerTransaction.OpenTable(ClusterStateMachine.SubscriptionStateSchema, ClusterStateMachine.SubscriptionState);
            using (GetDatabaseAndSubscriptionKeyPrefix(context, _databaseName, SubscriptionId, SubscriptionType.Revision, out var prefix))
            using (Slice.External(context.Allocator, prefix, out var prefixSlice))
            {
                foreach (var (_, tvh) in subscriptionState.SeekByPrimaryKeyPrefix(prefixSlice, Slices.Empty, 0))
                {
                    var batchId = Bits.SwapBytes(tvh.Reader.ReadLong((int)ClusterStateMachine.SubscriptionStateTable.BatchId));
                    if (activeBatches.Contains(batchId))
                        continue;

                    string current = tvh.Reader.ReadStringWithPrefix((int)ClusterStateMachine.SubscriptionStateTable.Key, prefix.Length);
                    string previous = tvh.Reader.ReadString((int)ClusterStateMachine.SubscriptionStateTable.ChangeVector);
                        
                    yield return new RevisionRecord
                    {
                        Current = current,
                        Previous = previous
                    };
                }
            }
        }

        public bool IsDocumentInActiveBatch(ClusterOperationContext context, string documentId, HashSet<long> activeBatches)
        {
            var subscriptionState = context.Transaction.InnerTransaction.OpenTable(ClusterStateMachine.SubscriptionStateSchema, ClusterStateMachine.SubscriptionState);
            using (GetDatabaseAndSubscriptionAndDocumentKey(context, _databaseName, SubscriptionId, documentId, out var key))
            using (Slice.External(context.Allocator,key, out var keySlice))
            {
                if (subscriptionState.ReadByKey(keySlice, out var reader) == false)
                    return false;

                var batchId = Bits.SwapBytes(reader.ReadLong((int)ClusterStateMachine.SubscriptionStateTable.BatchId));
                return activeBatches.Contains(batchId);
            }
        }
            
        public bool IsRevisionInActiveBatch(ClusterOperationContext context, string current, HashSet<long> activeBatches)
        {
            var subscriptionState = context.Transaction.InnerTransaction.OpenTable(ClusterStateMachine.SubscriptionStateSchema, ClusterStateMachine.SubscriptionState);
            using (GetDatabaseAndSubscriptionAndRevisionKey(context, _databaseName, SubscriptionId, current, out var key))
            using (Slice.External(context.Allocator,key, out var keySlice))
            {
                if (subscriptionState.ReadByKey(keySlice, out var reader) == false)
                    return false;

                var batchId = Bits.SwapBytes(reader.ReadLong((int)ClusterStateMachine.SubscriptionStateTable.BatchId));
                return activeBatches.Contains(batchId);
            }
        }

        protected virtual AcknowledgeSubscriptionBatchCommand GetAcknowledgeSubscriptionBatchCommand(string changeVector, long? batchId, List<DocumentRecord> docsToResend)
        {
            return new AcknowledgeSubscriptionBatchCommand(_databaseName, RaftIdGenerator.NewId())
            {
                ChangeVector = changeVector,
                NodeTag = _server.NodeTag,
                HasHighlyAvailableTasks = _server.LicenseManager.HasHighlyAvailableTasks(),
                SubscriptionId = SubscriptionId,
                SubscriptionName = SubscriptionName,
                LastTimeServerMadeProgressWithDocuments = DateTime.UtcNow,
                BatchId = batchId,
                DocumentsToResend = docsToResend,
            };
        }

        public async Task AcknowledgeBatchProcessed(string changeVector, long? batchId, List<DocumentRecord> docsToResend)
        {
            var command = GetAcknowledgeSubscriptionBatchCommand(changeVector, batchId, docsToResend);
                
            var (etag, _) = await _server.SendToLeaderAsync(command);
            await WaitForIndexNotificationAsync(etag);
        }


        public async Task<long> RecordBatchRevisions(List<RevisionRecord> list, string lastRecordedChangeVector)
        {
            var command = new RecordBatchSubscriptionDocumentsCommand(
                _databaseName, 
                SubscriptionId, 
                SubscriptionName, 
                list, 
                PreviouslyRecordedChangeVector, 
                lastRecordedChangeVector, 
                _server.NodeTag, 
                _server.LicenseManager.HasHighlyAvailableTasks(), 
                RaftIdGenerator.NewId());

            return await RecordBatchInternal(command);
        }

        public async Task<long> RecordBatchDocuments(List<DocumentRecord> list, List<string> deleted, string lastRecordedChangeVector)
        {
            var command = new RecordBatchSubscriptionDocumentsCommand(
                _databaseName, 
                SubscriptionId, 
                SubscriptionName, 
                list, 
                PreviouslyRecordedChangeVector, 
                lastRecordedChangeVector, 
                _server.NodeTag, 
                _server.LicenseManager.HasHighlyAvailableTasks(), 
                RaftIdGenerator.NewId());

            command.Deleted = deleted;
            return await RecordBatchInternal(command);
        }

        protected virtual async Task<long> RecordBatchInternal(RecordBatchSubscriptionDocumentsCommand command)
        {
            var (etag, _) = await _server.SendToLeaderAsync(command);
            await WaitForIndexNotificationAsync(etag);
            return etag;
        }
    
    

        public override void Dispose()
        {
            base.Dispose();
            
            _disposableNotificationsRegistration?.Dispose();
        }

        public static SubscriptionConnectionsState CreateDummyState(DocumentsStorage storage, SubscriptionState state)
        {
            if (storage.DocumentDatabase is ShardedDocumentDatabase sharded)
                return new DummySubscriptionConnectionsState(sharded.ShardedDatabaseName, storage, state);

            return new DummySubscriptionConnectionsState(storage.DocumentDatabase.Name, storage, state);
        }
    }
}
