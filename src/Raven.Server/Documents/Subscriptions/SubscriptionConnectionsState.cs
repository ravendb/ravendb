using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client;
using Raven.Client.Documents.Changes;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.Exceptions.Database;
using Raven.Client.Exceptions.Documents.Subscriptions;
using Raven.Client.ServerWide;
using Raven.Client.Util;
using Raven.Server.Documents.Includes;
using Raven.Server.Documents.Sharding;
using Raven.Server.Documents.TcpHandlers;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Commands.Subscriptions;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Binary;
using Voron;
using ConnectionStatus = Raven.Server.Documents.Subscriptions.ISubscriptionConnection.ConnectionStatus;

namespace Raven.Server.Documents.Subscriptions
{
    public class SubscriptionConnectionsState : AbstractSubscriptionConnectionsState<SubscriptionConnection, DatabaseIncludesCommandImpl>
    {
        private readonly SubscriptionStorage _subscriptionStorage;
        public DocumentDatabase DocumentDatabase => _subscriptionStorage._db;
        private IDisposable _disposableNotificationsRegistration;
        private readonly SemaphoreSlim _subscriptionConnectingLock = new SemaphoreSlim(1);

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

        public override async Task InitializeAsync(SubscriptionConnection connection, bool afterSubscribe = false)
        {
            await base.InitializeAsync(connection, afterSubscribe);

            if (afterSubscribe == false)
                return;

            // update the subscription data only on new concurrent connection or regular connection
            if (IsConcurrent == false)
            {
                RefreshFeatures(connection);
                return;
            }

            connection.AddToStatusDescription(connection.CreateStatusMessage(ConnectionStatus.Info, "Starting to subscribe."));

            if (_subscriptionState == null)
            {
                // this connection is the first one, we initialize everything
                RefreshFeatures(connection);
                return;
            }

            if (connection.SubscriptionState.RaftCommandIndex < _subscriptionState.RaftCommandIndex)
            {
                // this connection was modified while waiting to subscribe, lets try to drop it
                DropSingleConnection(connection, new SubscriptionClosedException($"The subscription '{_subscriptionName}' was modified, connection have to be restarted.", canReconnect: true));
                return;
            }

            if (connection.SubscriptionState.RaftCommandIndex == _subscriptionState.RaftCommandIndex)
            {
                // no changes in the subscription task 
                // the LastChangeVectorSent have to be refreshed since the concurrent subscription task might got processed on different node
                if (_connections.Count == 1)
                {
                    SetLastChangeVectorSent(connection);
                }

                return;
            }

            if (connection.SubscriptionState.RaftCommandIndex > _subscriptionState.RaftCommandIndex)
            {
                // we have new connection after subscription have changed
                // we have to wait until old connections (with smaller raft index) will get disconnected
                // then we continue and will re-initialize 

                var sp = Stopwatch.StartNew();
                while (_connections.Any(c => c.SubscriptionState.RaftCommandIndex == _subscriptionState.RaftCommandIndex))
                {
                    connection.CancellationTokenSource.Token.ThrowIfCancellationRequested();
                    await Task.Delay(300);
                    await connection.SendHeartBeatIfNeededAsync(sp, $"A connection from IP '{connection.ClientUri}' is waiting for old subscription connections to disconnect.");
                }

                RefreshFeatures(connection);
            }
        }

        public override void ReleaseConcurrentConnectionLock(SubscriptionConnection connection)
        {
            if (connection.Strategy != SubscriptionOpeningStrategy.Concurrent)
                return;

            _subscriptionConnectingLock.Release();
        }

        public override async Task TakeConcurrentConnectionLockAsync(SubscriptionConnection connection)
        {
            if (connection.Strategy != SubscriptionOpeningStrategy.Concurrent)
                return;

            DocumentDatabase.ForTestingPurposes?.ConcurrentSubscription_ActionToCallDuringWaitForSubscribe?.Invoke(_connections);
            while (await _subscriptionConnectingLock.WaitAsync(ISubscriptionConnection.WaitForChangedDocumentsTimeoutInMs) == false)
            {
                connection.CancellationTokenSource.Token.ThrowIfCancellationRequested();
                await connection.SendHeartBeatAsync($"A connection from IP '{connection.ClientUri}' is waiting for other concurrent connections to subscribe.");
            }
        }

        private void RefreshFeatures(SubscriptionConnection connection)
        {
            using (var old = _disposableNotificationsRegistration)
            {
                _disposableNotificationsRegistration = RegisterForNotificationOnNewDocuments(connection);
            }

            SetLastChangeVectorSent(connection);
            _subscriptionState = connection.SubscriptionState;
        }

        protected override void SetLastChangeVectorSent(SubscriptionConnection connection)
        {
            LastChangeVectorSent = connection.SubscriptionState.ChangeVectorForNextBatchStartingPoint;
        }

        public HashSet<long> GetActiveBatches()
        {
            var set = new HashSet<long>();
            
            foreach (var connection in _connections)
            {
                var batch = connection.CurrentBatchId;
                if (batch == ISubscriptionConnection.NonExistentBatch)
                    continue;

                set.Add(batch);
            }

            return set;
        }

        public async Task AcknowledgeBatchAsync(string changeVector, long batchId, List<DocumentRecord> addDocumentsToResend, Action<AcknowledgeSubscriptionBatchCommand> modifyCommand = null)
        {
            var command = GetAcknowledgeSubscriptionBatchCommand(changeVector, batchId, addDocumentsToResend);
            modifyCommand?.Invoke(command);

            var (etag, _) = await _server.SendToLeaderAsync(command);
            await WaitForIndexNotificationAsync(etag);
        }
        
        public IEnumerable<RevisionRecord> GetRevisionsFromResendInternal(ClusterOperationContext context, HashSet<long> activeBatches)
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

        public IEnumerable<(Document Previous, Document Current)> GetRevisionsFromResend(DocumentDatabase database, ClusterOperationContext clusterContext, DocumentsOperationContext docsContext, HashSet<long> activeBatches)
        {
            foreach (var r in GetRevisionsFromResendInternal(clusterContext, activeBatches))
            {
                yield return (
                    database.DocumentsStorage.RevisionsStorage.GetRevision(docsContext, r.Previous),
                    database.DocumentsStorage.RevisionsStorage.GetRevision(docsContext, r.Current)
                );
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
            
        public bool IsRevisionInActiveBatch(ClusterOperationContext context, Document current, HashSet<long> activeBatches)
        {
            var subscriptionState = context.Transaction.InnerTransaction.OpenTable(ClusterStateMachine.SubscriptionStateSchema, ClusterStateMachine.SubscriptionState);
            using (GetDatabaseAndSubscriptionAndRevisionKey(context, _databaseName, SubscriptionId, current.Id, current.ChangeVector, out var key))
            using (Slice.External(context.Allocator,key, out var keySlice))
            {
                if (subscriptionState.ReadByKey(keySlice, out var reader) == false)
                    return false;

                var batchId = Bits.SwapBytes(reader.ReadLong((int)ClusterStateMachine.SubscriptionStateTable.BatchId));
                return activeBatches.Contains(batchId);
            }
        }

        protected virtual AcknowledgeSubscriptionBatchCommand GetAcknowledgeSubscriptionBatchCommand(string changeVector, long batchId, List<DocumentRecord> docsToResend)
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

        private async Task NoopAcknowledgeSubscriptionAsync(bool force)
        {
            var command = GetAcknowledgeSubscriptionBatchCommand(nameof(Constants.Documents.SubscriptionChangeVectorSpecialStates.DoNotChange),
                ISubscriptionConnection.NonExistentBatch, docsToResend: null);

            var state = _server.Engine.CurrentCommittedState.State;
            if (state is RachisState.Leader or RachisState.Follower)
            {
                // there are no changes for this subscription but we still want to check if we are the node that is responsible for this task.
                // we can do that locally if we have a functional cluster (in a leader or follower state).

                using (_server.Engine.ContextPool.AllocateOperationContext(out ClusterOperationContext context))
                using (context.OpenReadTransaction())
                {
                    var rawDatabaseRecord = _server.Cluster.ReadRawDatabaseRecord(context, command.DatabaseName);
                    if (rawDatabaseRecord == null)
                        throw new DatabaseDoesNotExistException($"Cannot set command value of type {nameof(AcknowledgeSubscriptionBatchCommand)} for database {command.DatabaseName}, because it does not exist");

                    var subscriptionTask = _subscriptionStorage.GetSubscriptionById(context, SubscriptionId);

                    command.AssertSubscriptionState(rawDatabaseRecord, subscriptionTask, command.SubscriptionName);
                    return;
                }
            }

            if (force == false && _subscriptionStorage.ShouldWaitForClusterStabilization())
            {
                // in case of unstable cluster, we will try to heartbeat the client for 30 seconds and then send actual no-op command
                return;
            }

            var (etag, _) = await _server.SendToLeaderAsync(command);
            await WaitForIndexNotificationAsync(etag);
        }

        public async Task<(long Index, object Skipped)> RecordBatchRevisions(List<RevisionRecord> list, string lastRecordedChangeVector)
        {
            var command = new RecordBatchSubscriptionDocumentsCommand(
                _databaseName, 
                SubscriptionId, 
                SubscriptionName, 
                list, 
                LastChangeVectorSent, 
                lastRecordedChangeVector, 
                _server.NodeTag, 
                _server.LicenseManager.HasHighlyAvailableTasks(), 
                RaftIdGenerator.NewId());

            return await RecordBatchInternal(command);
        }

        public async Task<(long Index, object Skipped)> TryRecordBatchDocumentsAsync(List<DocumentRecord> list, List<string> deleted, string lastRecordedChangeVector)
        {
            if (list.Count == 0 && deleted.Count == 0 && lastRecordedChangeVector == null)
            {
                // nothing to record
                return await Task.FromResult<(long, object)>((ISubscriptionConnection.NonExistentBatch, null));
            }

            var command = new RecordBatchSubscriptionDocumentsCommand(
                _databaseName, 
                SubscriptionId, 
                SubscriptionName, 
                list, 
                LastChangeVectorSent, 
                lastRecordedChangeVector, 
                _server.NodeTag, 
                _server.LicenseManager.HasHighlyAvailableTasks(), 
                RaftIdGenerator.NewId());

            command.Deleted = deleted;
            return await RecordBatchInternal(command);
        }

        protected virtual async Task<(long Index, object Skipped)> RecordBatchInternal(RecordBatchSubscriptionDocumentsCommand command)
        {
            var result = await _server.SendToLeaderAsync(command);
            await WaitForIndexNotificationAsync(result.Index);
            return result;
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

        private long _lastNoopAckTicks;
        private Task _lastNoopAckTask = Task.CompletedTask;

        public Task SendNoopAck(bool force = false)
        {
            if (IsConcurrent)
            {
                var localLastNoopAckTicks = Interlocked.Read(ref _lastNoopAckTicks);
                var nowTicks = DateTime.Now.Ticks;
                if ((nowTicks - localLastNoopAckTicks) / TimeSpan.TicksPerMillisecond < ISubscriptionConnection.WaitForChangedDocumentsTimeoutInMs)
                {
                    return _lastNoopAckTask;
                }

                var currentLastNoopAckTicks = Interlocked.CompareExchange(ref _lastNoopAckTicks, nowTicks, localLastNoopAckTicks);
                if (currentLastNoopAckTicks != localLastNoopAckTicks)
                    return _lastNoopAckTask;

                var ackTask = NoopAcknowledgeSubscriptionAsync(force);

                Interlocked.Exchange(ref _lastNoopAckTask, ackTask);

                return ackTask;
            }

            return NoopAcknowledgeSubscriptionAsync(force);
        }
    }
}
