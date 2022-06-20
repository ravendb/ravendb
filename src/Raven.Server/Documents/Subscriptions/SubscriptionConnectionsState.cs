using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Changes;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.Exceptions.Documents.Subscriptions;
using Raven.Client.Util;
using Raven.Server.Documents.Sharding;
using Raven.Server.Documents.TcpHandlers;
using Raven.Server.Utils;
using Sparrow.Server;

namespace Raven.Server.Documents.Subscriptions
{
    public class SubscriptionConnectionsState : SubscriptionConnectionsStateBase<SubscriptionConnection>
    {
        private readonly SubscriptionStorage _subscriptionStorage;
        public DocumentDatabase DocumentDatabase => _subscriptionStorage._db;
        private IDisposable _disposableNotificationsRegistration;
        private readonly AsyncManualResetEvent _waitForMoreDocuments;
        public string LastChangeVectorSent;

        public SubscriptionConnectionsState(string databaseName, long subscriptionId, SubscriptionStorage storage) : base(storage._db.ServerStore, databaseName, subscriptionId)
        {
            _subscriptionStorage = storage;
            CancellationTokenSource = CancellationTokenSource.CreateLinkedTokenSource(storage._db.DatabaseShutdown);
            _waitForMoreDocuments = new AsyncManualResetEvent(CancellationTokenSource.Token);
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
                    _waitForMoreDocuments.Set();
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

        public override void NotifyHasMoreDocs() => _waitForMoreDocuments.Set();

        public void NotifyNoMoreDocs() => _waitForMoreDocuments.Reset();

        public Task<bool> WaitForMoreDocs() => _waitForMoreDocuments.WaitAsync();

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
