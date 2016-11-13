using System;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.NewClient.Client.Changes;
using System.Linq;
using Raven.NewClient.Client.Data;

namespace Raven.NewClient.Client.Shard
{
    public class ShardedDatabaseChanges : IDatabaseChanges
    {
        private readonly IDatabaseChanges[] shardedDatabaseChanges;

        public ShardedDatabaseChanges(IDatabaseChanges[] shardedDatabaseChanges)
        {
            this.shardedDatabaseChanges = shardedDatabaseChanges;
            ConnectionTask = System.Threading.Tasks.Task.Factory.ContinueWhenAll(shardedDatabaseChanges.Select(x => x.ConnectionTask).ToArray(), tasks =>
            {
                foreach (var task in tasks)
                {
                    task.AssertNotFailed();
                }
                return (IDatabaseChanges) this;
            });
        }

        public bool Connected { get; private set; }
        public event EventHandler ConnectionStatusChanged = delegate {};
        public Task<IDatabaseChanges> ConnectionTask { get; private set; }

        public IObservableWithTask<IndexChangeNotification> ForIndex(string indexName)
        {
            var observableWithTasks = shardedDatabaseChanges.Select(x => x.ForIndex(indexName)).ToArray();
            return new ShardedObservableWithTask<IndexChangeNotification>(observableWithTasks);
        }

        public IObservableWithTask<DocumentChangeNotification> ForDocument(string docId)
        {
            var observableWithTasks = shardedDatabaseChanges.Select(x => x.ForDocument(docId)).ToArray();
            return new ShardedObservableWithTask<DocumentChangeNotification>(observableWithTasks);
        }

        public IObservableWithTask<DocumentChangeNotification> ForAllDocuments()
        {
            var observableWithTasks = shardedDatabaseChanges.Select(x => x.ForAllDocuments()).ToArray();
            return new ShardedObservableWithTask<DocumentChangeNotification>(observableWithTasks);
        }

        public IObservableWithTask<IndexChangeNotification> ForAllIndexes()
        {
            var observableWithTasks = shardedDatabaseChanges.Select(x => x.ForAllIndexes()).ToArray();
            return new ShardedObservableWithTask<IndexChangeNotification>(observableWithTasks);
        }

        public IObservableWithTask<TransformerChangeNotification> ForAllTransformers()
        {
            var observableWithTasks = shardedDatabaseChanges.Select(x => x.ForAllTransformers()).ToArray();
            return new ShardedObservableWithTask<TransformerChangeNotification>(observableWithTasks);
        }

        public IObservableWithTask<DocumentChangeNotification> ForDocumentsStartingWith(string docIdPrefix)
        {
            var observableWithTasks = shardedDatabaseChanges.Select(x => x.ForDocumentsStartingWith(docIdPrefix)).ToArray();
            return new ShardedObservableWithTask<DocumentChangeNotification>(observableWithTasks);
        }

        public IObservableWithTask<DocumentChangeNotification> ForDocumentsInCollection(string collectionName)
        {
            var observableWithTasks = shardedDatabaseChanges.Select(x => x.ForDocumentsInCollection(collectionName)).ToArray();
            return new ShardedObservableWithTask<DocumentChangeNotification>(observableWithTasks);
        }

        public IObservableWithTask<DocumentChangeNotification> ForDocumentsInCollection<TEntity>()
        {
            var observableWithTasks = shardedDatabaseChanges.Select(x => x.ForDocumentsInCollection<TEntity>()).ToArray();
            return new ShardedObservableWithTask<DocumentChangeNotification>(observableWithTasks);
        }

        public IObservableWithTask<DocumentChangeNotification> ForDocumentsOfType(string typeName)
        {
            var observableWithTasks = shardedDatabaseChanges.Select(x => x.ForDocumentsOfType(typeName)).ToArray();
            return new ShardedObservableWithTask<DocumentChangeNotification>(observableWithTasks);
        }

        public IObservableWithTask<DocumentChangeNotification> ForDocumentsOfType(Type type)
        {
            var observableWithTasks = shardedDatabaseChanges.Select(x => x.ForDocumentsOfType(type)).ToArray();
            return new ShardedObservableWithTask<DocumentChangeNotification>(observableWithTasks);
        }

        public IObservableWithTask<DocumentChangeNotification> ForDocumentsOfType<TEntity>()
        {
            var observableWithTasks = shardedDatabaseChanges.Select(x => x.ForDocumentsOfType<TEntity>()).ToArray();
            return new ShardedObservableWithTask<DocumentChangeNotification>(observableWithTasks);
        }

        public IObservableWithTask<ReplicationConflictNotification> ForAllReplicationConflicts()
        {
            var observableWithTasks = shardedDatabaseChanges.Select(x => x.ForAllReplicationConflicts()).ToArray();
            return new ShardedObservableWithTask<ReplicationConflictNotification>(observableWithTasks);
        }

        public IObservableWithTask<BulkInsertChangeNotification> ForBulkInsert(Guid? operationId = null)
        {
            var observableWithTasks = shardedDatabaseChanges.Select(x => x.ForBulkInsert(operationId)).ToArray();
            return new ShardedObservableWithTask<BulkInsertChangeNotification>(observableWithTasks);
        }

        public IObservableWithTask<DataSubscriptionChangeNotification> ForAllDataSubscriptions()
        {
            var observableWithTasks = shardedDatabaseChanges.Select(x => x.ForAllDataSubscriptions()).ToArray();
            return new ShardedObservableWithTask<DataSubscriptionChangeNotification>(observableWithTasks);
        }

        public IObservableWithTask<DataSubscriptionChangeNotification> ForDataSubscription(long id)
        {
            var observableWithTasks = shardedDatabaseChanges.Select(x => x.ForDataSubscription(id)).ToArray();
            return new ShardedObservableWithTask<DataSubscriptionChangeNotification>(observableWithTasks);
        }

        public IObservableWithTask<OperationStatusChangeNotification> ForOperationId(long id)
        {
            var observableWithTasks = shardedDatabaseChanges.Select(x => x.ForOperationId(id)).ToArray();
            return new ShardedObservableWithTask<OperationStatusChangeNotification>(observableWithTasks);
        }

        public IObservableWithTask<OperationStatusChangeNotification> ForAllOperations()
        {
            var observableWithTasks = shardedDatabaseChanges.Select(x => x.ForAllOperations()).ToArray();
            return new ShardedObservableWithTask<OperationStatusChangeNotification>(observableWithTasks);
        }

        public void WaitForAllPendingSubscriptions()
        {
            foreach (var shardedDatabaseChange in shardedDatabaseChanges)
            {
                shardedDatabaseChange.WaitForAllPendingSubscriptions();
            }
        }
    }
}
