using System;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Client.Changes;
using System.Linq;
using Raven.Client.Data;

namespace Raven.Client.Shard
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

        public IObservableWithTask<IndexChange> ForIndex(string indexName)
        {
            var observableWithTasks = shardedDatabaseChanges.Select(x => x.ForIndex(indexName)).ToArray();
            return new ShardedObservableWithTask<IndexChange>(observableWithTasks);
        }

        public IObservableWithTask<DocumentChange> ForDocument(string docId)
        {
            var observableWithTasks = shardedDatabaseChanges.Select(x => x.ForDocument(docId)).ToArray();
            return new ShardedObservableWithTask<DocumentChange>(observableWithTasks);
        }

        public IObservableWithTask<DocumentChange> ForAllDocuments()
        {
            var observableWithTasks = shardedDatabaseChanges.Select(x => x.ForAllDocuments()).ToArray();
            return new ShardedObservableWithTask<DocumentChange>(observableWithTasks);
        }

        public IObservableWithTask<IndexChange> ForAllIndexes()
        {
            var observableWithTasks = shardedDatabaseChanges.Select(x => x.ForAllIndexes()).ToArray();
            return new ShardedObservableWithTask<IndexChange>(observableWithTasks);
        }

        public IObservableWithTask<TransformerChange> ForAllTransformers()
        {
            var observableWithTasks = shardedDatabaseChanges.Select(x => x.ForAllTransformers()).ToArray();
            return new ShardedObservableWithTask<TransformerChange>(observableWithTasks);
        }

        public IObservableWithTask<DocumentChange> ForDocumentsStartingWith(string docIdPrefix)
        {
            var observableWithTasks = shardedDatabaseChanges.Select(x => x.ForDocumentsStartingWith(docIdPrefix)).ToArray();
            return new ShardedObservableWithTask<DocumentChange>(observableWithTasks);
        }

        public IObservableWithTask<DocumentChange> ForDocumentsInCollection(string collectionName)
        {
            var observableWithTasks = shardedDatabaseChanges.Select(x => x.ForDocumentsInCollection(collectionName)).ToArray();
            return new ShardedObservableWithTask<DocumentChange>(observableWithTasks);
        }

        public IObservableWithTask<DocumentChange> ForDocumentsInCollection<TEntity>()
        {
            var observableWithTasks = shardedDatabaseChanges.Select(x => x.ForDocumentsInCollection<TEntity>()).ToArray();
            return new ShardedObservableWithTask<DocumentChange>(observableWithTasks);
        }

        public IObservableWithTask<DocumentChange> ForDocumentsOfType(string typeName)
        {
            var observableWithTasks = shardedDatabaseChanges.Select(x => x.ForDocumentsOfType(typeName)).ToArray();
            return new ShardedObservableWithTask<DocumentChange>(observableWithTasks);
        }

        public IObservableWithTask<DocumentChange> ForDocumentsOfType(Type type)
        {
            var observableWithTasks = shardedDatabaseChanges.Select(x => x.ForDocumentsOfType(type)).ToArray();
            return new ShardedObservableWithTask<DocumentChange>(observableWithTasks);
        }

        public IObservableWithTask<DocumentChange> ForDocumentsOfType<TEntity>()
        {
            var observableWithTasks = shardedDatabaseChanges.Select(x => x.ForDocumentsOfType<TEntity>()).ToArray();
            return new ShardedObservableWithTask<DocumentChange>(observableWithTasks);
        }

        public IObservableWithTask<ReplicationConflictChange> ForAllReplicationConflicts()
        {
            var observableWithTasks = shardedDatabaseChanges.Select(x => x.ForAllReplicationConflicts()).ToArray();
            return new ShardedObservableWithTask<ReplicationConflictChange>(observableWithTasks);
        }

        public IObservableWithTask<BulkInsertChange> ForBulkInsert(Guid? operationId = null)
        {
            var observableWithTasks = shardedDatabaseChanges.Select(x => x.ForBulkInsert(operationId)).ToArray();
            return new ShardedObservableWithTask<BulkInsertChange>(observableWithTasks);
        }

        public IObservableWithTask<DataSubscriptionChange> ForAllDataSubscriptions()
        {
            var observableWithTasks = shardedDatabaseChanges.Select(x => x.ForAllDataSubscriptions()).ToArray();
            return new ShardedObservableWithTask<DataSubscriptionChange>(observableWithTasks);
        }

        public IObservableWithTask<DataSubscriptionChange> ForDataSubscription(long id)
        {
            var observableWithTasks = shardedDatabaseChanges.Select(x => x.ForDataSubscription(id)).ToArray();
            return new ShardedObservableWithTask<DataSubscriptionChange>(observableWithTasks);
        }

        public IObservableWithTask<OperationStatusChanged> ForOperationId(long id)
        {
            var observableWithTasks = shardedDatabaseChanges.Select(x => x.ForOperationId(id)).ToArray();
            return new ShardedObservableWithTask<OperationStatusChanged>(observableWithTasks);
        }

        public IObservableWithTask<OperationStatusChanged> ForAllOperations()
        {
            var observableWithTasks = shardedDatabaseChanges.Select(x => x.ForAllOperations()).ToArray();
            return new ShardedObservableWithTask<OperationStatusChanged>(observableWithTasks);
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
