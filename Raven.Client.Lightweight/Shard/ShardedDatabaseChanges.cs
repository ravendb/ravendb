using System;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Client.Changes;
using System.Linq;
using Raven.Client.Extensions;

namespace Raven.Client.Shard
{
	public class ShardedDatabaseChanges : IDatabaseChanges
	{
		private readonly IDatabaseChanges[] shardedDatabaseChanges;

		public ShardedDatabaseChanges(IDatabaseChanges[] shardedDatabaseChanges)
		{
			this.shardedDatabaseChanges = shardedDatabaseChanges;
			Task = System.Threading.Tasks.Task.Factory.ContinueWhenAll(shardedDatabaseChanges.Select(x => x.Task).ToArray(), tasks =>
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
		public Task<IDatabaseChanges> Task { get; private set; }

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

		public IObservableWithTask<DocumentChangeNotification> ForDocumentsStartingWith(string docIdPrefix)
		{
			var observableWithTasks = shardedDatabaseChanges.Select(x => x.ForDocumentsStartingWith(docIdPrefix)).ToArray();
			return new ShardedObservableWithTask<DocumentChangeNotification>(observableWithTasks);
		}

		public IObservableWithTask<ReplicationConflictNotification> ForAllReplicationConflicts()
		{
			var observableWithTasks = shardedDatabaseChanges.Select(x => x.ForAllReplicationConflicts()).ToArray();
			return new ShardedObservableWithTask<ReplicationConflictNotification>(observableWithTasks);
		}

		public IObservableWithTask<BulkInsertChangeNotification> ForBulkInsert(Guid operationId)
		{
			var observableWithTasks = shardedDatabaseChanges.Select(x => x.ForBulkInsert(operationId)).ToArray();
			return new ShardedObservableWithTask<BulkInsertChangeNotification>(observableWithTasks);
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