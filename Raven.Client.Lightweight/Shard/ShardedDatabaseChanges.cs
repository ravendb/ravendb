using System;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
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
			Task = Task.Factory.ContinueWhenAll(shardedDatabaseChanges.Select(x => x.Task).ToArray(), tasks =>
			{
				foreach (var task in tasks)
				{
					task.AssertNotFailed();
				}
			});
		}

		public Task Task { get; private set; }

		public IObservableWithTask<IndexChangeNotification> IndexSubscription(string indexName)
		{
			var observableWithTasks = shardedDatabaseChanges.Select(x=>x.IndexSubscription(indexName)).ToArray();
			return new ShardedObservableWithTask<IndexChangeNotification>(observableWithTasks);
		}

		public IObservableWithTask<DocumentChangeNotification> DocumentSubscription(string docId)
		{
			var observableWithTasks = shardedDatabaseChanges.Select(x => x.DocumentSubscription(docId)).ToArray();
			return new ShardedObservableWithTask<DocumentChangeNotification>(observableWithTasks);
		}

		public IObservableWithTask<DocumentChangeNotification> DocumentPrefixSubscription(string docIdPrefix)
		{
			var observableWithTasks = shardedDatabaseChanges.Select(x => x.DocumentPrefixSubscription(docIdPrefix)).ToArray();
			return new ShardedObservableWithTask<DocumentChangeNotification>(observableWithTasks);
		}
	}
}