using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Abstractions.Util;
using Raven.Client.Changes;
using Raven.Client.Connection.Async;
using Raven.Client.Extensions;
using Raven.Client.Shard;

namespace Raven.Client.Document
{
	public class ShardedBulkInsertOperation :  IDisposable
	{
		private readonly GenerateEntityIdOnTheClient generateEntityIdOnTheClient;
		private readonly ShardedDocumentStore shardedDocumentStore;
		public IAsyncDatabaseCommands DatabaseCommands { get; private set; }
		public IDictionary<string, IDocumentStore> Shards;
		private string database;
		private readonly BulkInsertOptions options;
		private readonly IShardResolutionStrategy shardResolutionStrategy;
		private readonly ShardStrategy shardStrategy;
		//key - shardID, Value - bulk
		private IDictionary<string, BulkInsertOperation> Bulks { get; set; }

		public ShardedBulkInsertOperation(string database, ShardedDocumentStore shardedDocumentStore, BulkInsertOptions options)
		{

			this.database = database;
			this.shardedDocumentStore = shardedDocumentStore;
			this.options = options;
			Shards = shardedDocumentStore.ShardStrategy.Shards;
			Bulks = new Dictionary<string, BulkInsertOperation>();
			generateEntityIdOnTheClient = new GenerateEntityIdOnTheClient(shardedDocumentStore.Conventions,
				entity => AsyncHelpers.RunSync(() => shardedDocumentStore.Conventions.GenerateDocumentKeyAsync(database, DatabaseCommands, entity)));
			shardResolutionStrategy = shardedDocumentStore.ShardStrategy.ShardResolutionStrategy;
			shardStrategy = this.shardedDocumentStore.ShardStrategy;
		}
		public bool IsAborted
		{
			get { return Bulks.Select(bulk => bulk.Value).Any(bulkOperation => bulkOperation.IsAborted); }
		}

		public void Abort()
		{
			foreach (var bulkOperation in Bulks.Select(bulk => bulk.Value))
			{
				bulkOperation.Abort();
			}
		}

		public void Store(object entity)
		{
			var shardId = shardResolutionStrategy.GenerateShardIdFor(entity, this);
			DatabaseCommands = Shards[shardId].AsyncDatabaseCommands;
			 
			database = MultiDatabase.GetDatabaseName(Shards[shardId].Url);
			string id;

			if (generateEntityIdOnTheClient.TryGetIdFromInstance(entity, out id) == false)
			{
				id = generateEntityIdOnTheClient.GetOrGenerateDocumentKey(entity);
				
			}

			var modifyDocumentId = shardStrategy.ModifyDocumentId(shardedDocumentStore.Conventions, shardId, id);
	
			BulkInsertOperation bulkInsertOperation;
			if (Bulks.TryGetValue(shardId, out bulkInsertOperation) == false)
			{
				var shard = Shards[shardId];
				bulkInsertOperation = new BulkInsertOperation(database, shard, shard.Listeners, options, shard.Changes());
				Bulks.Add(shardId, bulkInsertOperation);
			}

			bulkInsertOperation.Store(entity, modifyDocumentId);
		}

		void IDisposable.Dispose()
		{
			foreach (var bulkOperation in Bulks.Select(bulk => bulk.Value))
			{
				bulkOperation.Dispose();
			}
		}
	}
}

