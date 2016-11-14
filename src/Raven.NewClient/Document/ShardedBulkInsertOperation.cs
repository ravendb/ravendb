using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Raven.NewClient.Abstractions.Data;
using Raven.NewClient.Abstractions.Util;
using Raven.NewClient.Client.Connection.Async;
using Raven.NewClient.Client.Extensions;
using Raven.NewClient.Client.Shard;

namespace Raven.NewClient.Client.Document
{
    public class ShardedBulkInsertOperation : IDisposable
    {
        private readonly GenerateEntityIdOnTheClient generateEntityIdOnTheClient;
        private readonly ShardedDocumentStore shardedDocumentStore;
        private readonly IDictionary<string, IDocumentStore> shards;
        private readonly IShardResolutionStrategy shardResolutionStrategy;
        private readonly ShardStrategy shardStrategy;
        private readonly string database;

        //Key - ShardID, Value - BulkInsertOperation
        private IDictionary<string, BulkInsertOperation> Bulks { get; set; }
        private IAsyncDatabaseCommands DatabaseCommands { get; set; }

        public ShardedBulkInsertOperation(string database, ShardedDocumentStore shardedDocumentStore)
        {
            this.database = database;
            this.shardedDocumentStore = shardedDocumentStore;
            shards = shardedDocumentStore.ShardStrategy.Shards;
            Bulks = new Dictionary<string, BulkInsertOperation>();
            generateEntityIdOnTheClient = new GenerateEntityIdOnTheClient(shardedDocumentStore.Conventions,
                entity => AsyncHelpers.RunSync(() => shardedDocumentStore.Conventions.GenerateDocumentKeyAsync(database, DatabaseCommands, entity)));
            shardResolutionStrategy = shardedDocumentStore.ShardStrategy.ShardResolutionStrategy;
            shardStrategy = this.shardedDocumentStore.ShardStrategy;
        }

        public void Abort()
        {
            foreach (var bulkOperation in Bulks.Select(bulk => bulk.Value))
            {
                bulkOperation.Abort();
            }
        }

        public async Task StoreAsync(object entity)
        {
            var shardId = shardResolutionStrategy.GenerateShardIdFor(entity, this);
            var shard = shards[shardId];
            BulkInsertOperation bulkInsertOperation;
            if (Bulks.TryGetValue(shardId, out bulkInsertOperation) == false)
            {
                var actualDatabaseName = database ?? ((DocumentStore)shard).DefaultDatabase ?? MultiDatabase.GetDatabaseName(shard.Url);
                bulkInsertOperation = new BulkInsertOperation(actualDatabaseName, shard, shard.Listeners);
                Bulks.Add(shardId, bulkInsertOperation);
            }

            DatabaseCommands = shard.AsyncDatabaseCommands;
            string id;
            if (generateEntityIdOnTheClient.TryGetIdFromInstance(entity, out id) == false)
            {
                id = generateEntityIdOnTheClient.GetOrGenerateDocumentKey(entity);
            }
            var modifyDocumentId = shardStrategy.ModifyDocumentId(shardedDocumentStore.Conventions, shardId, id);
            await bulkInsertOperation.StoreAsync(entity, modifyDocumentId).ConfigureAwait(false);
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

