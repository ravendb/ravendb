using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Abstractions.Data;
using Raven.Abstractions.Util;
using Raven.Client.Connection.Async;
using Raven.Client.Extensions;
using Raven.Client.Shard;

namespace Raven.Client.Document
{
    public class ShardedBulkInsertOperation : IDisposable
    {
        private readonly GenerateEntityIdOnTheClient generateEntityIdOnTheClient;
        private readonly ShardedDocumentStore shardedDocumentStore;
        private readonly IDictionary<string, IDocumentStore> shards;
        private readonly BulkInsertOptions options;
        private readonly IShardResolutionStrategy shardResolutionStrategy;
        private readonly ShardStrategy shardStrategy;
        private readonly string database;

        //Key - ShardID, Value - BulkInsertOperation
        private IDictionary<string, BulkInsertOperation> Bulks { get; set; }
        private IAsyncDatabaseCommands DatabaseCommands { get; set; }

        public ShardedBulkInsertOperation(string database, ShardedDocumentStore shardedDocumentStore, BulkInsertOptions options)
        {
            this.database = database;
            this.shardedDocumentStore = shardedDocumentStore;
            this.options = options;
            shards = shardedDocumentStore.ShardStrategy.Shards;
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
            var shard = shards[shardId];
            BulkInsertOperation bulkInsertOperation;
            if (Bulks.TryGetValue(shardId, out bulkInsertOperation) == false)
            {
                var actualDatabaseName = database ?? ((dynamic)shard).DefaultDatabase ?? MultiDatabase.GetDatabaseName(shard.Url);
                bulkInsertOperation = new BulkInsertOperation(actualDatabaseName, shard, shard.Listeners, options, shard.Changes());
                Bulks.Add(shardId, bulkInsertOperation);
            }

            DatabaseCommands = string.IsNullOrWhiteSpace(database)
                ? shard.AsyncDatabaseCommands
                : shard.AsyncDatabaseCommands.ForDatabase(database);

            string id;
            if (generateEntityIdOnTheClient.TryGetIdFromInstance(entity, out id) == false)
            {
                id = generateEntityIdOnTheClient.GetOrGenerateDocumentKey(entity);
            }
            var modifyDocumentId = shardStrategy.ModifyDocumentId(shardedDocumentStore.Conventions, shardId, id);
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

