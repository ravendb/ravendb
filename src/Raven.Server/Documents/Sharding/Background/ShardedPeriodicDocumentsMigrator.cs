using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Raven.Server.Background;
using Raven.Server.ServerWide.Commands.Sharding;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Logging;

namespace Raven.Server.Documents.Sharding.Background
{
    public class ShardedPeriodicDocumentsMigrator : BackgroundWorkBase
    {
        private readonly ShardedDocumentDatabase _database;

        public ShardedPeriodicDocumentsMigrator(ShardedDocumentDatabase database) : base(database.ShardedDatabaseName, database.ShardedDocumentsStorage.DocumentDatabase.DatabaseShutdown)
        {
            _database = database;
        }

        protected override async Task DoWork()
        {
            await WaitOrThrowOperationCanceled(_database.Configuration.Sharding.PeriodicDocumentsMigrationInterval.AsTimeSpan);

            await ExecuteMoveDocuments();
        }

        internal async Task ExecuteMoveDocuments()
        {
            try
            {
                var etag = 0L;
                var buckets = new Dictionary<int, int>();

                using (_database.ShardedDocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                {
                    while (true)
                    {
                        buckets.Clear();
                        context.Reset();
                        context.Renew();

                        using (context.OpenReadTransaction())
                        {
                            var record = _database.ReadDatabaseRecord();
                            var documents = _database.ShardedDocumentsStorage.GetDocumentsFrom(context, etag, start: 0, take: 1024).ToList();

                            if (documents.Any() == false)
                                return;

                            foreach (var document in documents)
                            {
                                var (shardNumber, bucket) = ShardHelper.GetShardNumberAndBucketFor(record.Sharding, context.Allocator, document.Id);
                                if (shardNumber == _database.ShardNumber)
                                    continue;

                                if (buckets.ContainsKey(bucket) == false)
                                    buckets.Add(bucket, shardNumber);
                              
                                etag = document.Etag;
                            }
                        }

                        if (buckets.Count > 0)
                            await MoveDocumentsToShard(buckets);
                        
                        etag++;
                    }
                }
            }
            catch (OperationCanceledException)
            {
                // this will stop processing
                throw;
            }
            catch (Exception e)
            {
                if (Logger.IsOperationsEnabled)
                    Logger.Operations($"Failed to execute documents migration for '{_database.Name}'", e);

                throw;
            }
        }

        private async Task MoveDocumentsToShard(Dictionary<int, int> buckets)
        {
            var commands = new List<StartBucketMigrationCommand>();
            foreach (var (bucket, moveToShard) in buckets)
            {
                var cmd = new StartBucketMigrationCommand(bucket, _database.ShardNumber, moveToShard, _database.ShardedDatabaseName,
                    $"{Guid.NewGuid()}/{bucket}", backgroundMigration: true);
                commands.Add(cmd);
            }

            foreach (var cmd in commands)
            {
                var result = await _database.ServerStore.SendToLeaderAsync(cmd);
                await _database.ServerStore.Cluster.WaitForIndexNotification(result.Index);
            }
        }
    }
}
