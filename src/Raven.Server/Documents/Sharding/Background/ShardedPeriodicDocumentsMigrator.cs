using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Raven.Server.Background;
using Raven.Server.ServerWide.Commands.Sharding;
using Raven.Server.ServerWide.Context;
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
                            var configuration = _database.ShardingConfiguration;
                            for (var index = 0; index < configuration.BucketRanges.Count; index++)
                            {
                                var range = configuration.BucketRanges[index];
                                if (range.ShardNumber == _database.ShardNumber)
                                    continue;

                                var start = range.BucketRangeStart;
                                var end = index == configuration.BucketRanges.Count - 1
                                    ? int.MaxValue
                                    : configuration.BucketRanges[index + 1].BucketRangeStart;

                                var bucketStatistics = ShardedDocumentsStorage.GetBucketStatistics(context, start, end);

                                if (bucketStatistics == null)
                                    continue;

                                foreach (var bucketStats in bucketStatistics)
                                    buckets.Add(bucketStats.Bucket, range.ShardNumber);
                            }
                        }

                        if (buckets.Count > 0)
                        {
                            await MoveDocumentsToShard(buckets);
                            return;
                        }
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
               
                while (_database.ServerStore.Sharding.HasActiveMigrations(_database.ShardedDatabaseName))
                    await WaitOrThrowOperationCanceled(TimeSpan.FromMilliseconds(300));
            }
        }
    }
}
