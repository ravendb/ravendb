using System;
using System.Threading;
using System.Threading.Tasks;
using Raven.Server.ServerWide.Commands.Sharding;
using Raven.Server.ServerWide.Context;
using Sparrow.Logging;
using Sparrow.Utils;

namespace Raven.Server.Documents.Sharding.Background
{
    public sealed class ShardedDocumentsMigrator
    {
        private readonly ShardedDocumentDatabase _database;
        private readonly CancellationToken _token;
        private readonly Logger _logger;

        public ShardedDocumentsMigrator(ShardedDocumentDatabase database)
        {
            _database = database;
            _token = database.ShardedDocumentsStorage.DocumentDatabase.DatabaseShutdown;
            _logger = LoggingSource.Instance.GetLogger(database.Name, GetType().FullName);
        }

        internal async Task ExecuteMoveDocumentsAsync()
        {
            try
            {
                var configuration = _database.ShardingConfiguration;
                if (configuration.HasActiveMigrations())
                    return;

                int bucket = -1;
                int moveToShard = -1;
                bool found = false;
                using (_database.ShardedDocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenReadTransaction())
                {
                    for (var index = 0; index < configuration.BucketRanges.Count; index++)
                    {
                        _token.ThrowIfCancellationRequested();

                        var range = configuration.BucketRanges[index];
                        if (range.ShardNumber == _database.ShardNumber)
                            continue;

                        var start = range.BucketRangeStart;
                        var end = index == configuration.BucketRanges.Count - 1
                            ? int.MaxValue
                            : configuration.BucketRanges[index + 1].BucketRangeStart - 1;

                        var bucketStatistics = ShardedDocumentsStorage.GetBucketStatistics(context, start, end);

                        foreach (var bucketStats in bucketStatistics)
                        {
                            _token.ThrowIfCancellationRequested();

                            if (bucketStats.Size == 0)
                                continue;

                            bucket = bucketStats.Bucket;

                            // this bucket already been migrated
                            if (configuration.BucketMigrations.ContainsKey(bucket))
                                continue;

                            if (bucketStats.NumberOfDocuments == 0)
                            {
                                var foundTombstone = false;
                                foreach (var tombstone in _database.ShardedDocumentsStorage.RetrieveTombstonesByBucketFrom(context, bucket, 0))
                                {
                                    DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Shiran, DevelopmentHelper.Severity.Normal, "handle DeletedTimeSeriesRange?");

                                    if (tombstone.Flags.Contain(DocumentFlags.Artificial))
                                        continue;

                                    foundTombstone = true;
                                }

                                if (foundTombstone == false)
                                    continue;
                            }

                            moveToShard = range.ShardNumber;
                            found = true;
                            break;
                        }

                        if (found)
                            break;
                    }
                }

                if (found)
                    await MoveDocumentsToShardAsync(bucket, moveToShard);
            }
            catch (Exception e)
            {
                if (_token.IsCancellationRequested)
                    return;

                if (_logger.IsOperationsEnabled)
                    _logger.Operations($"Failed to execute documents migration for '{_database.Name}'", e);

                throw;
            }
        }

        private async Task MoveDocumentsToShardAsync(int bucket, int moveToShard)
        {
            var cmd = new StartBucketMigrationCommand(bucket, _database.ShardNumber, moveToShard, _database.ShardedDatabaseName,
                $"{Guid.NewGuid()}/{bucket}");

            await _database.ServerStore.SendToLeaderAsync(cmd);
        }
    }
}
