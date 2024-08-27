using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.ServerWide.Sharding;
using Raven.Server.ServerWide.Commands.Sharding;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
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

                        if (TryFindWrongBucket(context, configuration, bucketStatistics, out bucket))
                        {
                            moveToShard = range.ShardNumber;
                            break;
                        }
                    }
                }

                if (bucket != -1)
                    await MoveDocumentsToShardAsync(bucket, moveToShard, configuration);
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

        private bool TryFindWrongBucket(DocumentsOperationContext context, ShardingConfiguration configuration, IEnumerable<BucketStats> bucketStatistics, out int bucket)
        {
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
                    foreach (var tombstone in _database.ShardedDocumentsStorage.RetrieveTombstonesByBucketFrom(context, bucket, 0))
                    {
                        DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Shiran, DevelopmentHelper.Severity.Normal, "handle DeletedTimeSeriesRange?");

                        if (tombstone.Flags.Contain(DocumentFlags.Artificial) &&
                            tombstone.Flags.Contain(DocumentFlags.FromResharding))
                            continue;

                        return true;
                    }

                    continue;
                }

                return true;
            }

            bucket = -1;
            return false;
        }

        private async Task MoveDocumentsToShardAsync(int bucket, int moveToShard, ShardingConfiguration configuration)
        {
            string prefix = null;
            if (bucket >= ShardHelper.NumberOfBuckets)
            {
                // bucket belongs to a prefixed range
                // need to find the corresponding prefix setting in order to validate the destination shard

                foreach (var setting in configuration.Prefixed)
                {
                    var bucketRangeStart = setting.BucketRangeStart;
                    var nextRangeStart = bucketRangeStart + ShardHelper.NumberOfBuckets;

                    if (bucket < bucketRangeStart || bucket >= nextRangeStart)
                        continue;

                    prefix = setting.Prefix;
                    break;
                }

                if (string.IsNullOrEmpty(prefix))
                    throw new InvalidOperationException($"Bucket {bucket} should belong to a prefixed range, but a corresponding {nameof(PrefixedShardingSetting)} wasn't found in database record");
            }

            var cmd = new StartBucketMigrationCommand(bucket, 
                sourceShard: _database.ShardNumber, 
                destShard: moveToShard, 
                _database.ShardedDatabaseName, 
                prefix,
                raftId: $"{Guid.NewGuid()}/{bucket}");

            await _database.ServerStore.SendToLeaderAsync(cmd);
        }
    }
}
