using System;
using System.Threading;
using System.Threading.Tasks;
using Raven.Server.ServerWide.Commands.Sharding;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Sharding.Background
{
    public class ShardedDocumentsMigrator : IDisposable
    {
        private readonly ShardedDocumentDatabase _database;
        private readonly CancellationTokenSource _cts;
        private readonly Logger _logger;

        public ShardedDocumentsMigrator(ShardedDocumentDatabase database)
        {
            _database = database;
            _cts = CancellationTokenSource.CreateLinkedTokenSource(database.ShardedDocumentsStorage.DocumentDatabase.DatabaseShutdown);
            _logger = LoggingSource.Instance.GetLogger(database.Name, GetType().FullName);
        }

        internal async Task<bool> ExecuteMoveDocumentsAsync()
        {
            _cts.Token.ThrowIfCancellationRequested();

            if (_database.ServerStore.Sharding.HasActiveMigrations(_database.ShardedDatabaseName))
                return false;

            bool found = false;
            try
            {
                int bucket = -1;
                int moveToShard = -1;
                using (_database.ShardedDocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
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
                            : configuration.BucketRanges[index + 1].BucketRangeStart - 1;

                        var bucketStatistics = ShardedDocumentsStorage.GetBucketStatistics(context, start, end);

                        foreach (var bucketStats in bucketStatistics)
                        {
                            if (bucketStats.NumberOfDocuments == 0)
                                continue;

                            bucket = bucketStats.Bucket;
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

                return found;
            }
            catch (Exception e)
            {
                if (_cts.IsCancellationRequested)
                    return found;

                if (_logger.IsOperationsEnabled)
                    _logger.Operations($"Failed to execute documents migration for '{_database.Name}'", e);

                throw;
            }
        }

        private async ValueTask MoveDocumentsToShardAsync(int bucket, int moveToShard)
        {
            var cmd = new StartBucketMigrationCommand(bucket, _database.ShardNumber, moveToShard, _database.ShardedDatabaseName,
                $"{Guid.NewGuid()}/{bucket}");

            var result = await _database.ServerStore.SendToLeaderAsync(cmd);
            await _database.ServerStore.Cluster.WaitForIndexNotification(result.Index);
        }

        public void Dispose()
        {
            try
            {
                _cts.Dispose();
            }
            catch (ObjectDisposedException) //precaution, shouldn't happen
            {
                //don't care, we are disposing...
            }
        }
    }
}
