using System.Threading.Tasks;
using Raven.Client.Util;
using Raven.Server.ServerWide.Commands;

namespace Raven.Server.ServerWide
{
    public class ShardingStore
    {
        private readonly ServerStore _server;

        public ShardingStore(ServerStore server)
        {
            _server = server;
        }

        
        public Task<(long Index, object Result)> StartBucketMigration(string database, int bucket, int fromShard, int toShard)
        {
            var cmd = new StartBucketMigrationCommand(database, RaftIdGenerator.NewId())
            {
                Bucket = bucket,
                SourceShard = fromShard,
                DestinationShard = toShard
            };

            return _server.SendToLeaderAsync(cmd);
        }

        public Task<(long Index, object Result)> SourceMigrationCompleted(string database, int bucket, long migrationIndex, string lastChangeVector)
        {
            var cmd = new SourceMigrationSendCompletedCommand(database, RaftIdGenerator.NewId())
            {
                Bucket = bucket,
                MigrationIndex = migrationIndex,
                LastSentChangeVector = lastChangeVector
            };

            return _server.SendToLeaderAsync(cmd);
        }

        public Task<(long Index, object Result)> DestinationMigrationConfirm(string database, int bucket, long migrationIndex)
        {
            var cmd = new DestinationMigrationConfirmCommand(database, RaftIdGenerator.NewId())
            {
                Bucket = bucket,
                MigrationIndex = migrationIndex,
                Node = _server.NodeTag
            };

            return _server.SendToLeaderAsync(cmd);
        }

        public Task<(long Index, object Result)> SourceMigrationCleanup(string database, int bucket, long migrationIndex)
        {
            var cmd = new SourceMigrationCleanupCommand(database, RaftIdGenerator.NewId())
            {
                Bucket = bucket,
                MigrationIndex = migrationIndex,
                Node = _server.NodeTag
            };

            return _server.SendToLeaderAsync(cmd);
        }
    }
}
