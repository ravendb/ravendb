using System.Threading.Tasks;
using Raven.Client.Util;
using Raven.Server.ServerWide.Commands.Sharding;

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
            var cmd = new StartBucketMigrationCommand(bucket, fromShard, toShard, database, RaftIdGenerator.NewId());

            return _server.SendToLeaderAsync(cmd);
        }

        public Task<(long Index, object Result)> SourceMigrationCompleted(string database, int bucket, long migrationIndex, string lastChangeVector)
        {
            var cmd = new SourceMigrationSendCompletedCommand(bucket, migrationIndex, lastChangeVector, database, RaftIdGenerator.NewId());

            return _server.SendToLeaderAsync(cmd);
        }

        public Task<(long Index, object Result)> DestinationMigrationConfirm(string database, int bucket, long migrationIndex)
        {
            var cmd = new DestinationMigrationConfirmCommand(bucket, migrationIndex, _server.NodeTag, database, RaftIdGenerator.NewId());

            return _server.SendToLeaderAsync(cmd);
        }

        public Task<(long Index, object Result)> SourceMigrationCleanup(string database, int bucket, long migrationIndex)
        {
            var cmd = new SourceMigrationCleanupCommand(bucket, migrationIndex, _server.NodeTag, database, RaftIdGenerator.NewId());

            return _server.SendToLeaderAsync(cmd);
        }
    }
}
