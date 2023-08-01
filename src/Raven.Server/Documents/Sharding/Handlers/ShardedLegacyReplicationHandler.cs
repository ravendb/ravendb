using System.Threading.Tasks;
using Raven.Server.Documents.Sharding.Handlers.Processors;
using Raven.Server.Routing;

namespace Raven.Server.Documents.Sharding.Handlers
{
    public sealed class ShardedLegacyReplicationHandler : ShardedDatabaseRequestHandler
    {
        [RavenShardedAction("/databases/*/replication/lastEtag", "GET")]
        public async Task LastEtag()
        {
            using (var processor = new NotSupportedInShardingProcessor(this, $"Database '{DatabaseName}' is a sharded database and does not support LegacyReplication."))
                await processor.ExecuteAsync();
        }

        [RavenShardedAction("/databases/*/replication/replicateDocs", "POST")]
        public async Task Documents()
        {
            using (var processor = new NotSupportedInShardingProcessor(this, $"Database '{DatabaseName}' is a sharded database and does not support LegacyReplication."))
                await processor.ExecuteAsync();
        }

        [RavenShardedAction("/databases/*/replication/replicateAttachments", "POST")]
        public async Task Attachments()
        {
            using (var processor = new NotSupportedInShardingProcessor(this, $"Database '{DatabaseName}' is a sharded database and does not support LegacyReplication."))
                await processor.ExecuteAsync();
        }

        [RavenShardedAction("/databases/*/replication/heartbeat", "POST")]
        public async Task Heartbeat()
        {
            using (var processor = new NotSupportedInShardingProcessor(this, $"Database '{DatabaseName}' is a sharded database and does not support LegacyReplication."))
                await processor.ExecuteAsync();
        }

        [RavenShardedAction("/databases/*/indexes/last-queried", "POST")]
        public async Task LastQueried()
        {
            using (var processor = new NotSupportedInShardingProcessor(this, $"Database '{DatabaseName}' is a sharded database and does not support LegacyReplication."))
                await processor.ExecuteAsync();
        }

        [RavenShardedAction("/databases/*/side-by-side-indexes", "PUT")]
        public async Task SideBySideIndexes()
        {
            using (var processor = new NotSupportedInShardingProcessor(this, $"Database '{DatabaseName}' is a sharded database and does not support LegacyReplication."))
                await processor.ExecuteAsync();
        }

        [RavenShardedAction("/databases/*/transformers/$", "PUT")]
        public async Task PutTransformer()
        {
            using (var processor = new NotSupportedInShardingProcessor(this, $"Database '{DatabaseName}' is a sharded database and does not support LegacyReplication."))
                await processor.ExecuteAsync();
        }

        [RavenShardedAction("/databases/*/transformers/$", "DELETE")]
        public async Task DeleteTransformer()
        {
            using (var processor = new NotSupportedInShardingProcessor(this, $"Database '{DatabaseName}' is a sharded database and does not support LegacyReplication."))
                await processor.ExecuteAsync();
        }
    }
}
