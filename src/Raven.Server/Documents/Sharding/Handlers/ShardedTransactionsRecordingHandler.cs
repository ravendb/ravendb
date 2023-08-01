using System.Threading.Tasks;
using Raven.Server.Documents.Sharding.Handlers.Processors;
using Raven.Server.Routing;

namespace Raven.Server.Documents.Sharding.Handlers
{
    public sealed class ShardedTransactionsRecordingHandler : ShardedDatabaseRequestHandler
    {
        [RavenShardedAction("/databases/*/transactions/replay", "POST")]
        public async Task ReplayRecording()
        {
            using (var processor = new NotSupportedInShardingProcessor(this, $"Database '{DatabaseName}' is a sharded database and does not support Transactions replay recording operation."))
                await processor.ExecuteAsync();
        }

        [RavenShardedAction("/databases/*/admin/transactions/start-recording", "POST")]
        public async Task StartRecording()
        {
            using (var processor = new NotSupportedInShardingProcessor(this, $"Database '{DatabaseName}' is a sharded database and does not support Transactions start recording operation."))
                await processor.ExecuteAsync();
        }

        [RavenShardedAction("/databases/*/admin/transactions/stop-recording", "POST")]
        public async Task StopRecording()
        {
            using (var processor = new NotSupportedInShardingProcessor(this, $"Database '{DatabaseName}' is a sharded database and does not support Transactions stop recording operation."))
                await processor.ExecuteAsync();
        }
    }
}
