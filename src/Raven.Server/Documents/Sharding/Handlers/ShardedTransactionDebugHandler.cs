using System.Threading.Tasks;
using Raven.Server.Documents.Sharding.Handlers.Processors;
using Raven.Server.Documents.Sharding.Handlers.Processors.Debugging;
using Raven.Server.Routing;

namespace Raven.Server.Documents.Sharding.Handlers
{
    public sealed class ShardedTransactionDebugHandler : ShardedDatabaseRequestHandler
    {
        [RavenShardedAction("/databases/*/admin/debug/txinfo", "GET")]
        public async Task TxInfo()
        {
            using (var processor = new NotSupportedInShardingProcessor(this, $"Database '{DatabaseName}' is a sharded database and does not support Admin debug operations."))
                await processor.ExecuteAsync();
        }

        [RavenShardedAction("/databases/*/admin/debug/cluster/txinfo", "GET")]
        public async Task ClusterTxInfo()
        {
            using (var processor = new ShardedTransactionDebugHandlerProcessorForGetClusterInfo(this))
                await processor.ExecuteAsync();
        }
    }
}
