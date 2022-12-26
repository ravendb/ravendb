using System.Threading.Tasks;
using Raven.Server.Documents.Sharding.Handlers.Processors;
using Raven.Server.Routing;

namespace Raven.Server.Documents.Sharding.Handlers
{
    public class ShardedTransactionDebugHandler : ShardedDatabaseRequestHandler
    {
        [RavenShardedAction("/databases/*/admin/debug/txinfo", "GET")]
        public async Task TxInfo()
        {
            using (var processor = new NotSupportedInShardingProcessor(this, $"Database '{DatabaseName}' is a sharded database and does not support Admin debug operations."))
                await processor.ExecuteAsync();
        }
    }
}
