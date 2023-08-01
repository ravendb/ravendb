using System.Threading.Tasks;
using Raven.Server.Documents.Sharding.Handlers.Processors;
using Raven.Server.Routing;

namespace Raven.Server.Documents.Sharding.Handlers.Admin
{
    public sealed class ShardedTransactionsModeHandler : ShardedDatabaseRequestHandler
    {
        [RavenShardedAction("/databases/*/admin/transactions-mode", "GET")]
        public async Task CommitNonLazyTx()
        {
            using (var processor = new NotSupportedInShardingProcessor(this, $"Database '{DatabaseName}' is a sharded database and does not support Admin transactions-mode operation."))
                await processor.ExecuteAsync();
        }
    }
}
