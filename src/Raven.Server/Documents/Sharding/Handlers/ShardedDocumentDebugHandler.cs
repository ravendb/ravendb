using System.Threading.Tasks;
using Raven.Server.Documents.Sharding.Handlers.Processors;
using Raven.Server.Routing;

namespace Raven.Server.Documents.Sharding.Handlers
{
    public sealed class ShardedDocumentDebugHandler : ShardedDatabaseRequestHandler
    {
        [RavenShardedAction("/databases/*/debug/documents/huge", "GET")]
        public async Task HugeDocuments()
        {
            using (var processor = new NotSupportedInShardingProcessor(this, $"Database '{DatabaseName}' is a sharded database and does not support Debug Information operations."))
                await processor.ExecuteAsync();
        }
    }
}
