using System.Threading.Tasks;
using Raven.Server.Documents.Sharding.Handlers.Processors;
using Raven.Server.Routing;

namespace Raven.Server.Documents.Sharding.Handlers
{
    public sealed class ShardedAllDocumentIdsDebugHandler : ShardedDatabaseRequestHandler
    {
        [RavenShardedAction("/databases/*/debug/documents/export-all-ids", "GET")]
        public async Task ExportAllDocIds()
        {
            using (var processor = new NotSupportedInShardingProcessor(this, $"Database '{DatabaseName}' is a sharded database and does not support Documents debug operations."))
                await processor.ExecuteAsync();
        }
    }
}
