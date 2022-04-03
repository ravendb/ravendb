using System.Threading.Tasks;
using Raven.Server.Documents.Sharding.Handlers.Processors.DocumentsCompression;
using Raven.Server.Routing;

namespace Raven.Server.Documents.Sharding.Handlers
{
    public class ShardedDocumentsCompressionHandler : ShardedDatabaseRequestHandler
    {
        [RavenShardedAction("/databases/*/documents-compression/config", "GET")]
        public async Task GetDocumentsCompressionConfig()
        {
            using (var processor = new ShardedDocumentsCompressionHandlerProcessorForGet(this))
                await processor.ExecuteAsync();
        }

        [RavenShardedAction("/databases/*/admin/documents-compression/config", "POST")]
        public async Task ConfigDocumentsCompression()
        {
            using (var processor = new ShardedDocumentsCompressionHandlerProcessorForPost(this))
                await processor.ExecuteAsync();
        }
    }
}
