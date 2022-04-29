using System.Net.Http;
using System.Threading.Tasks;
using Raven.Server.Documents.Sharding.Handlers.Processors.Documents;
using Raven.Server.Routing;

namespace Raven.Server.Documents.Sharding.Handlers
{
    public class ShardedDocumentHandler : ShardedDatabaseRequestHandler
    {
        [RavenShardedAction("/databases/*/docs", "HEAD")]
        public async Task Head()
        {
            using (var processor = new ShardedDocumentHandlerProcessorForHead(this))
            {
                await processor.ExecuteAsync();
            }
        }

        [RavenShardedAction("/databases/*/docs/size", "GET")]
        public async Task GetDocSize()
        {
            using (var processor = new ShardedDocumentHandlerProcessorForGetDocSize(this))
            {
                await processor.ExecuteAsync();
            }
        }

        [RavenShardedAction("/databases/*/docs", "GET")]
        public async Task Get()
        {
            using (var processor = new ShardedDocumentHandlerProcessorForGet(HttpMethod.Get, this))
            {
                await processor.ExecuteAsync();
            }
        }

        [RavenShardedAction("/databases/*/docs", "POST")]
        public async Task PostGet()
        {
            using (var processor = new ShardedDocumentHandlerProcessorForGet(HttpMethod.Post, this))
            {
                await processor.ExecuteAsync();
            }
        }

        [RavenShardedAction("/databases/*/docs", "DELETE")]
        public async Task Delete()
        {
            using (var processor = new ShardedDocumentHandlerProcessorForDelete(this))
            {
                await processor.ExecuteAsync();
            }
        }

        [RavenShardedAction("/databases/*/docs", "PUT")]
        public async Task Put()
        {
            using (var processor = new ShardedDocumentHandlerProcessorForPut(this))
            {
                await processor.ExecuteAsync();
            }
        }

        [RavenShardedAction("/databases/*/docs", "PATCH")]
        public async Task Patch()
        {
            using (var processor = new ShardedDocumentHandlerProcessorForPatch(this))
            {
                await processor.ExecuteAsync();
            }
        }

        [RavenShardedAction("/databases/*/docs/class", "GET")]
        public async Task GenerateClassFromDocument()
        {
            using (var processor = new ShardedDocumentHandlerProcessorForGenerateClassFromDocument(this))
            {
                await processor.ExecuteAsync();
            }
        }
    }
}
