using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Threading.Tasks;
using Raven.Server.Documents.Handlers.Processors.Documents;
using Raven.Server.Documents.Sharding.Handlers.Processors.Documents;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Sharding.Handlers
{
    public sealed class ShardedDocumentHandler : ShardedDatabaseRequestHandler
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
        public Task Get()
        {
            // Disposal of the processor is done by having it auto-register to the request context disposal mechanism
            return new ShardedDocumentHandlerProcessorForGet(HttpMethod.Get, this).ExecuteAsync().AsTask();
        }

        [RavenShardedAction("/databases/*/docs", "POST")]
        public async Task PostGet()
        {
            TransactionOperationContext context = GetContextScopedToRequest();
            List<ReadOnlyMemory<char>> ids = await ShardedDocumentHandlerProcessorForGet.GetIdsFromRequestBodyAsync(context, this);
            
            // Disposal of the processor is done by having it auto-register to the request context disposal mechanism
            await new ShardedDocumentHandlerProcessorForGet(HttpMethod.Post, this, ids).ExecuteAsync();
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
