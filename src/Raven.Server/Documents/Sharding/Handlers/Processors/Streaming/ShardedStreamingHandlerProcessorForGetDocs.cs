using System.Collections.Generic;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.Documents.Handlers.Processors.Streaming;
using Raven.Server.Documents.Sharding.Handlers.ContinuationTokens;
using Raven.Server.Documents.Sharding.Handlers.Processors.Collections;
using Raven.Server.Documents.Sharding.Streaming;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.Streaming
{
    internal class ShardedStreamingHandlerProcessorForGetDocs : AbstractStreamingHandlerProcessorForGetDocs<ShardedDatabaseRequestHandler, TransactionOperationContext>
    {
        public ShardedStreamingHandlerProcessorForGetDocs([NotNull] ShardedDatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected override async ValueTask GetDocumentsAndWriteAsync(TransactionOperationContext context, int start, int pageSize, string startsWith,
            string excludes, string matches, string startAfter)
        {
            using (context.OpenReadTransaction())
            {
                var continuationToken =
                    RequestHandler.ContinuationTokens.GetOrCreateContinuationToken(context, start, pageSize);

                var op = new ShardedStreamDocumentsOperation(HttpContext, startsWith, matches, excludes, startAfter, null, continuationToken);
                var results = await RequestHandler.ShardExecutor.ExecuteParallelForAllAsync(op);
                using var streams = await results.Result.InitializeAsync(RequestHandler.DatabaseContext, HttpContext.RequestAborted);

                IAsyncEnumerable<BlittableJsonReaderObject> documents = string.IsNullOrEmpty(startsWith) == false ? 
                    OrderDocumentsById(streams, continuationToken) : 
                    OrderDocumentsByLastModified(streams, continuationToken);

                await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
                {
                    writer.WriteStartObject();

                    await writer.WriteArrayAsync("Results", documents);
                    
                    writer.WriteEndObject();
                }
            }
        }
        
        public async IAsyncEnumerable<BlittableJsonReaderObject> OrderDocumentsByLastModified(CombinedReadContinuationState documents, ShardedPagingContinuation pagingContinuation)
        {
            await foreach (var result in RequestHandler.DatabaseContext.Streaming.PagedShardedDocumentsBlittableByLastModified(documents, nameof(CollectionResult.Results), pagingContinuation))
            {
                yield return result.Item;
            }
        }

        public async IAsyncEnumerable<BlittableJsonReaderObject> OrderDocumentsById(CombinedReadContinuationState documents, ShardedPagingContinuation pagingContinuation)
        {
            await foreach (var result in RequestHandler.DatabaseContext.Streaming.PagedShardedDocumentsBlittableById(documents, nameof(CollectionResult.Results), pagingContinuation))
            {
                yield return result.Item;
            }
        }
    }
}
