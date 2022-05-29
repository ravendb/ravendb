using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Session.Operations;
using Raven.Client.Http;
using Raven.Server.Documents.Sharding.Handlers.ContinuationTokens;
using Raven.Server.Documents.Sharding.Handlers.Processors.Collections;
using Raven.Server.Documents.Sharding.Operations;
using Raven.Server.Documents.Sharding.Streaming;
using Raven.Server.Json;
using Raven.Server.Routing;
using Sparrow.Json;
using Sparrow.Utils;

namespace Raven.Server.Documents.Sharding.Handlers
{
    public class ShardedCollectionHandler : ShardedDatabaseRequestHandler
    {
        [RavenShardedAction("/databases/*/collections/stats", "GET")]
        public async Task GetCollectionStats()
        {
            using (var processor = new ShardedCollectionsHandlerProcessorForGetCollectionStats(this, detailed: false))
                await processor.ExecuteAsync();
        }

        [RavenShardedAction("/databases/*/collections/stats/detailed", "GET")]
        public async Task GetDetailedCollectionStats()
        {
            using (var processor = new ShardedCollectionsHandlerProcessorForGetCollectionStats(this, detailed: true))
                await processor.ExecuteAsync();
        }

        [RavenShardedAction("/databases/*/collections/docs", "GET")]
        public async Task GetCollectionDocuments()
        {
            using (Server.ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            {
                var token = ContinuationTokens.GetOrCreateContinuationToken(context);

                DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Arek, DevelopmentHelper.Severity.Normal,
                    "See `null` passed as etag to above new ShardedCollectionPreviewOperation()");

                var op = new ShardedStreamDocumentsOperation(HttpContext, null, token);
                var results = await ShardExecutor.ExecuteParallelForAllAsync(op);
                using var streams = await results.Result.InitializeAsync(DatabaseContext, HttpContext.RequestAborted);

                long numberOfResults;
                long totalDocumentsSizeInBytes;
                using (var cancelToken = CreateOperationToken())
                await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    var documents = GetDocuments(streams, token);
                    writer.WriteStartObject();
                    writer.WritePropertyName(nameof(CollectionResult.Results));
                    (numberOfResults, totalDocumentsSizeInBytes) = await writer.WriteDocumentsAsync(context, documents, metadataOnly: false, cancelToken.Token);
                    writer.WriteComma();
                    writer.WriteContinuationToken(context, token);
                    writer.WriteEndObject();
                }

                //AddPagingPerformanceHint(PagingOperationType.Documents, "Collection", HttpContext.Request.QueryString.Value, numberOfResults, pageSize, sw.ElapsedMilliseconds, totalDocumentsSizeInBytes);
                DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Karmel, DevelopmentHelper.Severity.Normal, "Handle sharded AddPagingPerformanceHint");
            }
        }

        public async IAsyncEnumerable<Document> GetDocuments(CombinedReadContinuationState documents, ShardedPagingContinuation pagingContinuation)
        {
            await foreach (var result in DatabaseContext.Streaming.PagedShardedDocumentsByLastModified(documents, nameof(CollectionResult.Results), pagingContinuation))
            {
                yield return result.Item;
            }
        }
        
        public class CollectionResult
        {
            public List<object> Results;
            public string ContinuationToken;
        }

        public readonly struct ShardedStreamDocumentsOperation : IShardedStreamableOperation
        {
            private readonly string _startsWith;
            private readonly string _matches;
            private readonly string _exclude;
            private readonly string _startAfter;
            private readonly HttpContext _httpContext;
            private readonly ShardedPagingContinuation _token;

            public ShardedStreamDocumentsOperation(HttpContext httpContext, string etag, ShardedPagingContinuation token)
            {
                _startsWith = null;
                _matches = null;
                _exclude = null;
                _startAfter = null;
                _httpContext = httpContext;
                _token = token;
                ExpectedEtag = etag;
            }

            public ShardedStreamDocumentsOperation(HttpContext httpContext, string startsWith, string matches, string exclude, string startAfter, string etag, ShardedPagingContinuation token)
            {
                _httpContext = httpContext;
                _startsWith = startsWith;
                _matches = matches;
                _exclude = exclude;
                _startAfter = startAfter;
                _token = token;
                ExpectedEtag = etag;
            }

            public HttpRequest HttpRequest => _httpContext.Request;

            public RavenCommand<StreamResult> CreateCommandForShard(int shardNumber) =>
                StreamOperation.CreateStreamCommand(_startsWith, _matches, _token.Pages[shardNumber].Start, _token.PageSize, _exclude, _startAfter);

            public string ExpectedEtag { get; }

            public CombinedStreamResult CombineResults(Memory<StreamResult> results)
            {
                return new CombinedStreamResult {Results = results};
            }
        }
    }
}
