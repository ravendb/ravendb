using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Session.Operations;
using Raven.Client.Http;
using Raven.Server.Documents.Sharding.Handlers.ContinuationTokens;
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
            var op = new ShardedCollectionStatisticsOperation(HttpContext);

            var stats = await ShardExecutor.ExecuteParallelForAllAsync(op);

            using (ContextPool.AllocateOperationContext(out JsonOperationContext context))
            {
                await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
                    context.Write(writer, stats.ToJson());
            }
        }

        [RavenShardedAction("/databases/*/collections/stats/detailed", "GET")]
        public async Task GetDetailedCollectionStats()
        {
            var op = new ShardedDetailedCollectionStatisticsOperation(HttpContext);
            var stats = await ShardExecutor.ExecuteParallelForAllAsync(op);

            using (ContextPool.AllocateOperationContext(out JsonOperationContext context))
            {
                await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
                    context.Write(writer, stats.ToJson());
            }
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

            public RavenCommand<StreamResult> CreateCommandForShard(int shard) =>
                StreamOperation.CreateStreamCommand(_startsWith, _matches, _token.Pages[shard].Start, _token.PageSize, _exclude, _startAfter);

            public string ExpectedEtag { get; }

            public CombinedStreamResult CombineResults(Memory<StreamResult> results)
            {
                return new CombinedStreamResult {Results = results};
            }
        }

        internal readonly struct ShardedCollectionStatisticsOperation : IShardedOperation<CollectionStatistics>
        {
            private readonly HttpContext _httpContext;

            public ShardedCollectionStatisticsOperation(HttpContext httpContext)
            {
                _httpContext = httpContext;
            }

            public HttpRequest HttpRequest => _httpContext.Request;

            public CollectionStatistics Combine(Memory<CollectionStatistics> results)
            {
                var stats = new CollectionStatistics();
                var span = results.Span;
                for (int i = 0; i < span.Length; i++)
                {
                    var result = span[i];
                    stats.CountOfDocuments += result.CountOfDocuments;
                    stats.CountOfConflicts += result.CountOfConflicts;
                    foreach (var collectionInfo in result.Collections)
                    {
                        stats.Collections[collectionInfo.Key] = stats.Collections.ContainsKey(collectionInfo.Key)
                            ? stats.Collections[collectionInfo.Key] + collectionInfo.Value
                            : collectionInfo.Value;
                    }
                }

                return stats;
            }

            public RavenCommand<CollectionStatistics> CreateCommandForShard(int shard) => new GetCollectionStatisticsOperation.GetCollectionStatisticsCommand();
        }

        private readonly struct ShardedDetailedCollectionStatisticsOperation : IShardedOperation<DetailedCollectionStatistics>
        {
            private readonly HttpContext _httpContext;

            public ShardedDetailedCollectionStatisticsOperation(HttpContext httpContext)
            {
                _httpContext = httpContext;
            }

            public HttpRequest HttpRequest => _httpContext.Request;

            public DetailedCollectionStatistics Combine(Memory<DetailedCollectionStatistics> results)
            {
                var stats = new DetailedCollectionStatistics();
                var span = results.Span;
                for (int i = 0; i < span.Length; i++)
                {
                    var result = span[i];
                    stats.CountOfDocuments += result.CountOfDocuments;
                    stats.CountOfConflicts += result.CountOfConflicts;
                    foreach (var collectionInfo in result.Collections)
                    {
                        if (stats.Collections.ContainsKey(collectionInfo.Key))
                        {
                            stats.Collections[collectionInfo.Key].CountOfDocuments += collectionInfo.Value.CountOfDocuments;
                            stats.Collections[collectionInfo.Key].DocumentsSize.SizeInBytes += collectionInfo.Value.DocumentsSize.SizeInBytes;
                            stats.Collections[collectionInfo.Key].RevisionsSize.SizeInBytes += collectionInfo.Value.RevisionsSize.SizeInBytes;
                            stats.Collections[collectionInfo.Key].TombstonesSize.SizeInBytes += collectionInfo.Value.TombstonesSize.SizeInBytes;
                        }
                        else
                        {
                            stats.Collections[collectionInfo.Key] = collectionInfo.Value;
                        }
                    }
                }

                return stats;
            }

            public RavenCommand<DetailedCollectionStatistics> CreateCommandForShard(int shard) =>
                new GetDetailedCollectionStatisticsOperation.GetDetailedCollectionStatisticsCommand();
        }
    }
}
