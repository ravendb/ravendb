using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Session;
using Raven.Client.Documents.Session.Operations;
using Raven.Client.Extensions;
using Raven.Client.Http;
using Raven.Server.Documents.Replication.Senders;
using Raven.Server.Documents.ShardedHandlers.ContinuationTokens;
using Raven.Server.Documents.ShardedHandlers.ShardedCommands;
using Raven.Server.Documents.Sharding;
using Raven.Server.Json;
using Raven.Server.Routing;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Server;
using Sparrow.Threading;
using Sparrow.Utils;

namespace Raven.Server.Documents.ShardedHandlers
{
    public class ShardedCollectionHandler : ShardedRequestHandler
    {
        [RavenShardedAction("/databases/*/collections/stats", "GET")]
        public async Task GetCollectionStats()
        {
            var op = new ShardedCollectionStatisticsOperation();

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
            var op = new ShardedDetailedCollectionStatisticsOperation();
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
            var qToken = GetStringQueryString(ContinuationToken.ContinuationTokenQueryString, required: false);
            using (Server.ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            {
                var token = ContinuationToken.FromBase64<ShardedDocumentsPagingContinuation>(context, qToken) ?? 
                            new ShardedDocumentsPagingContinuation(ShardedContext, GetStart(), GetPageSize());

                var op = new ShardedCollectionDocumentsOperation(token);
                var results = (ExtendedStreamResult)(await ShardExecutor.ExecuteParallelForAllAsync(op));

                long numberOfResults;
                long totalDocumentsSizeInBytes;
                using (var cancelToken = CreateOperationToken())
                await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    var documents = GetDocuments(results, token, cancelToken.Token);
                    writer.WriteStartObject();
                    writer.WritePropertyName("Results");
                    (numberOfResults, totalDocumentsSizeInBytes) = await writer.WriteDocumentsAsync(context, documents, metadataOnly: false, cancelToken.Token);
                    writer.WriteComma();
                    writer.WritePropertyName(ContinuationToken.PropertyName);
                    writer.WriteString(token.ToBase64(context));
                    writer.WriteEndObject();
                }

                //AddPagingPerformanceHint(PagingOperationType.Documents, "Collection", HttpContext.Request.QueryString.Value, numberOfResults, pageSize, sw.ElapsedMilliseconds, totalDocumentsSizeInBytes);
                DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Karmel, DevelopmentHelper.Severity.Normal, "Handle sharded AddPagingPerformanceHint");
            }
        }

        public async IAsyncEnumerable<Document> GetDocuments(ExtendedStreamResult documents, ShardedDocumentsPagingContinuation pagingContinuation, [EnumeratorCancellation] CancellationToken token)
        {
            var pageSize = pagingContinuation.PageSize;
            using (var context = new ByteStringContext(SharedMultipleUseFlag.None))
            {
                await using (var merged = new MergedAsyncEnumerator<Document>(StreamDocumentByLastModifiedComparer.Instance))
                {
                    for (int i = 0; i < documents.Results.Span.Length; i++)
                    {
                        var contextPool = ShardedContext.RequestExecutors[i].ContextPool;
                        var it = new StreamOperation.YieldStreamResults(contextPool, response: documents.Results.Span[i], isQueryStream: false,
                            isTimeSeriesStream: false, isAsync: true, streamQueryStatistics: null);
                        await it.InitializeAsync();
                        await merged.AddAsyncEnumerator(new YieldDocuments(it, token));
                    }

                    while (await merged.MoveNextAsync(token))
                    {
                        if (pageSize-- <= 0)
                            yield break;

                        var id = merged.Current.Id;
                        var shard = ShardedContext.GetShardIndex(context, id);
                        pagingContinuation.Pages[shard].Start++;

                        yield return merged.Current;
                    }
                }
            }
        }

        public class ShardedDocumentsPagingContinuation : ContinuationToken
        {
            public int PageSize;
            public ShardPaging[] Pages;

            public ShardedDocumentsPagingContinuation()
            {
                
            }

            public ShardedDocumentsPagingContinuation(ShardedContext shardedContext, int start, int pageSize)
            {
                var shards = shardedContext.ShardCount;
                var startPortion = start / shards;
                var remaining = start - startPortion * shards;

                Pages = new ShardPaging[shards];
                
                for (var index = 0; index < Pages.Length; index++)
                {
                    Pages[index].Shard = index;
                    Pages[index].Start = startPortion;
                }
                Pages[0].Start += remaining;

                PageSize = pageSize;
            }

            public override DynamicJsonValue ToJson()
            {
                return new DynamicJsonValue
                {
                    [nameof(Pages)] = new DynamicJsonArray(Pages),
                    [nameof(PageSize)] = PageSize
                };
            }

            public struct ShardPaging : IDynamicJson
            {
                public int Shard;
                public int Start;
                public DynamicJsonValue ToJson()
                {
                    return new DynamicJsonValue
                    {
                        [nameof(Start)] = Start,
                        [nameof(Shard)] = Shard
                    };
                }
            }
        }
        public class ExtendedStreamResult : StreamResult
        {
            public Memory<StreamResult> Results;
        }

        private readonly struct ShardedCollectionDocumentsOperation : IShardedOperation<StreamResult>
        {
            private readonly ShardedDocumentsPagingContinuation _token;

            public ShardedCollectionDocumentsOperation(ShardedDocumentsPagingContinuation token)
            {
                _token = token;
            }

            public StreamResult Combine(Memory<StreamResult> results)
            {
                return new ExtendedStreamResult { Results = results };
            }

            public RavenCommand<StreamResult> CreateCommandForShard(int shard) =>
                StreamOperation.CreateStreamCommand(startsWith: null, matches: null, _token.Pages[shard].Start, _token.PageSize, exclude: null);
        }

        private struct ShardedCollectionStatisticsOperation : IShardedOperation<CollectionStatistics>
        {
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

        private struct ShardedDetailedCollectionStatisticsOperation : IShardedOperation<DetailedCollectionStatistics>
        {
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

    public class StreamDocumentByLastModifiedComparer : Comparer<Document>
    {
        public static StreamDocumentByLastModifiedComparer Instance = new StreamDocumentByLastModifiedComparer();

        public override int Compare(Document x, Document y)
        {
            var diff = x.LastModified.Ticks - y.LastModified.Ticks;
            if (diff > 0)
                return 1;
            if (diff < 0)
                return -1;
            return 0;
        }
    }

    internal class YieldDocuments : AsyncDocumentSession.AbstractYieldStream<Document>
    {
        public YieldDocuments(StreamOperation.YieldStreamResults enumerator, CancellationToken token) : base(enumerator, token)
        {
        }

        internal override Document ResultCreator(StreamOperation.YieldStreamResults asyncEnumerator)
        {
            var json = asyncEnumerator.Current;
            var metadata = json.GetMetadata();
            metadata.TryGet(Constants.Documents.Metadata.Id, out LazyStringValue id);

            return new Document
            {
                Data = json,
                Id = id
            };
        }
    }
}
