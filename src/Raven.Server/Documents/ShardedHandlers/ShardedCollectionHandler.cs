using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Session.Operations;
using Raven.Client.Extensions;
using Raven.Client.Http;
using Raven.Server.Documents.ShardedHandlers.ContinuationTokens;
using Raven.Server.Documents.ShardedHandlers.ShardedCommands;
using Raven.Server.Documents.Sharding;
using Raven.Server.Json;
using Raven.Server.Routing;
using Sparrow.Json;
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
            using (Server.ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            {
                var token = GetOrCreateContinuationToken(context);

                var op = new ShardedCollectionDocumentsOperation(token);
                var results = await ShardExecutor.ExecuteParallelForAllAsync(op);

                long numberOfResults;
                long totalDocumentsSizeInBytes;
                using (var cancelToken = CreateOperationToken())
                await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    var documents = GetDocuments(results, token, cancelToken.Token);
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

        public async IAsyncEnumerable<Document> GetDocuments(CombinedStreamResult documents, ShardedPagingContinuation pagingContinuation, [EnumeratorCancellation] CancellationToken token)
        {
            await foreach (var result in ShardedContext.Streaming.PagedShardedStream(documents, BlittableToStreamDocument, StreamDocumentByLastModifiedComparer.Instance,
                               pagingContinuation, token))
            {
                yield return result.Item;
            }
        }

        private ShardStreamItem<Document> BlittableToStreamDocument(BlittableJsonReaderObject json)
        {
            var metadata = json.GetMetadata();
            return new ShardStreamItem<Document>
            {
                Item = new Document
                {
                    Data = json
                },
                Id = metadata.GetIdAsLazyString()
            };
        }
        
        public class CollectionResult
        {
            public List<object> Results;
            public string ContinuationToken;
        }

        private readonly struct ShardedCollectionDocumentsOperation : IShardedOperation<StreamResult, CombinedStreamResult>
        {
            private readonly ShardedPagingContinuation _token;

            public ShardedCollectionDocumentsOperation(ShardedPagingContinuation token)
            {
                _token = token;
            }

            public CombinedStreamResult Combine(Memory<StreamResult> results)
            {
                return new CombinedStreamResult { Results = results };
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

    public class StreamDocumentByLastModifiedComparer : Comparer<ShardStreamItem<Document>>
    {
        public static StreamDocumentByLastModifiedComparer Instance = new StreamDocumentByLastModifiedComparer();
        
        public override int Compare(ShardStreamItem<Document> x, ShardStreamItem<Document> y)
        {
            return DocumentByLastModifiedComparer.Instance.Compare(x.Item, y.Item);
        }
    }

    public class DocumentByLastModifiedComparer : Comparer<Document>
    {
        public static DocumentByLastModifiedComparer Instance = new DocumentByLastModifiedComparer();
        
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
}
