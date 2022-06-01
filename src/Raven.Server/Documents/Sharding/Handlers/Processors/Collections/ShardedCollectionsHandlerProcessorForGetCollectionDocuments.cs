using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Microsoft.AspNetCore.Http;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Session.Operations;
using Raven.Client.Exceptions.Sharding;
using Raven.Client.Http;
using Raven.Server.Documents.Handlers.Processors.Collections;
using Raven.Server.Documents.Sharding.Handlers.ContinuationTokens;
using Raven.Server.Documents.Sharding.Operations;
using Raven.Server.Documents.Sharding.Streaming;
using Raven.Server.Json;
using Raven.Server.NotificationCenter.Notifications.Details;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Utils;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.Collections
{
    internal class ShardedCollectionsHandlerProcessorForGetCollectionDocuments : AbstractCollectionsHandlerProcessorForGetCollectionDocuments<ShardedDatabaseRequestHandler, TransactionOperationContext>
    {
        public ShardedCollectionsHandlerProcessorForGetCollectionDocuments([NotNull] ShardedDatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected override async ValueTask<(long numberOfResults, long totalDocumentsSizeInBytes)> GetCollectionDocumentsAndWriteAsync(TransactionOperationContext context, string name, int _, int __, CancellationToken token)
        {
            throw new NotSupportedInShardingException($"Fetching documents by collection not supported in sharding.");

            DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Karmel, DevelopmentHelper.Severity.Normal,
                "Need to have ShardedStreamDocumentsOperation fetch by collection name");

            DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Arek, DevelopmentHelper.Severity.Normal,
                "See `null` passed as etag to above new ShardedCollectionPreviewOperation()");

            var continuationToken = RequestHandler.ContinuationTokens.GetOrCreateContinuationToken(context);

            var op = new ShardedStreamDocumentsOperation(HttpContext, null, continuationToken);
            var results = await RequestHandler.ShardExecutor.ExecuteParallelForAllAsync(op);
            using var streams = await results.Result.InitializeAsync(RequestHandler.DatabaseContext, HttpContext.RequestAborted);
            var documents = GetDocuments(streams, continuationToken);

            long numberOfResults;
            long totalDocumentsSizeInBytes;

            await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
            {
                writer.WriteStartObject();
                writer.WritePropertyName("Results");
                (numberOfResults, totalDocumentsSizeInBytes) = await writer.WriteDocumentsAsync(context, documents, metadataOnly: false, token);

                if (continuationToken != null)
                {
                    writer.WriteComma();
                    writer.WriteContinuationToken(context, continuationToken);
                }
                writer.WriteEndObject();
            }

            return (numberOfResults, totalDocumentsSizeInBytes);
        }

        public async IAsyncEnumerable<Document> GetDocuments(CombinedReadContinuationState documents, ShardedPagingContinuation pagingContinuation)
        {
            await foreach (var result in RequestHandler.DatabaseContext.Streaming.PagedShardedDocumentsByLastModified(documents, nameof(CollectionResult.Results), pagingContinuation))
            {
                yield return result.Item;
            }
        }

        protected override void AddPagingPerformanceHint(PagingOperationType operation, string action, string details, long numberOfResults, int pageSize, long duration,
            long totalDocumentsSizeInBytes)
        {
            DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Karmel, DevelopmentHelper.Severity.Normal, "Handle sharded AddPagingPerformanceHint");
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
            return new CombinedStreamResult { Results = results };
        }
    }
}
