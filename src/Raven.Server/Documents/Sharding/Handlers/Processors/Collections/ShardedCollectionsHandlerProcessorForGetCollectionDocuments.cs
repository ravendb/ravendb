using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Microsoft.AspNetCore.Http;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Session.Operations;
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

        protected override async ValueTask<(long numberOfResults, long totalDocumentsSizeInBytes)> GetCollectionDocumentsAndWriteAsync(TransactionOperationContext context, string name, int start, int pageSize, CancellationToken token)
        {
            DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Arek, DevelopmentHelper.Severity.Normal,
                "See `null` passed as etag to above new ShardedCollectionPreviewOperation()");

            var continuationToken = RequestHandler.ContinuationTokens.GetOrCreateContinuationToken(context, start, pageSize);

            var op = new ShardedStreamDocumentsCollectionOperation(RequestHandler.HttpContext, name, etag: null, continuationToken);
            var results = await RequestHandler.ShardExecutor.ExecuteParallelForAllAsync(op, token);
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
    }

    public class CollectionResult
    {
        public BlittableJsonReaderArray Results;
        public string ContinuationToken;
    }

    public readonly struct ShardedStreamDocumentsCollectionOperation : IShardedStreamableOperation
    {
        private readonly HttpContext _httpContext;
        private readonly string _collectionName;
        private readonly ShardedPagingContinuation _token;

        public ShardedStreamDocumentsCollectionOperation(HttpContext httpContext, string collectionName, string etag, ShardedPagingContinuation token)
        {
            _httpContext = httpContext;
            _collectionName = collectionName;
            _token = token;
            ExpectedEtag = etag;
        }

        public HttpRequest HttpRequest => _httpContext.Request;

        public RavenCommand<StreamResult> CreateCommandForShard(int shardNumber) =>
            StreamOperation.CreateCollectionDocsStreamCommand(_collectionName, _token.Pages[shardNumber].Start, _token.PageSize);

        public string ExpectedEtag { get; }

        public CombinedStreamResult CombineResults(Memory<StreamResult> results)
        {
            return new CombinedStreamResult { Results = results };
        }
    }
}
