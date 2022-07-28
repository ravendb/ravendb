using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Microsoft.AspNetCore.Http;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Session.Operations;
using Raven.Client.Http;
using Raven.Server.Documents.Handlers.Processors.Streaming;
using Raven.Server.Documents.Sharding.Handlers.ContinuationTokens;
using Raven.Server.Documents.Sharding.Handlers.Processors.Collections;
using Raven.Server.Documents.Sharding.Operations;
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
