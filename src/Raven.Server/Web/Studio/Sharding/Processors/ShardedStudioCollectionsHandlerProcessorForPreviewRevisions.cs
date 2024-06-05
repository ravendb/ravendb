using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using Amazon.Runtime.Internal.Endpoints.StandardLibrary;
using JetBrains.Annotations;
using Microsoft.AspNetCore.Http;
using Raven.Client;
using Raven.Client.Documents.Commands;
using Raven.Client.Http;
using Raven.Client.Util;
using Raven.Server.Documents;
using Raven.Server.Documents.Handlers.Processors.Revisions;
using Raven.Server.Documents.Sharding.Executors;
using Raven.Server.Documents.Sharding.Handlers;
using Raven.Server.Documents.Sharding.Handlers.ContinuationTokens;
using Raven.Server.Documents.Sharding.Operations;
using Raven.Server.Documents.Sharding.Streaming;
using Raven.Server.Documents.Sharding.Streaming.Comparers;
using Raven.Server.ServerWide.Context;
using Raven.Server.Web.Studio.Processors;
using Sparrow.Json;

namespace Raven.Server.Web.Studio.Sharding.Processors;

internal sealed class ShardedStudioCollectionsHandlerProcessorForPreviewRevisions : AbstractStudioCollectionsHandlerProcessorForPreviewRevisions<ShardedDatabaseRequestHandler,
    TransactionOperationContext>
{
    private ShardedPagingContinuation _continuationToken;
    private string _combinedHttpEtag;
    private CombinedReadContinuationState _combinedReadState;

    public ShardedStudioCollectionsHandlerProcessorForPreviewRevisions([NotNull] ShardedDatabaseRequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected override IAsyncEnumerable<Document> GetRevisionsAsync(TransactionOperationContext context)
    {
        return RequestHandler.DatabaseContext.Streaming.PagedShardedStream(
            _combinedReadState,
            "Results",
            ShardResultConverter.BlittableToRevisionConverter,
            StreamDocumentByLastModifiedComparer.Instance,
            _continuationToken).Select(s => s.Item); ;
    }

    protected override async ValueTask InitializeAsync(TransactionOperationContext context1)
    {
        await base.InitializeAsync(context1);
        using (RequestHandler.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            _continuationToken = RequestHandler.ContinuationTokens.GetOrCreateContinuationToken(context);

        var expectedEtag = RequestHandler.GetStringFromHeaders(Constants.Headers.IfNoneMatch);

        var op = new ShardedRevisionsCollectionPreviewOperation(RequestHandler, _collection, expectedEtag, _continuationToken);
        var result = await RequestHandler.ShardExecutor.ExecuteParallelForAllAsync(op);
        if (result.StatusCode != (int)HttpStatusCode.NotModified)
            _combinedReadState = await result.Result.InitializeAsync(RequestHandler.DatabaseContext, RequestHandler.AbortRequestToken);
        _combinedHttpEtag = result.CombinedEtag;
    }

    protected override IDisposable OpenReadTransaction(TransactionOperationContext context)
    {
        return context.OpenReadTransaction();
    }

    protected override void WriteAdditionalField(TransactionOperationContext context, AsyncBlittableJsonTextWriter writer)
    {
        writer.WriteComma();
        writer.WritePropertyName(ContinuationToken.PropertyName);
        writer.WriteString(_continuationToken.ToBase64(context));
    }

    protected override async ValueTask<long> GetTotalCountAsync()
    {
        var result = await RequestHandler.DatabaseContext.Streaming.ReadCombinedLongAsync(_combinedReadState, nameof(PreviewRevisionsResult.TotalResults));
        var total = 0L;
        for (int i = 0; i < result.Span.Length; i++)
        {
            total += result.Span[i].Item;
        }

        return total;
    }

    public override void Dispose()
    {
        _combinedReadState?.Dispose();
        _combinedReadState = null;

        base.Dispose();
    }

    protected override bool NotModified(TransactionOperationContext context, out string httpEtag)
    {
        httpEtag = null;
        var httpEtagFromRequest = RequestHandler.GetStringFromHeaders(Constants.Headers.IfNoneMatch);

        if (httpEtagFromRequest != null && httpEtagFromRequest == _combinedHttpEtag)
            return true;

        httpEtag = _combinedHttpEtag;
        return false;
    }

    private readonly struct ShardedRevisionsCollectionPreviewOperation : IShardedStreamableOperation
    {
        private readonly ShardedDatabaseRequestHandler _handler;
        private readonly string _collection;
        private readonly ShardedPagingContinuation _continuationToken;

        public ShardedRevisionsCollectionPreviewOperation(ShardedDatabaseRequestHandler handler, string collection, string etag, ShardedPagingContinuation continuationToken)
        {
            _handler = handler;
            _collection = collection;
            _continuationToken = continuationToken;
            ExpectedEtag = etag;
        }

        public HttpRequest HttpRequest => _handler.HttpContext.Request;

        public RavenCommand<StreamResult> CreateCommandForShard(int shardNumber)
        {
            return new ShardedRevisionsPreviewCommand(_collection, _continuationToken.Pages[shardNumber].Start, _continuationToken.PageSize);
        }

        private sealed class ShardedRevisionsPreviewCommand : RavenCommand<StreamResult>
        {
            private readonly string _collection;
            private readonly int _start;
            private readonly int _pageSize;

            public ShardedRevisionsPreviewCommand(string collection, int start, int pageSize)
            {
                _collection = collection;
                _start = start;
                _pageSize = pageSize;
            }

            public override bool IsReadRequest => false;

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/studio/revisions/preview?{Web.RequestHandler.StartParameter}={_start}&{Web.RequestHandler.PageSizeParameter}={_pageSize}";

                if (string.IsNullOrEmpty(_collection) == false)
                    url += $"&collection={Uri.EscapeDataString(_collection)}";

                var message = new HttpRequestMessage
                {
                    Method = HttpMethod.Get,
                };

                return message;
            }

            public override async Task<ResponseDisposeHandling> ProcessResponse(JsonOperationContext context, HttpCache cache, HttpResponseMessage response, string url)
            {
                var responseStream = await response.Content.ReadAsStreamWithZstdSupportAsync().ConfigureAwait(false);

                Result = new StreamResult
                {
                    Response = response,
                    Stream = new StreamWithTimeout(responseStream)
                };

                return ResponseDisposeHandling.Manually;
            }
        }

        public string ExpectedEtag { get; }

        public CombinedStreamResult CombineResults(Dictionary<int, ShardExecutionResult<StreamResult>> results)
        {
            return new CombinedStreamResult { Results = results };
        }
    }
}
