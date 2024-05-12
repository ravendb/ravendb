using System;
using System.Collections.Generic;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Microsoft.AspNetCore.Http;
using Raven.Client;
using Raven.Client.Documents.Commands;
using Raven.Client.Http;
using Raven.Client.Util;
using Raven.Server.Documents;
using Raven.Server.Documents.Sharding.Executors;
using Raven.Server.Documents.Sharding.Handlers;
using Raven.Server.Documents.Sharding.Handlers.ContinuationTokens;
using Raven.Server.Documents.Sharding.Operations;
using Raven.Server.Documents.Sharding.Streaming;
using Raven.Server.ServerWide.Context;
using Raven.Server.Web.Studio.Processors;
using Sparrow.Json;
using static Raven.Server.Documents.Sharding.ShardedDatabaseContext.ShardedStreaming;

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

    protected override async Task WriteItemsAsync(TransactionOperationContext context, AsyncBlittableJsonTextWriter writer)
    {
        var shardItems = RequestHandler.DatabaseContext.Streaming.PagedShardedStream(
            _combinedReadState,
            "Results",
            x => x,
            RevisionLastModifiedComparer.Instance,
            _continuationToken);

        writer.WriteStartArray();

        var first = true;
        await foreach (var item in shardItems)
        {
            if (first)
                first = false;
            else
                writer.WriteComma();

            var json = item.Item;

            if (json.TryGet(nameof(Document.Id), out string id) == false)
                throw new InvalidOperationException("Revision does not contain 'Id' field.");

            if (json.TryGet(nameof(Document.ChangeVector), out string changeVector) == false)
                throw new InvalidOperationException($"Revision of \"{id}\" does not contain 'ChangeVector' field.");

            if (json.TryGet(nameof(Document.Etag), out int etag) == false)
                throw new InvalidOperationException($"Revision of \"{id}\" and change vector '{changeVector}' does not contain 'Etag' field.");

            if (json.TryGet(nameof(Document.LastModified), out DateTime lastModified) == false)
                throw new InvalidOperationException($"Revision of \"{id}\" and change vector '{changeVector}' does not contain 'LastModified' field.");

            if (json.TryGet(nameof(Document.Flags), out DocumentFlags flags) == false)
                throw new InvalidOperationException($"Revision of \"{id}\" and change vector '{changeVector}' does not contain 'Flags' field.");

            writer.WriteStartObject();

            writer.WritePropertyName(nameof(Document.Id));
            writer.WriteString(id);
            writer.WriteComma();

            writer.WritePropertyName(nameof(Document.Etag));
            writer.WriteInteger(etag);
            writer.WriteComma();

            writer.WritePropertyName(nameof(Document.LastModified));
            writer.WriteDateTime(lastModified, isUtc: true);
            writer.WriteComma();

            writer.WritePropertyName(nameof(Document.ChangeVector));
            writer.WriteString(changeVector);
            writer.WriteComma();

            writer.WritePropertyName(nameof(Document.Flags));
            writer.WriteString(flags.ToString());
            writer.WriteComma();

            writer.WritePropertyName(nameof(ShardStreamItem<BlittableJsonReaderObject>.ShardNumber));
            writer.WriteInteger(item.ShardNumber);

            writer.WriteEndObject();
        }

        writer.WriteEndArray();

        writer.WriteComma();
        writer.WritePropertyName(ContinuationToken.PropertyName);
        writer.WriteString(_continuationToken.ToBase64(context));
    }

    protected override async Task InitializeAsync(TransactionOperationContext context, CancellationToken token)
    {
        await base.InitializeAsync(context, token);

        _continuationToken = RequestHandler.ContinuationTokens.GetOrCreateContinuationToken(context);

        var expectedEtag = RequestHandler.GetStringFromHeaders(Constants.Headers.IfNoneMatch);

        var op = new ShardedRevisionsCollectionPreviewOperation(RequestHandler, Collection, expectedEtag, _continuationToken);
        var result = await RequestHandler.ShardExecutor.ExecuteParallelForAllAsync(op, token);
        if (result.StatusCode != (int)HttpStatusCode.NotModified)
            _combinedReadState = await result.Result.InitializeAsync(RequestHandler.DatabaseContext, RequestHandler.AbortRequestToken);
        _combinedHttpEtag = result.CombinedEtag;
    }

    protected override IDisposable OpenReadTransaction(TransactionOperationContext context) => null;

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
