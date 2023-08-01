using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using NuGet.Packaging;
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
using Raven.Server.Json;
using Raven.Server.Web.Studio.Processors;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Web.Studio.Sharding.Processors;

public sealed class ShardedStudioCollectionsHandlerProcessorForPreviewCollection : AbstractStudioCollectionsHandlerProcessorForPreviewCollection<ShardedDatabaseRequestHandler, ShardStreamItem<Document>>
{
    private readonly ShardedDatabaseRequestHandler _requestHandler;
    private IDisposable _releaseContext;

    private JsonOperationContext _context;

    private ShardedPagingContinuation _continuationToken;

    private CombinedReadContinuationState _combinedReadState;
    private string _combinedEtag;

    public ShardedStudioCollectionsHandlerProcessorForPreviewCollection(ShardedDatabaseRequestHandler requestHandler)
        : base(requestHandler)
    {
        _requestHandler = requestHandler;
    }

    protected override async ValueTask InitializeAsync()
    {
        await base.InitializeAsync();

        _releaseContext = RequestHandler.ContextPool.AllocateOperationContext(out _context);
        _continuationToken = RequestHandler.ContinuationTokens.GetOrCreateContinuationToken(_context);

        var expectedEtag = RequestHandler.GetStringFromHeaders(Constants.Headers.IfNoneMatch);

        var op = new ShardedCollectionPreviewOperation(RequestHandler, Collection, expectedEtag, _continuationToken);
        var result = await RequestHandler.ShardExecutor.ExecuteParallelForAllAsync(op);
        if (result.StatusCode != (int)HttpStatusCode.NotModified)
            _combinedReadState = await result.Result.InitializeAsync(_requestHandler.DatabaseContext, _requestHandler.AbortRequestToken);
        _combinedEtag = result.CombinedEtag;
    }

    protected override JsonOperationContext GetContext()
    {
        return _context;
    }

    protected override async ValueTask WriteResultsAsync(
        AsyncBlittableJsonTextWriter writer,
        IAsyncEnumerable<ShardStreamItem<Document>> results,
        JsonOperationContext context,
        PreviewState state)
    {
        await base.WriteResultsAsync(writer, results, context, state);
        writer.WriteComma();
        writer.WriteContinuationToken(context, _continuationToken);
    }

    private sealed class ShardedPreviewState : PreviewState
    {
        private const string ShardNumberKey = "@shard-number";
        public int ShardNumber;

        public override DynamicJsonValue CreateMetadata(BlittableJsonReaderObject current)
        {
            return new DynamicJsonValue(current)
            {
                [ShardNumberKey] = ShardNumber
            };
        }
    }

    protected override PreviewState CreatePreviewState() => new ShardedPreviewState();

    protected override void WriteResult(AsyncBlittableJsonTextWriter writer, JsonOperationContext context, ShardStreamItem<Document> result, PreviewState state)
    {
        ((ShardedPreviewState)state).ShardNumber = result.ShardNumber;
        WriteDocument(writer, context, result.Item, state);
    }

    protected override async ValueTask<long> GetTotalResultsAsync()
    {
        var result = await _requestHandler.DatabaseContext.Streaming.ReadCombinedLongAsync(_combinedReadState, nameof(PreviewCollectionResult.TotalResults));
        var total = 0L;
        for (int i = 0; i < result.Span.Length; i++)
        {
            total += result.Span[i].Item;
        }

        return total;
    }

    protected override bool NotModified(out string etag)
    {
        etag = null;
        var etagFromRequest = RequestHandler.GetStringFromHeaders(Constants.Headers.IfNoneMatch);

        if (etagFromRequest != null && etagFromRequest == _combinedEtag)
            return true;

        etag = _combinedEtag;
        return false;
    }

    protected override IAsyncEnumerable<ShardStreamItem<Document>> GetDocumentsAsync() =>
        RequestHandler.DatabaseContext.Streaming.GetDocumentsAsync(_combinedReadState, _continuationToken);

    protected override async ValueTask<List<string>> GetAvailableColumnsAsync()
    {
        var result = await _requestHandler.DatabaseContext.Streaming.ReadCombinedObjectAsync(_combinedReadState, nameof(PreviewCollectionResult.AvailableColumns), ShardResultConverter.BlittableToStringListConverter);
        var total = new HashSet<string>();
        for (int i = 0; i < result.Span.Length; i++)
        {
            total.AddRange(result.Span[i].Item);
        }

        return total.ToList();
    }

    public override void Dispose()
    {
        _combinedReadState?.Dispose();
        _combinedReadState = null;

        base.Dispose();

        _releaseContext?.Dispose();
        _releaseContext = null;
    }

    private readonly struct ShardedCollectionPreviewOperation : IShardedStreamableOperation
    {
        private readonly ShardedDatabaseRequestHandler _handler;
        private readonly string _collection;
        private readonly ShardedPagingContinuation _token;

        public ShardedCollectionPreviewOperation(ShardedDatabaseRequestHandler handler, string collection, string etag, ShardedPagingContinuation token)
        {
            _handler = handler;
            _collection = collection;
            _token = token;
            ExpectedEtag = etag;
        }

        public HttpRequest HttpRequest => _handler.HttpContext.Request;

        public RavenCommand<StreamResult> CreateCommandForShard(int shardNumber)
        {
            return new ShardedCollectionPreviewCommand(_collection, _token.Pages[shardNumber].Start, _token.PageSize);
        }

        private sealed class ShardedCollectionPreviewCommand : RavenCommand<StreamResult>
        {
            private readonly string _collection;
            private readonly int _start;
            private readonly int _pageSize;

            public ShardedCollectionPreviewCommand(string collection, int start, int pageSize)
            {
                _collection = collection;
                _start = start;
                _pageSize = pageSize;
            }

            public override bool IsReadRequest => false;

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/studio/collections/preview?{Web.RequestHandler.StartParameter}={_start}&{Web.RequestHandler.PageSizeParameter}={_pageSize}";

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
                var responseStream = await response.Content.ReadAsStreamAsync().ConfigureAwait(false);

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
