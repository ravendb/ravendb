using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Threading.Tasks;
using System.Web;
using Microsoft.AspNetCore.Http;
using NuGet.Packaging;
using Raven.Client.Documents.Commands;
using Raven.Client.Http;
using Raven.Client.Util;
using Raven.Server.Documents;
using Raven.Server.Documents.Sharding;
using Raven.Server.Documents.Sharding.Commands;
using Raven.Server.Documents.Sharding.Handlers;
using Raven.Server.Documents.Sharding.Handlers.ContinuationTokens;
using Raven.Server.Documents.Sharding.Operations;
using Raven.Server.Documents.Sharding.Streaming;
using Raven.Server.Json;
using Raven.Server.Web.Studio.Processors;
using Sparrow.Json;
using Sparrow.Utils;

namespace Raven.Server.Web.Studio.Sharding.Processors;

public class ShardedStudioCollectionsHandlerProcessorForPreviewCollection : AbstractStudioCollectionsHandlerProcessorForPreviewCollection<ShardedDatabaseRequestHandler>
{
    private readonly ShardedDatabaseRequestHandler _requestHandler;
    private IDisposable _releaseContext;

    private JsonOperationContext _context;

    private ShardedPagingContinuation _continuationToken;

    private CombinedReadContinuationState _combinedReadState;

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

        var op = new ShardedCollectionPreviewOperation(RequestHandler, null, _continuationToken);
        var result = await RequestHandler.ShardExecutor.ExecuteParallelForAllAsync(op);
        _combinedReadState = await result.Result.InitializeAsync(_requestHandler.DatabaseContext, _requestHandler.AbortRequestToken);
    }

    protected override JsonOperationContext GetContext()
    {
        return _context;
    }

    protected override async ValueTask WriteResultsAsync(
        AsyncBlittableJsonTextWriter writer, 
        IAsyncEnumerable<Document> documents, 
        JsonOperationContext context, 
        HashSet<string> propertiesPreviewToSend, 
        HashSet<string> fullPropertiesToSend,
        long totalResults, 
        List<string> availableColumns)
    {
        await base.WriteResultsAsync(writer, documents, context, propertiesPreviewToSend, fullPropertiesToSend, totalResults, availableColumns);
        writer.WriteComma();
        writer.WriteContinuationToken(context, _continuationToken);
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
        DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Karmel, DevelopmentHelper.Severity.Normal,
            "Need to figure out the best way to combine ETags and send not modified. See `null` passed as etag to above new ShardedCollectionPreviewOperation()");

        etag = null;
        return false;
    }

    protected override IAsyncEnumerable<Document> GetDocumentsAsync() =>
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
        private readonly ShardedPagingContinuation _token;

        public ShardedCollectionPreviewOperation(ShardedDatabaseRequestHandler handler, string etag, ShardedPagingContinuation token)
        {
            _handler = handler;
            _token = token;
            ExpectedEtag = etag;
        }

        public HttpRequest HttpRequest => _handler.HttpContext.Request;

        public RavenCommand<StreamResult> CreateCommandForShard(int shard) =>
            new ShardedCollectionPreviewCommand(_handler, _token.Pages[shard].Start, _token.PageSize);

        private class ShardedCommandAsStream : ShardedBaseCommand<StreamResult>
        {
            public override bool IsReadRequest => true;

            public ShardedCommandAsStream(ShardedDatabaseRequestHandler handler, Headers headers) : base(handler, headers, content: null)
            {
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

        private class ShardedCollectionPreviewCommand : ShardedCommandAsStream
        {
            public ShardedCollectionPreviewCommand(ShardedDatabaseRequestHandler handler, int start, int pageSize) : base(handler, Documents.Sharding.Commands.Headers.IfNoneMatch)
            {
                var queryString = HttpUtility.ParseQueryString(handler.HttpContext.Request.QueryString.Value);
                queryString[Web.RequestHandler.StartParameter] = start.ToString();
                queryString[Web.RequestHandler.PageSizeParameter] = pageSize.ToString();
                Url = handler.BaseShardUrl + "?" + queryString;
            }
        }

        public string ExpectedEtag { get; }

        public CombinedStreamResult CombineResults(Memory<StreamResult> results)
        {
            return new CombinedStreamResult {Results = results};
        }
    }
}
