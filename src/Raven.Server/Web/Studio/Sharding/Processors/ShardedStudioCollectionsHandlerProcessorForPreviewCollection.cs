using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System.Web;
using NuGet.Packaging;
using Raven.Client.Extensions;
using Raven.Client.Http;
using Raven.Server.Documents;
using Raven.Server.Documents.ShardedHandlers;
using Raven.Server.Documents.ShardedHandlers.ShardedCommands;
using Raven.Server.Documents.Sharding;
using Raven.Server.Web.Studio.Processors;
using Sparrow.Json;
using Sparrow.Utils;

namespace Raven.Server.Web.Studio.Sharding.Processors;

public class ShardedStudioCollectionsHandlerProcessorForPreviewCollection : AbstractStudioCollectionsHandlerProcessorForPreviewCollection<ShardedRequestHandler>
{
    private IDisposable _releaseContext;

    private JsonOperationContext _context;

    private ShardedPagingContinuation _continuationToken;

    private PreviewCollectionResult _result;

    public ShardedStudioCollectionsHandlerProcessorForPreviewCollection(ShardedRequestHandler requestHandler)
        : base(requestHandler)
    {
    }

    protected override void Initialize()
    {
        base.Initialize();

        _releaseContext = RequestHandler.ContextPool.AllocateOperationContext(out _context);

        _continuationToken = RequestHandler.GetOrCreateContinuationToken(_context);
    }

    protected override JsonOperationContext GetContext()
    {
        return _context;
    }

    protected override long GetTotalResults()
    {
        return _result.TotalResults;
    }

    protected override bool NotModified(out string etag)
    {
        DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Karmel, DevelopmentHelper.Severity.Normal, "Need to figure out the best way to combine ETags and send not modified");

        etag = null;
        return false;
    }

    protected override async ValueTask<List<Document>> GetDocumentsAsync()
    {
        var op = new ShardedCollectionPreviewOperation(RequestHandler, _continuationToken, _context);
        _result = await RequestHandler.ShardExecutor.ExecuteParallelForAllAsync(op);

        return _result.Results;
    }

    protected override List<string> GetAvailableColumns(List<Document> documents)
    {
        return _result.AvailableColumns;
    }

    public override void Dispose()
    {
        base.Dispose();

        _releaseContext?.Dispose();
        _releaseContext = null;
    }

    private class PreviewCollectionResult
    {
        public List<Document> Results;
        public long TotalResults;
        public List<string> AvailableColumns;
    }

    private readonly struct ShardedCollectionPreviewOperation : IShardedOperation<PreviewCollectionResult>
    {
        private readonly ShardedRequestHandler _handler;
        private readonly ShardedPagingContinuation _token;
        private readonly JsonOperationContext _context;

        public ShardedCollectionPreviewOperation(ShardedRequestHandler handler, ShardedPagingContinuation token, JsonOperationContext context)
        {
            _handler = handler;
            _token = token;
            _context = context;
        }

        public PreviewCollectionResult Combine(Memory<PreviewCollectionResult> results)
        {
            var availableColumns = new HashSet<string>(StringComparer.InvariantCultureIgnoreCase);
            var total = new PreviewCollectionResult
            {
                Results = new List<Document>()
            };

            var span = results.Span;
            var totalDocuments = 0L;

            for (int i = 0; i < span.Length; i++)
            {
                availableColumns.AddRange(span[i].AvailableColumns);
                total.TotalResults += span[i].TotalResults;
            }

            for (int i = 0; i < span.Length * _token.PageSize; i++)
            {
                var takeFromShard = i % _token.Pages.Length;

                var shardDocs = span[takeFromShard].Results;
                if (shardDocs.Count == 0)
                    continue;

                if (totalDocuments++ >= _token.PageSize)
                    return total;

                var position = shardDocs.Count - 1;
                var doc = shardDocs[^1].Clone(_context);
                shardDocs.RemoveAt(position);
                _token.Pages[takeFromShard].Start++;
                total.Results.Add(doc);
            }

            total.AvailableColumns = availableColumns.ToList();
            total.Results.Sort(DocumentByLastModifiedComparer.Instance);
            return total;
        }

        public RavenCommand<PreviewCollectionResult> CreateCommandForShard(int shard) =>
            new ShardedCollectionPreviewCommand(_handler, _token.Pages[shard].Start, _token.PageSize);

        private class ShardedCollectionPreviewCommand : ShardedBaseCommand<PreviewCollectionResult>
        {
            public override bool IsReadRequest => true;

            public ShardedCollectionPreviewCommand(ShardedRequestHandler handler, int start, int pageSize) : base(handler, Documents.ShardedHandlers.ShardedCommands.Headers.IfNoneMatch, content: null)
            {
                var queryString = HttpUtility.ParseQueryString(handler.HttpContext.Request.QueryString.Value);
                queryString[Raven.Server.Web.RequestHandler.StartParameter] = start.ToString();
                queryString[Raven.Server.Web.RequestHandler.PageSizeParameter] = pageSize.ToString();
                Url = handler.BaseShardUrl + "?" + queryString;
            }

            public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
            {
                if (response == null)
                    ThrowInvalidResponse();

                var result = new PreviewCollectionResult
                {
                    Results = new List<Document>(),
                    AvailableColumns = new List<string>()
                };

                response.TryGet(nameof(PreviewCollectionResult.Results), out BlittableJsonReaderArray array);
                response.TryGet(nameof(PreviewCollectionResult.TotalResults), out result.TotalResults);
                response.TryGet(nameof(PreviewCollectionResult.AvailableColumns), out BlittableJsonReaderArray availableColumns);

                foreach (BlittableJsonReaderObject doc in array)
                {
                    var metadata = doc.GetMetadata();

                    result.Results.Add(new Document
                    {
                        Data = doc,
                        ChangeVector = metadata.GetChangeVector(),
                        LastModified = metadata.GetLastModified(),
                        Id = metadata.GetIdAsLazyString(),
                    });
                }

                foreach (LazyStringValue column in availableColumns)
                {
                    result.AvailableColumns.Add(column);
                }

                Result = result;
            }
        }
    }
}
