using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Raven.Client.Http;
using Raven.Server.Documents.Sharding.Executors;
using Raven.Server.Documents.Sharding.Handlers;
using Raven.Server.Documents.Sharding.Handlers.ContinuationTokens;
using Raven.Server.Documents.Sharding.Operations;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Handlers.Processors.Collections
{
    internal abstract class AbstractCollectionHandlerProcessorForGetDocumentIds<TRequestHandler, TOperationContext> : AbstractDatabaseHandlerProcessor<TRequestHandler, TOperationContext>
        where TOperationContext : JsonOperationContext
        where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
    {
        protected AbstractCollectionHandlerProcessorForGetDocumentIds([NotNull] TRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected abstract Task<string[]> GetCollectionIdsAsync(string name, int start, int pageSize, CancellationToken token);

        public override async ValueTask ExecuteAsync()
        {
            var pageSize = RequestHandler.GetPageSize();
            var start = RequestHandler.GetStart();
            var name = RequestHandler.GetStringQueryString("name");

            using (var token = RequestHandler.CreateHttpRequestBoundOperationToken())
            {
                var ids = await GetCollectionIdsAsync(name, start, pageSize, token.Token);
                using (ContextPool.AllocateOperationContext(out JsonOperationContext context))
                {
                    var results = context.ReadObject(new DynamicJsonValue
                    {
                        ["Results"] = ids
                    },"get-ids");

                    await context.WriteAsync(RequestHandler.ResponseBodyStream(), results, token.Token);
                }
            }
        }
    }

    internal class CollectionHandlerProcessorForGetDocumentIds : AbstractCollectionHandlerProcessorForGetDocumentIds<DatabaseRequestHandler, DocumentsOperationContext>
    {
        public CollectionHandlerProcessorForGetDocumentIds(DatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected override Task<string[]> GetCollectionIdsAsync(string name, int start, int pageSize, CancellationToken token)
        {
            var isAllDocsCollection = string.IsNullOrEmpty(name);
            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (context.OpenReadTransaction())
            {
                var documents = isAllDocsCollection
                    ? RequestHandler.Database.DocumentsStorage
                        .GetDocumentsInReverseEtagOrder(context, start, pageSize, fields: DocumentFields.Id)
                    : RequestHandler.Database.DocumentsStorage
                        .GetDocumentsInReverseEtagOrder(context, name, start, pageSize, fields: DocumentFields.Id);

                return Task.FromResult(documents.Select(d => d.Id.ToString()).ToArray());
            }
        }
    }

    internal class ShardedCollectionHandlerProcessorForGetDocumentIds : AbstractCollectionHandlerProcessorForGetDocumentIds<ShardedDatabaseRequestHandler, TransactionOperationContext>
    {
        public ShardedCollectionHandlerProcessorForGetDocumentIds(ShardedDatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected override async Task<string[]> GetCollectionIdsAsync(string name, int start, int pageSize, CancellationToken token)
        {
            using (ContextPool.AllocateOperationContext(out JsonOperationContext context))
            {
                var continuationToken = RequestHandler.ContinuationTokens.GetOrCreateContinuationToken(context, start, pageSize);

                var op = new ShardedStreamDocumentsCollectionOperation(RequestHandler.HttpContext, name, continuationToken);
                var results = await RequestHandler.ShardExecutor.ExecuteParallelForAllAsync(op, token);
                return results.Result.Results;
            }
        }

        public class DocumentIdsResult
        {
            public string[] Results;
        }

        public readonly struct ShardedStreamDocumentsCollectionOperation : IShardedReadOperation<DocumentIdsResult>
        {
            private readonly HttpContext _httpContext;
            private readonly string _collectionName;
            private readonly ShardedPagingContinuation _token;

            public ShardedStreamDocumentsCollectionOperation(HttpContext httpContext, string collectionName, ShardedPagingContinuation token)
            {
                _httpContext = httpContext;
                _collectionName = collectionName;
                _token = token;
            }

            public HttpRequest HttpRequest => _httpContext.Request;
            public RavenCommand<DocumentIdsResult> CreateCommandForShard(int shardNumber) => 
                new GetIdsCommand(_collectionName, _token.Pages[shardNumber].Start, _token.PageSize);

            public string ExpectedEtag { get; } = null;
            public DocumentIdsResult CombineResults(Dictionary<int, ShardExecutionResult<DocumentIdsResult>> results)
            {
                var ids = new List<string>();
                foreach (var shard in results)
                {
                    ids.AddRange(shard.Value.Result.Results);
                }

                return new DocumentIdsResult
                {
                    Results = ids.ToArray()
                };
            }
        }

        public class GetIdsCommand : RavenCommand<DocumentIdsResult>
        {
            private readonly string _collection;
            private readonly int _start;
            private readonly int _pageSize;

            public GetIdsCommand(string collection, int start, int pageSize)
            {
                _collection = collection;
                _start = start;
                _pageSize = pageSize;
            }
            public override bool IsReadRequest => false;
            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/collections/ids?name={_collection}&start={_start}&pageSize={_pageSize}";

                return new HttpRequestMessage
                {
                    Method = HttpMethod.Get,
                };
            }
        }
    }
}
