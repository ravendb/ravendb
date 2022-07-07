using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Primitives;
using Raven.Client.Documents.Commands;
using Raven.Client.Exceptions.Sharding;
using Raven.Client.Http;
using Raven.Server.Documents.Commands.Streaming;
using Raven.Server.Documents.Handlers.Processors.Streaming;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.Sharding.Handlers.ContinuationTokens;
using Raven.Server.Documents.Sharding.Operations;
using Raven.Server.Documents.Sharding.Queries;
using Raven.Server.Documents.Sharding.Streaming;
using Raven.Server.NotificationCenter;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.Streaming
{
    internal class ShardedStreamingHandlerProcessorForGetStreamQuery : AbstractStreamingHandlerProcessorForGetStreamQuery<ShardedDatabaseRequestHandler, TransactionOperationContext>
    {
        public ShardedStreamingHandlerProcessorForGetStreamQuery([NotNull] ShardedDatabaseRequestHandler requestHandler, HttpMethod method) : base(requestHandler, method)
        {
        }
        
        protected override async ValueTask ExecuteQueryAndWriteAsync(TransactionOperationContext context, IndexQueryServerSide query, string format, string debug, bool ignoreLimit, StringValues properties, RequestTimeTracker tracker, OperationCancelToken token)
        {
            //TODO stav: use the same time limited token for the sharded operation (as the non sharded)?
            using (context.OpenReadTransaction())
            {
                var queryProcessor = new ShardedQueryProcessor(context, RequestHandler, query);
                if (queryProcessor.IsMapReduce())
                    throw new NotSupportedInShardingException("MapReduce is not supported in sharded streaming queries");

                //TODO stav: do we support includes in sharded streaming?
                queryProcessor.Initialize();

                //TODO stav: Initialize() rewrites the query for paging but creates a blittable of it inside the processor and doesn't modify this 'query' var by ref

                var cmds = new PostQueryStreamCommand[RequestHandler.DatabaseContext.ShardCount];
                for (int i = 0; i < cmds.Length; i++)
                {
                    cmds[i] = new PostQueryStreamCommand(query.ToJson(context), format, debug, ignoreLimit, properties); //TODO stav: query.ToJson(context) needs to be queryTemplate from the processor!
                }

                //TODO stav: continuation token needs to get the offset and limit AFTER the rewrite - needs to be changed
                var continuationToken = RequestHandler.ContinuationTokens.GetOrCreateContinuationToken(context, query.Offset ?? 0, query.Limit ?? Int32.MaxValue);

                var op = new ShardedStreamQueryOperation(HttpContext, cmds);
                var results = await RequestHandler.ShardExecutor.ExecuteParallelForAllAsync(op);
                using var streams = await results.Result.InitializeAsync(RequestHandler.DatabaseContext, HttpContext.RequestAborted);

                //TODO stav: query metadata also needs to be after the rewrite
                IAsyncEnumerable<BlittableJsonReaderObject> documents = OrderDocumentsByQuery(streams, query.Metadata, continuationToken);

                //TODO stav: need to combine QueryStats and have them be written to stream first
                await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
                {
                    writer.WriteStartObject();

                    await writer.WriteArrayAsync("Results", documents);

                    writer.WriteEndObject();
                }
            }
        }

        protected override RequestTimeTracker GetTimeTracker()
        {
            return new RequestTimeTracker(HttpContext, Logger, null, "StreamQuery", doPerformanceHintIfTooLong: false);
        }

        protected override async ValueTask<BlittableJsonReaderObject> GetDocumentData(TransactionOperationContext context, string fromDocument)
        {
            var ids = ShardLocator.GetDocumentIdsByShards(context, RequestHandler.DatabaseContext, new[] {fromDocument});
            for (int i = 0; i < ids.Count; i++)
            {
                if (ids[i].Ids.Contains(fromDocument))
                {
                    var docs = await RequestHandler.ShardExecutor.ExecuteSingleShardAsync(context,
                        new GetDocumentsCommand(new string[] { fromDocument }, null, metadataOnly: false), i);
                    return (BlittableJsonReaderObject)docs.Results.Items.Current;
                }
            }

            return null;
        }

        protected override IDisposable AllocateContext(out TransactionOperationContext context)
        {
            return ContextPool.AllocateOperationContext(out context);
        }

        protected override QueryMetadataCache GetQueryMetadataCache()
        {
            return RequestHandler.DatabaseContext.QueryMetadataCache;
        }

        public async IAsyncEnumerable<BlittableJsonReaderObject> OrderDocumentsByQuery(CombinedReadContinuationState documents, QueryMetadata queryMetadata, ShardedPagingContinuation pagingContinuation)
        {
            await foreach (var result in RequestHandler.DatabaseContext.Streaming.PagedShardedStream(
                               documents,
                               "Results",
                               x => x,
                               new DocumentBlittableQueryComparer(new ShardedDocumentsComparer(queryMetadata, isMapReduce: false)),
                               pagingContinuation))
            {
                yield return result.Item;
            }
        }

        public class DocumentBlittableQueryComparer : Comparer<ShardStreamItem<BlittableJsonReaderObject>>
        {
            private readonly ShardedDocumentsComparer _queryComparer;

            public DocumentBlittableQueryComparer(ShardedDocumentsComparer queryComparer)
            {
                _queryComparer = queryComparer;
            }

            public override int Compare(ShardStreamItem<BlittableJsonReaderObject> x, ShardStreamItem<BlittableJsonReaderObject> y)
            {
                return _queryComparer.Compare(x.Item, y.Item);
            }
        }
    }

    public readonly struct ShardedStreamQueryOperation : IShardedStreamableOperation
    {
        private readonly HttpContext _httpContext;
        private readonly PostQueryStreamCommand[] _queryStreamCommands;

        public ShardedStreamQueryOperation(HttpContext httpContext, PostQueryStreamCommand[] queryStreamCommands)
        {
            _httpContext = httpContext;
            _queryStreamCommands = queryStreamCommands;
            ExpectedEtag = null; //TODO stav: what is this
        }

        public HttpRequest HttpRequest => _httpContext.Request;

        public RavenCommand<StreamResult> CreateCommandForShard(int shardNumber) => _queryStreamCommands[shardNumber];

        public string ExpectedEtag { get; }

        public CombinedStreamResult CombineResults(Memory<StreamResult> results)
        {
            return new CombinedStreamResult { Results = results };
        }
    }
}
