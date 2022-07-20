using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Microsoft.AspNetCore.Http;
using Raven.Client;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Session;
using Raven.Client.Documents.Session.Operations;
using Raven.Client.Exceptions.Sharding;
using Raven.Client.Extensions;
using Raven.Client.Http;
using Raven.Server.Documents.Commands.Streaming;
using Raven.Server.Documents.Handlers;
using Raven.Server.Documents.Handlers.Processors.Streaming;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.Replication.Senders;
using Raven.Server.Documents.Sharding.Operations;
using Raven.Server.Documents.Sharding.Queries;
using Raven.Server.NotificationCenter;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Utils;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.Streaming
{
    internal class ShardedStreamingHandlerProcessorForGetStreamQuery : AbstractStreamingHandlerProcessorForGetStreamQuery<ShardedDatabaseRequestHandler, TransactionOperationContext>
    {
        public ShardedStreamingHandlerProcessorForGetStreamQuery([NotNull] ShardedDatabaseRequestHandler requestHandler, HttpMethod method) : base(requestHandler, method)
        {
        }
        
        protected override RequestTimeTracker GetTimeTracker()
        {
            return new RequestTimeTracker(HttpContext, Logger, RequestHandler.DatabaseContext.NotificationCenter, RequestHandler.DatabaseContext.Configuration, "StreamQuery", doPerformanceHintIfTooLong: false);
        }

        protected override async ValueTask<BlittableJsonReaderObject> GetDocumentData(TransactionOperationContext context, string fromDocument)
        {
            var shard = RequestHandler.DatabaseContext.GetShardNumber(context, fromDocument);
            
            var docs = await RequestHandler.ShardExecutor.ExecuteSingleShardAsync(context,
                new GetDocumentsCommand(new string[] { fromDocument }, null, metadataOnly: false), shard);
            return (BlittableJsonReaderObject)docs.Results.Items.Current;
        }

        protected override IDisposable AllocateContext(out TransactionOperationContext context)
        {
            return ContextPool.AllocateOperationContext(out context);
        }

        protected override QueryMetadataCache GetQueryMetadataCache()
        {
            return RequestHandler.DatabaseContext.QueryMetadataCache;
        }

        private async ValueTask<(IEnumerator<BlittableJsonReaderObject>, StreamQueryStatistics)> ExecuteQueryAsync(TransactionOperationContext context, IndexQueryServerSide query, string debug, bool ignoreLimit, OperationCancelToken token)
        {
            //TODO stav: use the time limited token we create in abstract for the entire sharded operation?

            var queryProcessor = new ShardedQueryStreamProcessor(context, RequestHandler, query);
            if (queryProcessor.IsMapReduce())
                throw new NotSupportedInShardingException("MapReduce is not supported in sharded streaming queries");

            queryProcessor.Initialize(out BlittableJsonReaderObject queryTemplate);

            var cmds = new PostQueryStreamCommand[RequestHandler.DatabaseContext.ShardCount];
            for (int i = 0; i < cmds.Length; i++)
            {
                cmds[i] = new PostQueryStreamCommand(queryTemplate, debug, ignoreLimit);
            }

            DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Stav, DevelopmentHelper.Severity.Normal, "Handle continuation token in streaming");
            //var continuationToken = RequestHandler.ContinuationTokens.GetOrCreateContinuationToken(context, query.Offset ?? 0, query.Limit ?? Int32.MaxValue);

            IComparer<BlittableJsonReaderObject> comparer = query.Metadata.OrderBy?.Length > 0
                ? new ShardedDocumentsComparer(query.Metadata, isMapReduce: false)
                : new DocumentBlittableLastModifiedComparer();

            var op = new ShardedStreamQueryOperation(HttpContext, () =>
            {
                IDisposable returnToContextPool = ContextPool.AllocateOperationContext(out JsonOperationContext ctx);
                return (ctx, returnToContextPool);
            }, comparer, cmds,  skip: query.Offset ?? 0, take: query.Limit ?? Int32.MaxValue, token.Token);

            return await RequestHandler.ShardExecutor.ExecuteParallelForAllAsync(op, token.Token);
        }

        protected override async ValueTask ExecuteAndWriteQueryStreamAsync(TransactionOperationContext context, IndexQueryServerSide query, string format,
            string[] propertiesArray, string fileNamePrefix, bool ignoreLimit, bool _, OperationCancelToken token)
        {
            var (results, queryStatistics) = await ExecuteQueryAsync(context, query, null, ignoreLimit, token);
            await using (var writer = new StreamBlittableDocumentQueryResultWriter(RequestHandler.ResponseBodyStream(), context))
            {
                var queryResult = new StreamDocumentIndexEntriesQueryResult(HttpContext.Response, writer, token);// writes blittable docs as blittable docs
                queryResult.TotalResults = queryStatistics.TotalResults;
                queryResult.IndexName = queryStatistics.IndexName;
                queryResult.IndexTimestamp = queryStatistics.IndexTimestamp;
                queryResult.IsStale = queryStatistics.IsStale;
                //queryStatistics.ResultEtag

                foreach (BlittableJsonReaderObject doc in results)
                {
                    await queryResult.AddResultAsync(doc, token.Token);
                }
                queryResult.Flush();
            }
        }

        protected override async ValueTask ExecuteAndWriteIndexQueryStreamEntriesAsync(TransactionOperationContext context, IndexQueryServerSide query, string format, string debug, string[] propertiesArray,
            string fileNamePrefix, bool ignoreLimit, bool _, OperationCancelToken token)
        {
            //make requests to shards asking for regular json format instead of csv
            //write results to stream as csv

            var (results, _) = await ExecuteQueryAsync(context, query, debug, ignoreLimit, token);

            await using (var writer = GetBlittableQueryResultWriter(format, context, HttpContext.Response, RequestHandler.ResponseBodyStream(), fromSharded: false, propertiesArray,
                             fileNamePrefix))
            {
                var queryResult = new StreamDocumentIndexEntriesQueryResult(HttpContext.Response, writer, token);
                
                foreach (BlittableJsonReaderObject doc in results)
                {
                    await queryResult.AddResultAsync(doc, token.Token);
                }
                queryResult.Flush();
            }
        }

        public class DocumentBlittableLastModifiedComparer : Comparer<BlittableJsonReaderObject>
        {
            public override int Compare(BlittableJsonReaderObject x, BlittableJsonReaderObject y)
            {
                return y.GetMetadata().GetLastModified().CompareTo(x.GetMetadata().GetLastModified());
            }

            public static DocumentBlittableLastModifiedComparer Instance = new();
        }
    }

    public readonly struct ShardedStreamQueryOperation : IShardedOperation<StreamResult, (IEnumerator<BlittableJsonReaderObject>, StreamQueryStatistics)>
    {
        private readonly HttpContext _httpContext;
        private readonly Func<(JsonOperationContext, IDisposable)> _allocateJsonContext;
        private readonly IComparer<BlittableJsonReaderObject> _comparer;
        private readonly PostQueryStreamCommand[] _queryStreamCommands;
        private readonly int _skip;
        private readonly int _take;
        private readonly CancellationToken _token;

        public ShardedStreamQueryOperation(HttpContext httpContext, Func<(JsonOperationContext, IDisposable)> allocateJsonContext, IComparer<BlittableJsonReaderObject> comparer, PostQueryStreamCommand[] queryStreamCommands, int skip, int take, CancellationToken token)
        {
            _httpContext = httpContext;
            _allocateJsonContext = allocateJsonContext;
            _comparer = comparer;
            _queryStreamCommands = queryStreamCommands;
            _skip = skip;
            _take = take;
            _token = token;
            ExpectedEtag = null; //TODO stav: what is this
        }

        public HttpRequest HttpRequest => _httpContext.Request;

        public RavenCommand<StreamResult> CreateCommandForShard(int shardNumber) => _queryStreamCommands[shardNumber];

        public string ExpectedEtag { get; }

        public (IEnumerator<BlittableJsonReaderObject>, StreamQueryStatistics) Combine(Memory<StreamResult> results)
        {
            var queryStats = new StreamQueryStatistics();

            var mergedEnumerator = new MergedEnumerator<BlittableJsonReaderObject>(_comparer);

            foreach (var streamResult in results.Span)
            {
                var qs = new StreamQueryStatistics();
                var enumerator = new StreamOperation.YieldStreamResults(_allocateJsonContext, streamResult, isQueryStream: true, isTimeSeriesStream: false, isAsync: false, qs, _token);
                enumerator.Initialize();
                queryStats.TotalResults += qs.TotalResults;
                queryStats.IndexName = qs.IndexName;
                queryStats.IsStale |= qs.IsStale;
                queryStats.TotalResults += qs.TotalResults;
                if (queryStats.IndexTimestamp < qs.IndexTimestamp)
                {
                    queryStats.IndexTimestamp = qs.IndexTimestamp;
                }

                mergedEnumerator.AddEnumerator(enumerator);
            }

            return (ApplySkipTake(mergedEnumerator, _skip, _take), queryStats);
        }

        private IEnumerator<BlittableJsonReaderObject> ApplySkipTake(MergedEnumerator<BlittableJsonReaderObject> mergedEnumerator, int skip, int take)
        {
            foreach (var item in mergedEnumerator)
            {
                if(skip-- > 0)
                    continue;

                if (take-- <= 0)
                    yield break;

                yield return item;
            }
        }
    }
}
