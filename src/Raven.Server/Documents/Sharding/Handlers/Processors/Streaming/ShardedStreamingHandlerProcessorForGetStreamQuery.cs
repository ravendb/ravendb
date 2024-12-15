using System;
using System.Collections.Generic;
using System.Net;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Microsoft.AspNetCore.Http;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Session;
using Raven.Client.Documents.Session.Operations;
using Raven.Client.Exceptions.Documents.Indexes;
using Raven.Client.Http;
using Raven.Server.Documents.Commands.Streaming;
using Raven.Server.Documents.Handlers.Processors.Streaming;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.Replication.Senders;
using Raven.Server.Documents.Sharding.Executors;
using Raven.Server.Documents.Sharding.Operations;
using Raven.Server.Documents.Sharding.Queries;
using Raven.Server.NotificationCenter;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow;
using Sparrow.Json;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.Streaming
{
    internal sealed class ShardedStreamingHandlerProcessorForGetStreamQuery : AbstractStreamingHandlerProcessorForGetStreamQuery<ShardedDatabaseRequestHandler, TransactionOperationContext>
    {
        public ShardedStreamingHandlerProcessorForGetStreamQuery([NotNull] ShardedDatabaseRequestHandler requestHandler, HttpMethod method) : base(requestHandler, method)
        {
        }

        protected override RequestTimeTracker GetTimeTracker()
        {
            return new RequestTimeTracker(HttpContext, Logger, RequestHandler.DatabaseContext.NotificationCenter, RequestHandler.DatabaseContext.Configuration, "StreamQuery", doPerformanceHintIfTooLong: false);
        }

        protected override async ValueTask<BlittableJsonReaderObject> GetDocumentDataAsync(TransactionOperationContext context, string fromDocument)
        {
            var shard = RequestHandler.DatabaseContext.GetShardNumberFor(context, fromDocument);

            var docs = await RequestHandler.ShardExecutor.ExecuteSingleShardAsync(context,
                new GetDocumentsCommand(RequestHandler.ShardExecutor.Conventions, new[] { fromDocument }, includes: null, metadataOnly: false), shard);
            return (BlittableJsonReaderObject)docs.Results[0];
        }

        protected override IDisposable AllocateContext(out TransactionOperationContext context)
        {
            return ContextPool.AllocateOperationContext(out context);
        }

        protected override QueryMetadataCache GetQueryMetadataCache()
        {
            return RequestHandler.DatabaseContext.QueryMetadataCache;
        }

        protected override ValueTask ExecuteAndWriteIndexQueryStreamEntriesAsync(TransactionOperationContext context, IndexQueryServerSide query, string format,
            string[] propertiesArray,
            string fileNamePrefix, bool ignoreLimit, OperationCancelToken token)
        {
            return ExecuteAndWriteQueryStreamAsync(context, query, format, propertiesArray, fileNamePrefix, debug: "entries", ignoreLimit, token);
        }

        private async ValueTask<ShardedStreamQueryResult> ExecuteShardedQueryAsync(TransactionOperationContext context, IndexQueryServerSide query, string debug, bool ignoreLimit, OperationCancelToken token)
        {
            var indexName = AbstractQueryRunner.GetIndexName(query);

            using (RequestHandler.DatabaseContext.QueryRunner.MarkQueryAsRunning(indexName, query, token))
            {
                var queryProcessor = new ShardedQueryStreamProcessor(context, RequestHandler, query, debug, ignoreLimit, token.Token);

                await queryProcessor.InitializeAsync();

                return await queryProcessor.ExecuteShardedOperations(null);
            }
        }

        protected override ValueTask ExecuteAndWriteQueryStreamAsync(TransactionOperationContext context, IndexQueryServerSide query, string format, string[] propertiesArray, string fileNamePrefix,
            OperationCancelToken token)
        {
            return ExecuteAndWriteQueryStreamAsync(context, query, format, propertiesArray, fileNamePrefix, debug: null, ignoreLimit: false, token);
        }

        private async ValueTask ExecuteAndWriteQueryStreamAsync(TransactionOperationContext context, IndexQueryServerSide query, string format, string[] propertiesArray, string fileNamePrefix, string debug, bool ignoreLimit,
            OperationCancelToken token)
        {
            //writer is blittable->document or blittable->csv
            await using (var writer = GetResultWriter<BlittableJsonReaderObject>(format, context, HttpContext.Response, RequestHandler.ResponseBodyStream(), propertiesArray, fileNamePrefix))
            {
                try
                {
                    using (var result = await ExecuteShardedQueryAsync(context, query, debug, ignoreLimit, token))
                    {
                        var queryResult = GetStreamQueryResult(context, format, query, writer, token); 
                        queryResult.TotalResults = result.Statistics.TotalResults;
                        queryResult.IndexName = result.Statistics.IndexName;
                        queryResult.IndexTimestamp = result.Statistics.IndexTimestamp;
                        queryResult.IsStale = result.Statistics.IsStale;
                        queryResult.ResultEtag = result.Statistics.ResultEtag;

                        using (var it = result.GetEnumerator())
                        {
                            foreach (BlittableJsonReaderObject doc in it)
                            {
                                await queryResult.AddResultAsync(doc, token.Token);
                            }    
                        }

                        queryResult.Flush();
                    }
                }
                catch (IndexDoesNotExistException)
                {
                    HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
                    await writer.WriteErrorAsync($"Index {query.Metadata.IndexName} does not exist");
                }
            }
        }

        private StreamQueryResult<BlittableJsonReaderObject> GetStreamQueryResult(JsonOperationContext context, string format, IndexQueryServerSide query, IStreamQueryResultWriter<BlittableJsonReaderObject> writer,
            OperationCancelToken token)
        {
            var queryFormat = GetQueryResultFormat(format);
            switch (queryFormat)
            {
                // writes blittable docs as blittable docs
                case QueryResultFormat.Default:
                case QueryResultFormat.Json:
                case QueryResultFormat.Jsonl:
                    return new StreamDocumentIndexQueryResult(HttpContext.Response, writer, indexDefinitionRaftIndex: null, token);
                case QueryResultFormat.Csv:
                    return new ShardedStreamQueryCsvResult(context, HttpContext.Response, writer, query, token);
                default:
                    throw new ArgumentOutOfRangeException($"Unknown format {format}");
            }
        }
    }

    public readonly struct ShardedStreamQueryOperation : IShardedOperation<StreamResult, ShardedStreamQueryResult>
    {
        private readonly HttpContext _httpContext;
        private readonly Func<(JsonOperationContext, IDisposable)> _allocateJsonContext;
        private readonly IComparer<BlittableJsonReaderObject> _comparer;
        private readonly Dictionary<int, PostQueryStreamCommand> _queryStreamCommands;
        private readonly long _skip;
        private readonly long _take;
        private readonly CancellationToken _token;

        public ShardedStreamQueryOperation(HttpContext httpContext, Func<(JsonOperationContext, IDisposable)> allocateJsonContext, IComparer<BlittableJsonReaderObject> comparer, Dictionary<int, PostQueryStreamCommand> queryStreamCommands, long skip, long take, CancellationToken token)
        {
            _httpContext = httpContext;
            _allocateJsonContext = allocateJsonContext;
            _comparer = comparer;
            _queryStreamCommands = queryStreamCommands;
            _skip = skip;
            _take = take;
            _token = token;
            ExpectedEtag = null;
        }

        public HttpRequest HttpRequest => _httpContext.Request;

        public RavenCommand<StreamResult> CreateCommandForShard(int shardNumber) => _queryStreamCommands[shardNumber];

        public string ExpectedEtag { get; }

        public ShardedStreamQueryResult Combine(Dictionary<int, ShardExecutionResult<StreamResult>> results)
        {
            var queryStats = new StreamQueryStatistics();

            var mergedEnumerator = new MergedEnumerator<BlittableJsonReaderObject>(_comparer);

            foreach (var streamResult in results.Values)
            {
                var qs = new StreamQueryStatistics();
                var enumerator = new StreamOperation.YieldStreamResults(_allocateJsonContext, streamResult.Result, isQueryStream: true, isTimeSeriesStream: false, isAsync: false, qs, _token);
                enumerator.Initialize();
                queryStats.TotalResults += qs.TotalResults;
                queryStats.IndexName = qs.IndexName;
                queryStats.IsStale |= qs.IsStale;
                queryStats.ResultEtag = Hashing.Combine(queryStats.ResultEtag, qs.ResultEtag);

                if (queryStats.IndexTimestamp < qs.IndexTimestamp)
                {
                    queryStats.IndexTimestamp = qs.IndexTimestamp;
                }

                mergedEnumerator.AddEnumerator(enumerator);
            }

            return new ShardedStreamQueryResult(mergedEnumerator, _skip, _take, queryStats);
        }
    }

    public readonly struct ShardedStreamQueryResult : IDisposable
    {
        private readonly MergedEnumerator<BlittableJsonReaderObject> _enumerator;
        private readonly long _skip;
        private readonly long _take;

        public readonly StreamQueryStatistics Statistics;

        public ShardedStreamQueryResult([NotNull] MergedEnumerator<BlittableJsonReaderObject> enumerator, long skip, long take, [NotNull] StreamQueryStatistics statistics)
        {
            _enumerator = enumerator ?? throw new ArgumentNullException(nameof(enumerator));
            _skip = skip;
            _take = take;
            Statistics = statistics ?? throw new ArgumentNullException(nameof(statistics));
        }

        public IEnumerator<BlittableJsonReaderObject> GetEnumerator()
        {
            var skip = _skip;
            var take = _take;

            foreach (var item in _enumerator)
            {
                if (skip-- > 0)
                {
                    item.Dispose();
                    continue;
                }

                if (take-- <= 0)
                    yield break;

                yield return item;
            }
        }

        public void Dispose()
        {
            _enumerator.Dispose();
        }
    }
}
