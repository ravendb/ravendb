using System;
using System.IO;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using Raven.Client.Documents.Changes;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.Sharding.Queries;
using Raven.Server.Json;
using Raven.Server.NotificationCenter;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Raven.Server.TrafficWatch;
using Sparrow.Json;
using Sparrow.Utils;

namespace Raven.Server.Documents.Sharding.Handlers
{
    public class ShardedQueriesHandler : ShardedDatabaseRequestHandler
    {
        [RavenShardedAction("/databases/*/queries", "POST")]
        public Task Post()
        {
            return HandleQuery(HttpMethod.Post);
        }

        [RavenShardedAction("/databases/*/queries", "GET")]
        public Task Get()
        {
            return HandleQuery(HttpMethod.Get);
        }

        public async Task HandleQuery(HttpMethod httpMethod)
        {
            DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Grisha, DevelopmentHelper.Severity.Normal, "Wrap this in a Processor");

            var metadataOnly = GetBoolValueQueryString("metadataOnly", required: false) ?? false;
            using (var tracker = new RequestTimeTracker(HttpContext, Logger, null, "Query"))
            {
                try
                {
                    DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Grisha, DevelopmentHelper.Severity.Normal, "CreateTimeLimitedQueryToken here or per each shard?");

                    var debug = GetStringQueryString("debug", required: false);
                    if (string.IsNullOrWhiteSpace(debug) == false)
                    {
                        DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Grisha, DevelopmentHelper.Severity.Normal, "Implement debug");
                        throw new NotImplementedException("Not yet done");
                    }

                    DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Grisha, DevelopmentHelper.Severity.Normal,
                        @"what do we do with: var diagnostics = GetBoolValueQueryString(""diagnostics"", required: false) ?? false");

                    var addSpatialProperties = GetBoolValueQueryString("addSpatialProperties", required: false) ?? false;
                    var indexQueryReader = new IndexQueryReader(GetStart(), GetPageSize(), HttpContext, RequestBodyStream(),
                        DatabaseContext.QueryMetadataCache, database: null, addSpatialProperties);

                    using (ContextPool.AllocateOperationContext(out TransactionOperationContext context))
                    {
                        var indexQuery = await indexQueryReader.GetIndexQueryAsync(context, Method, tracker);

                        if (TrafficWatchManager.HasRegisteredClients)
                            TrafficWatchQuery(indexQuery);

                        using (var queryProcessor = new ShardedQueryProcessor(context, this, indexQuery))
                        {
                            queryProcessor.Initialize();
                            await queryProcessor.ExecuteShardedOperations();

                            var existingResultEtag = GetLongFromHeaders("If-None-Match");
                            if (existingResultEtag != null && indexQuery.Metadata.HasOrderByRandom == false)
                            {
                                if (existingResultEtag == queryProcessor.ResultsEtag)
                                {
                                    HttpContext.Response.StatusCode = (int)HttpStatusCode.NotModified;
                                    return;
                                }
                            }

                            // * For includes, we send the includes to all shards, then we merge them together. We do explicitly
                            //   support including from another shard, so we'll need to do that again for missing includes
                            //   That means also recording the include() call from JS on missing values that we'll need to rerun on
                            //   other shards
                            var includeTask = queryProcessor.HandleIncludes();
                            if (includeTask.IsCompleted == false)
                            {
                                await includeTask.AsTask();
                            }

                            queryProcessor.MergeResults();

                            // * For map/reduce - we need to re-run the reduce portion of the index again on the results
                            queryProcessor.ReduceResults();

                            queryProcessor.ApplyPaging();

                            // * For map-reduce indexes we project the results after the reduce part 
                            queryProcessor.ProjectAfterMapReduce();

                            var result = queryProcessor.GetResult();
                            await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream(), HttpContext.RequestAborted))
                            {
                                result.Timings = indexQuery.Timings?.ToTimings();
                                await writer.WriteDocumentQueryResultAsync(context, result, metadataOnly);
                                await writer.OuterFlushAsync();
                            }

                            // * For JS projections and load clauses, we don't support calling load() on a
                            //   document that is not on the same shard
                            DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Grisha, DevelopmentHelper.Severity.Normal, "Add a test for that");
                        }
                    }
                }
                catch (Exception e)
                {
                    if (tracker.Query == null)
                    {
                        string errorMessage;
                        if (e is EndOfStreamException || e is ArgumentException)
                        {
                            errorMessage = "Failed: " + e.Message;
                        }
                        else
                        {
                            errorMessage = "Failed: " +
                                           HttpContext.Request.Path.Value +
                                           e.ToString();
                        }
                        tracker.Query = errorMessage;
                        if (TrafficWatchManager.HasRegisteredClients)
                            AddStringToHttpContext(errorMessage, TrafficWatchChangeType.Queries);
                    }
                    throw;
                }
            }
        }
    }
}
