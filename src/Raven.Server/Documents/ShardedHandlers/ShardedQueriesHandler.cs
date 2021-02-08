using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using Raven.Client.Documents.Changes;
using Raven.Client.Documents.Queries;
using Raven.Server.Documents.Handlers;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.Queries.LuceneIntegration;
using Raven.Server.Documents.ShardedHandlers.ShardedCommands;
using Raven.Server.Documents.Sharding;
using Raven.Server.Json;
using Raven.Server.NotificationCenter;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Raven.Server.TrafficWatch;
using Raven.Server.Utils;
using Sparrow;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using HttpMethod = Microsoft.AspNetCore.Server.Kestrel.Core.Internal.Http.HttpMethod;

namespace Raven.Server.Documents.ShardedHandlers
{
    public class ShardedQueriesHandler : ShardedRequestHandler
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
            // TODO: Sharding - figure out how to report this
            var metadataOnly = GetBoolValueQueryString("metadataOnly", required: false) ?? false;
            using (var tracker = new RequestTimeTracker(HttpContext, Logger, null, "Query"))
            {
                try
                {
                    //TODO: Sharding
                    //using (var token = CreateTimeLimitedQueryToken())
                    {
                        var debug = GetStringQueryString("debug", required: false);
                        if (string.IsNullOrWhiteSpace(debug) == false)
                        {
                            throw new NotImplementedException("Not yet done");
                        }

                        // TODO: Sharding
                        //var diagnostics = GetBoolValueQueryString("diagnostics", required: false) ?? false;
                        
                        var indexQueryReader = new QueriesHandler.IndexQueryReader(GetStart(), GetPageSize(), HttpContext, RequestBodyStream(), 
                            ShardedContext.QueryMetadataCache, database: null);

                        using (ContextPool.AllocateOperationContext(out TransactionOperationContext context))
                        {
                            var indexQuery = await indexQueryReader.GetIndexQueryAsync(context, Method, tracker);
                            
                            if (TrafficWatchManager.HasRegisteredClients)
                                TrafficWatchQuery(indexQuery);
                            
                            var queryProcessor = new ShardedQueryProcessor(context, this, indexQuery);
                            
                            queryProcessor.Initialize();

                            await queryProcessor.ExecuteShardedOperations();
                            
                            // * For includes, we send the includes to all shards, then we merge them together. We do explicitly
                            //   support including from another shard, so we'll need to do that again for missing includes
                            //   That means also recording the include() call from JS on missing values that we'll need to rerun on
                            //   other shards
                            var includeTask = queryProcessor.HandleIncludes();
                            if (includeTask.IsCompleted == false)
                            {
                                await includeTask.AsTask();
                            }
                            
                            // * For map/reduce - we need to re-run the reduce portion of the index again on the results
                            queryProcessor.ReduceResults();

                            // * For order by, we need to merge the results and compute the order again 
                            queryProcessor.OrderResults();

                            var result = queryProcessor.GetResult();
                            int numberOfResults;
                            using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream(), HttpContext.RequestAborted))
                            {
                                result.Timings = indexQuery.Timings?.ToTimings();
                                numberOfResults = await writer.WriteDocumentQueryResultAsync(context, result, metadataOnly); 
                                await writer.OuterFlushAsync();
                            }


                            // * For paging queries, we modify the limits on the query to include all the results from all
                            //   shards

                            // * For JS projections and load clauses, we don't support calling load() on a
                            //   document that is not on the same shard
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
        
        /// <summary>
        /// TrafficWatchQuery writes query data to httpContext
        /// </summary>
        /// <param name="indexQuery"></param>
        private void TrafficWatchQuery(IndexQueryServerSide indexQuery)
        {
            var sb = new StringBuilder();
            // append stringBuilder with the query
            sb.Append(indexQuery.Query);
            // if query got parameters append with parameters
            if (indexQuery.QueryParameters != null && indexQuery.QueryParameters.Count > 0)
                sb.AppendLine().Append(indexQuery.QueryParameters);
            AddStringToHttpContext(sb.ToString(), TrafficWatchChangeType.Queries);
        }

        /// <summary>
        /// A struct that we use to hold state and break the process
        /// of handling a sharded query into distinct steps
        /// </summary>
        private readonly struct ShardedQueryProcessor
        {
            private readonly TransactionOperationContext _context;
            private readonly ShardedQueriesHandler _parent;
            private readonly IndexQueryServerSide _query;
            private readonly Dictionary<int, ShardedQueryCommand> _commands;
            private readonly HashSet<int> _filteredShardIndexes;
            private readonly ShardedQueryResult _result;


            // User should also be able to define a query parameter ("Sharding.Context") which is an array
            // that contains the ids whose shards the query should be limited to. Advanced: Optimization if user
            // want to run a query and knows what shards it is on. Such as:
            // from Orders where State = $state and User = $user where all the orders are on the same share as the user
            
            // - TODO: Sharding - etag handling!
            public ShardedQueryProcessor(TransactionOperationContext context,ShardedQueriesHandler parent,IndexQueryServerSide query)
            {
                _context = context;
                _result = new ShardedQueryResult();
                _parent = parent;
                _query = query;
                _commands = new Dictionary<int, ShardedQueryCommand>();
                if (_query.QueryParameters != null && _query.QueryParameters.TryGet("Sharding.Context", out BlittableJsonReaderArray filter))
                {
                    _filteredShardIndexes = new HashSet<int>();
                    foreach (object item in filter)
                    {
                        _filteredShardIndexes.Add(_parent.ShardedContext.GetShardIndex(_context, item.ToString()));
                    }
                }
                else
                {
                    _filteredShardIndexes = null;
                }
            }

            public void Initialize()
            {
                if (_query.Metadata.IsGraph)
                {
                    // * Graph queries - not supported for sharding
                    throw new NotSupportedException("Graph queries aren't supported for sharding");
                }
                
                // now we have the index query, we need to process that and decide how to handle this.
                // There are a few different modes to handle:
                var sb = new StringBuilder();
                var queryTemplate = _query.ToJson(_context);
                if (_query.Metadata.IsCollectionQuery)
                {
                    // * For collection queries that specify ids, we can turn that into a set of loads that 
                    //   will hit the known servers

                    (List<string> ids, string _) = _query.ExtractIdsFromQuery(_parent.ServerStore, _parent.ShardedContext.DatabaseName);
                    if (ids != null)
                    {
                        var shards = ShardLocator.GroupIdsByShardIndex(ids, _parent.ShardedContext, _context);
                        sb.Clear();
                        sb.Append("from '").Append(_query.Metadata.CollectionName).AppendLine("'")
                            .AppendLine("where id() in ($list)");
                        if (_query.Metadata.Includes?.Length > 0)
                        {
                            sb.Append("include ").AppendJoin(", ", _query.Metadata.Includes).AppendLine();
                            
                        }
                        
                        foreach (var( shardId, list) in shards)
                        {
                            if(_filteredShardIndexes?.Contains(shardId) == false)
                                continue;
                            //TODO: have a way to turn the _query into a json file and then we'll modify that, instead of building it manually
                            var q = new DynamicJsonValue(queryTemplate)
                            {
                                [nameof(IndexQuery.QueryParameters)] = new DynamicJsonValue {["list"] = list}, [nameof(IndexQuery.Query)] = sb.ToString(),
                            };
                            queryTemplate.Modifications = q;
                            _commands[shardId] = new ShardedQueryCommand(_parent, _context.ReadObject(queryTemplate, "query"));
                        }
                        return;
                    }
                }

                // * For collection queries that specify startsWith by id(), we need to send to all shards
                // * For collection queries without any where clause, we need to send to all shards
                // * For indexes, we sent to all shards
                for (int i = 0; i < _parent.ShardedContext.NumberOfShardNodes; i++)
                {
                    if(_filteredShardIndexes?.Contains(i) == false)
                        continue;
                    _commands[i] = new ShardedQueryCommand(_parent, queryTemplate);
                }
            }

            public Task ExecuteShardedOperations()
            {
                var tasks = new List<Task>();

                foreach (var (shardIndex, cmd) in _commands)
                {
                    var task = _parent.ShardedContext.RequestExecutors[shardIndex].ExecuteAsync(cmd, cmd.Context);
                    tasks.Add(task);
                }

                return Task.WhenAll(tasks);
            }
            
            public ValueTask HandleIncludes()
            {
                foreach (var (_, cmd) in _commands)
                {
                    if (cmd.Result.Includes == null)
                        continue;
                    _result.Includes ??= new List<BlittableJsonReaderObject>();
                    foreach (var id in cmd.Result.Includes.GetPropertyNames())
                    {
                        if (cmd.Result.Includes.TryGet(id, out BlittableJsonReaderObject include))
                            _result.Includes.Add(include);
                    }
                }
                return ValueTask.CompletedTask;
            }

            public void OrderResults()
            {
                _result.Results = new List<BlittableJsonReaderObject>();
                long etag = _commands.Count;
                foreach (var (_, cmd) in _commands)
                {
                    etag = Hashing.HashCombiner.Combine(etag, cmd.Result.ResultEtag);
                    foreach (BlittableJsonReaderObject item in cmd.Result.Results)
                    {
                        _result.Results.Add(item);
                    }
                }
                _result.ResultEtag = etag;
            }

            public void ReduceResults()
            {
                if (_query.Metadata.IsDynamic)
                {
                    if (_query.Metadata.IsGroupBy == false)
                    {
                        return;
                    }
                    
                    // TODO: group by the fields and recompute
                }
                // check the database record to see if 
                // _query.Metadata.IndexName
                // is a map/reduce and run it
            }


            public ShardedQueryResult GetResult()
            {
                return _result;
            }
        }
    }
}
