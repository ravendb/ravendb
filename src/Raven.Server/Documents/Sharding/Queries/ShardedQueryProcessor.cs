using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Queries;
using Raven.Server.Documents.Includes;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.Queries.Results;
using Raven.Server.Documents.Queries.Results.Sharding;
using Raven.Server.Documents.Queries.Sharding;
using Raven.Server.Documents.Sharding.Commands;
using Raven.Server.Documents.Sharding.Handlers;
using Raven.Server.Documents.Sharding.Operations;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Utils;

namespace Raven.Server.Documents.Sharding.Queries;

/// <summary>
/// A struct that we use to hold state and break the process
/// of handling a sharded query into distinct steps
/// </summary>
public class ShardedQueryProcessor : AbstractShardedQueryProcessor<ShardedQueryCommand, QueryResult, ShardedQueryResult>
{
    private readonly long? _existingResultEtag;

    //private readonly HashSet<int> _filteredShardIndexes;
    private readonly List<IDisposable> _disposables = new();

    private IncludeCompareExchangeValuesCommand _includeCompareExchangeValues;

    // User should also be able to define a query parameter ("Sharding.Context") which is an array
    // that contains the ids whose shards the query should be limited to. Advanced: Optimization if user
    // want to run a query and knows what shards it is on. Such as:
    // from Orders where State = $state and User = $user where all the orders are on the same share as the user

    public ShardedQueryProcessor(TransactionOperationContext context, ShardedDatabaseRequestHandler requestHandler, IndexQueryServerSide query, long? existingResultEtag, bool metadataOnly, bool indexEntriesOnly,
        CancellationToken token) : base(context, requestHandler, query, metadataOnly, indexEntriesOnly, token)
    {
        _existingResultEtag = existingResultEtag;
        DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Grisha, DevelopmentHelper.Severity.Normal, "RavenDB-19084 Etag handling");

        DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Grisha, DevelopmentHelper.Severity.Normal, "RavenDB-19084 Add an option to select the shards for query in the client");
        
        //if (_query.QueryParameters != null && _query.QueryParameters.TryGet("@sharding-context", out BlittableJsonReaderArray filter))
        //{
        //    _filteredShardIndexes = new HashSet<int>();
        //    foreach (object item in filter)
        //    {
        //        _filteredShardIndexes.Add(_requestHandler.ShardedDatabaseContext.GetShardIndex(_context, item.ToString()));
        //    }
        //}
        //else
        //{
        //    _filteredShardIndexes = null;
        //}
    }

    protected override ShardedQueryCommand CreateCommand(BlittableJsonReaderObject query)
    {
        return new ShardedQueryCommand(_context.ReadObject(query, "query"), _query, _metadataOnly, _indexEntriesOnly, _query.Metadata.IndexName);
    }

    public override async Task<ShardedQueryResult> ExecuteShardedOperations()
    {
        if (_query.Metadata.HasCmpXchgIncludes)
        {
            _includeCompareExchangeValues = IncludeCompareExchangeValuesCommand.ExternalScope(_requestHandler.DatabaseContext, _query.Metadata.CompareExchangeValueIncludes);
            _disposables.Add(_includeCompareExchangeValues);
        }
        
        ShardedDocumentsComparer documentsComparer = null;

        if (_query.Metadata.OrderBy?.Length > 0 && (_isMapReduceIndex || _isAutoMapReduceQuery) == false)
        {
            // sorting only if:
            // 1. we have fields to sort
            // 2. it isn't a map-reduce index/query (the sorting will be done after the re-reduce)
            documentsComparer = new ShardedDocumentsComparer(_query.Metadata, _isMapReduceIndex || _isAutoMapReduceQuery);
        }

        var operation = new ShardedQueryOperation(_context, _requestHandler, _commands, _includeCompareExchangeValues, documentsComparer, _existingResultEtag?.ToString());

        var shardedReadResult = await _requestHandler.ShardExecutor.ExecuteParallelForShardsAsync(_commands.Keys.ToArray(), operation, _token);

        if (shardedReadResult.StatusCode == (int)HttpStatusCode.NotModified)
        {
            return new ShardedQueryResult { NotModified = true };
        }

        var result = shardedReadResult.Result;

        if (_isAutoMapReduceQuery && result.RaftCommandIndex.HasValue)
        {
            // we are waiting here for all nodes, we should wait for all of the orchestrators at least to apply that
            // so further queries would not throw index does not exist in case of a failover
            await _requestHandler.DatabaseContext.Cluster.WaitForExecutionOnAllNodesAsync(result.RaftCommandIndex.Value);
        }

        if (operation.MissingIncludes is {Count: > 0})
        {
            await HandleMissingIncludes(operation.MissingIncludes, result);
        }

        // For map/reduce - we need to re-run the reduce portion of the index again on the results
        ReduceResults(ref result);

        ApplyPaging(ref result);

        // For map-reduce indexes we project the results after the reduce part 
        ProjectAfterMapReduce(ref result);

        // * For JS projections and load clauses, we don't support calling load() on a
        //   document that is not on the same shard
        DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Grisha, DevelopmentHelper.Severity.Normal, "RavenDB-17889 Add a test for that");

        return result;
    }

    private async Task HandleMissingIncludes(HashSet<string> missingIncludes, ShardedQueryResult result)
    {
        var missingIncludeIdsByShard = ShardLocator.GetDocumentIdsByShards(_context, _requestHandler.DatabaseContext, missingIncludes);
        var missingIncludesOp = new FetchDocumentsFromShardsOperation(_context, _requestHandler, missingIncludeIdsByShard, null, null, null, _metadataOnly);
        var missingResult = await _requestHandler.DatabaseContext.ShardExecutor.ExecuteParallelForShardsAsync(missingIncludeIdsByShard.Keys.ToArray(), missingIncludesOp, _token);

        foreach (var (_, missing) in missingResult.Result.Documents)
        {
            if (missing == null)
                continue;

            result.Includes.Add(missing);
        }
    }

    private void ApplyPaging(ref ShardedQueryResult result)
    {
        if (_query.Offset is > 0 && result.Results.Count > _query.Offset)
        {
            result.Results.RemoveRange(0, _query.Offset ?? 0);
        }

        if (_query.Limit is > 0 && result.Results.Count > _query.Limit)
        {
            result.Results.RemoveRange(_query.Limit.Value, result.Results.Count - _query.Limit.Value);
        }
    }

    private void ReduceResults(ref ShardedQueryResult result)
    {
        if (_isMapReduceIndex || _isAutoMapReduceQuery)
        {
            var merger = new ShardedMapReduceQueryResultsMerger(result.Results, _requestHandler.DatabaseContext.Indexes, result.IndexName, _isAutoMapReduceQuery, _context);
            result.Results = merger.Merge();

            if (_query.Metadata.OrderBy?.Length > 0 && (_isMapReduceIndex || _isAutoMapReduceQuery))
            {
                // apply ordering after the re-reduce of a map-reduce index
                result.Results.Sort(new ShardedDocumentsComparer(_query.Metadata, isMapReduce: true));
            }
        }
    }

    private void ProjectAfterMapReduce(ref ShardedQueryResult result)
    {
        if (_isMapReduceIndex == false && _isAutoMapReduceQuery == false
            || (_query.Metadata.Query.Select == null || _query.Metadata.Query.Select.Count == 0)
            && _query.Metadata.Query.SelectFunctionBody.FunctionText == null)
            return;

        var fieldsToFetch = new FieldsToFetch(_query, null);
        var retriever = new ShardedMapReduceResultRetriever(_requestHandler.DatabaseContext.Indexes.ScriptRunnerCache, _query, null, SearchEngineType.Lucene, fieldsToFetch, null, _context, false, null, null, null,
            _requestHandler.DatabaseContext.IdentityPartsSeparator);

        var currentResults = result.Results;
        result.Results = new List<BlittableJsonReaderObject>();

        foreach (var data in currentResults)
        {
            var retrieverInput = new RetrieverInput();
            (Document document, List<Document> documents) = retriever.GetProjectionFromDocument(new Document
            {
                Data = data
            }, ref retrieverInput, fieldsToFetch, _context, CancellationToken.None);
            
            var results = result.Results;

            if (document != null)
            {
                AddDocument(document.Data);
            }
            else if (documents != null)
            {
                foreach (var doc in documents)
                {
                    AddDocument(doc.Data);
                }
            }

            void AddDocument(BlittableJsonReaderObject documentData)
            {
                var doc = _context.ReadObject(documentData, "modified-map-reduce-result");
                results.Add(doc);
            }
        }
    }

    public override void Dispose()
    {
        foreach (var toDispose in _disposables)
        {
            toDispose.Dispose();
        }
    }
}
