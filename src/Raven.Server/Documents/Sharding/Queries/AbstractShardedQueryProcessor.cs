using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Queries;
using Raven.Client.Documents.Session;
using Raven.Client.Documents.Session.Loaders;
using Raven.Client.Exceptions.Sharding;
using Raven.Client.Http;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.Queries.AST;
using Raven.Server.Documents.Queries.Sharding;
using Raven.Server.Documents.Sharding.Handlers;
using Raven.Server.Documents.Sharding.Operations;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Utils;
using Voron;

namespace Raven.Server.Documents.Sharding.Queries;

public abstract class AbstractShardedQueryProcessor<TCommand, TResult, TCombinedResult> where TCommand : RavenCommand<TResult>
{
    const string LimitToken = "__raven_limit";

    protected readonly TransactionOperationContext _context;
    protected readonly ShardedDatabaseRequestHandler _requestHandler;
    protected readonly IndexQueryServerSide _query;
    protected readonly bool _metadataOnly;
    protected readonly bool _indexEntriesOnly;
    protected readonly CancellationToken _token;

    protected readonly bool _isMapReduceIndex;
    protected readonly bool _isAutoMapReduceQuery;

    private Dictionary<int, BlittableJsonReaderObject> _queryTemplates;
    private Dictionary<int, TCommand> _commands;

    protected AbstractShardedQueryProcessor(TransactionOperationContext context, ShardedDatabaseRequestHandler requestHandler, IndexQueryServerSide query, bool metadataOnly, bool indexEntriesOnly,
        CancellationToken token)
    {
        _requestHandler = requestHandler;
        _query = query;
        _metadataOnly = metadataOnly;
        _indexEntriesOnly = indexEntriesOnly;
        _token = token;
        _context = context;

        _isMapReduceIndex = _query.Metadata.IndexName != null && (_requestHandler.DatabaseContext.Indexes.GetIndex(_query.Metadata.IndexName)?.Type.IsMapReduce() ?? false);
        _isAutoMapReduceQuery = _query.Metadata.IsDynamic && _query.Metadata.IsGroupBy;
    }

    public abstract Task<TCombinedResult> ExecuteShardedOperations();

    public virtual ValueTask InitializeAsync()
    {
        AssertQueryExecution();

        // now we have the index query, we need to process that and decide how to handle this.
        // There are a few different modes to handle:
        // - For collection queries that specify startsWith by id(), we need to send to all shards
        // - For collection queries without any where clause, we need to send to all shards
        // - For indexes, we sent to all shards
        
        var queryTemplate = _query.ToJson(_context);

        if (_query.Metadata.IsCollectionQuery && _query.Metadata.DeclaredFunctions is null or { Count: 0})
        {
            // * For collection queries that specify ids, we can turn that into a set of loads that 
            //   will hit the known servers

            (List<Slice> ids, string _) = _query.ExtractIdsFromQuery(_requestHandler.ServerStore, _context.Allocator, _requestHandler.DatabaseContext.DatabaseName);

            if (ids != null)
            {
                _queryTemplates = GenerateLoadByIdQueries(ids);
            }
        }

        if (_queryTemplates == null)
        {
            RewriteQueryIfNeeded(ref queryTemplate);

            _queryTemplates = new(_requestHandler.DatabaseContext.ShardCount);

            foreach (var shardNumber in _requestHandler.DatabaseContext.ShardsTopology.Keys)
            {
                _queryTemplates.Add(shardNumber, queryTemplate);
            }
        }

        return ValueTask.CompletedTask;
    }

    private Dictionary<int, TCommand> CreateQueryCommands(Dictionary<int, BlittableJsonReaderObject> preProcessedQueries)
    {
        var commands = new Dictionary<int, TCommand>(preProcessedQueries.Count);

        foreach (var (shard, query) in preProcessedQueries)
        {
            commands.Add(shard, CreateCommand(query));
        }

        return commands;
    }

    public Dictionary<int, TCommand> GetOperationCommands()
    {
        return _commands ??= CreateQueryCommands(_queryTemplates);
    }

    protected abstract TCommand CreateCommand(BlittableJsonReaderObject query);

    protected virtual void AssertQueryExecution()
    {
        AssertUsingCustomSorters();
    }

    private void AssertUsingCustomSorters()
    {
        if (_query.Metadata.OrderBy == null)
            return;

        foreach (var field in _query.Metadata.OrderBy)
        {
            if (field.OrderingType == OrderByFieldType.Custom)
                throw new NotSupportedInShardingException("Custom sorting is not supported in sharding as of yet");
        }
    }

    private void RewriteQueryIfNeeded(ref BlittableJsonReaderObject queryTemplate)
    {
        var rewriteForPaging = _query.Offset is > 0;
        var rewriteForProjection = true;

        var query = _query.Metadata.Query;
        if (query.Select?.Count > 0 == false &&
            query.SelectFunctionBody.FunctionText == null)
        {
            rewriteForProjection = false;
        }

        if (_query.Metadata.IndexName == null || _isMapReduceIndex == false)
        {
            rewriteForProjection = false;
        }

        if (rewriteForPaging == false && rewriteForProjection == false)
            return;

        var clone = _query.Metadata.Query.ShallowCopy();

        DynamicJsonValue modifications = new(queryTemplate);

        if (rewriteForPaging)
        {
            // For paging queries, we modify the limits on the query to include all the results from all
            // shards if there is an offset. But if there isn't an offset, we can just get the limit from
            // each node and then merge them

            clone.Offset = null; // sharded queries has to start from 0 on all nodes
            clone.Limit = new ValueExpression(LimitToken, ValueTokenType.Parameter);

            DynamicJsonValue modifiedArgs;
            if (queryTemplate.TryGet(nameof(IndexQuery.QueryParameters), out BlittableJsonReaderObject args))
            {
                modifiedArgs = new DynamicJsonValue(args);
                args.Modifications = modifiedArgs;
            }
            else
            {
                modifications[nameof(IndexQuery.QueryParameters)] = modifiedArgs = new DynamicJsonValue();
            }

            var limit = ((_query.Limit ?? 0) + (_query.Offset ?? 0)) * (long)_requestHandler.DatabaseContext.ShardCount;

            if (limit > int.MaxValue) // overflow
                limit = int.MaxValue;

            modifiedArgs[LimitToken] = limit;

            modifications.Remove(nameof(IndexQueryServerSide.Start));
            modifications.Remove(nameof(IndexQueryServerSide.PageSize));
        }

        if (rewriteForProjection)
        {
            // If we have a projection in a map-reduce index,
            // the shards will send the query result and the orchestrator will re-reduce and apply the projection
            // in that case we must send the query without the projection

            if (query.Load is { Count: > 0 })
            {
                DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Grisha, DevelopmentHelper.Severity.Normal, "https://issues.hibernatingrhinos.com/issue/RavenDB-17887");
                throw new NotSupportedInShardingException("Loading a document inside a projection from a map-reduce index isn't supported");
            }

            clone.Select = null;
            clone.SelectFunctionBody = default;
            clone.DeclaredFunctions = null;
        }

        modifications[nameof(IndexQuery.Query)] = clone.ToString();

        queryTemplate.Modifications = modifications;

        queryTemplate = _context.ReadObject(queryTemplate, "modified-query");
    }

    private Dictionary<int, BlittableJsonReaderObject> GenerateLoadByIdQueries(IEnumerable<Slice> ids)
    {
        const string listParameterName = "p0";

        var documentQuery = new DocumentQuery<dynamic>(null, null, _query.Metadata.CollectionName, isGroupBy: false, fromAlias: _query.Metadata.Query.From.Alias?.ToString());
        documentQuery.WhereIn(Constants.Documents.Indexing.Fields.DocumentIdFieldName, Enumerable.Empty<object>());
        
        IncludeBuilder includeBuilder = null;

        if (_query.Metadata.Includes is { Length: > 0 })
        {
            includeBuilder = new IncludeBuilder
            {
                DocumentsToInclude = new HashSet<string>(_query.Metadata.Includes)
            };
        }

        if (_query.Metadata.RevisionIncludes != null)
        {
            includeBuilder ??= new IncludeBuilder();

            includeBuilder.RevisionsToIncludeByChangeVector = _query.Metadata.RevisionIncludes.RevisionsChangeVectorsPaths;
            includeBuilder.RevisionsToIncludeByDateTime = _query.Metadata.RevisionIncludes.RevisionsBeforeDateTime;
        }

        if (_query.Metadata.TimeSeriesIncludes != null)
        {
            includeBuilder ??= new IncludeBuilder();

            includeBuilder.TimeSeriesToIncludeBySourceAlias = _query.Metadata.TimeSeriesIncludes.TimeSeries;
        }

        if (_query.Metadata.CounterIncludes != null)
        {
            includeBuilder ??= new IncludeBuilder();

            includeBuilder.CountersToIncludeBySourcePath = new Dictionary<string, (bool AllCounters, HashSet<string> CountersToInclude)>(StringComparer.OrdinalIgnoreCase);

            foreach (var counterIncludes in _query.Metadata.CounterIncludes.Counters)
            {
                var name = counterIncludes.Key;

                if (includeBuilder.CountersToIncludeBySourcePath.TryGetValue(name, out var counters) == false)
                    includeBuilder.CountersToIncludeBySourcePath[name] = counters = (false, new HashSet<string>(StringComparer.OrdinalIgnoreCase));

                foreach (string counterName in counterIncludes.Value)
                {
                    counters.CountersToInclude.Add(counterName);
                }
            }
        }

        if (includeBuilder != null)
            documentQuery.Include(includeBuilder);

        var queryText = documentQuery.ToString();

        if (_query.Metadata.Query.Select is { Count: > 0 })
        {
            var selectStartPosition = _query.Metadata.QueryText.IndexOf("select", StringComparison.OrdinalIgnoreCase);

            var selectClause = _query.Metadata.QueryText.Substring(selectStartPosition);

            queryText += $" {selectClause}";
        }
        
        Dictionary<int, BlittableJsonReaderObject> queryTemplates = new();

        var shards = ShardLocator.GetDocumentIdsByShards(_context, _requestHandler.DatabaseContext, ids);

        foreach ((int shardId, ShardLocator.IdsByShard<Slice> documentIds) in shards)
        {
            DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Grisha, DevelopmentHelper.Severity.Normal, "RavenDB-19084 have a way to turn the _query into a json file and then we'll modify that, instead of building it manually");

            var q = new DynamicJsonValue
            {
                [nameof(IndexQuery.QueryParameters)] = new DynamicJsonValue
                {
                    [listParameterName] = GetIds()
                },
                [nameof(IndexQuery.Query)] = queryText
            };

            queryTemplates[shardId] = _context.ReadObject(q, "query");

            IEnumerable<string> GetIds()
            {
                foreach (var idAsAlice in documentIds.Ids)
                {
                    yield return idAsAlice.ToString();
                }
            }
        }

        return queryTemplates;
    }

    protected async Task HandleMissingDocumentIncludes<T, TIncludes>(HashSet<string> missingIncludes, QueryResult<List<T>, List<TIncludes>> result)
    {
        var missingIncludeIdsByShard = ShardLocator.GetDocumentIdsByShards(_context, _requestHandler.DatabaseContext, missingIncludes);
        var missingIncludesOp = new FetchDocumentsFromShardsOperation(_context, _requestHandler, missingIncludeIdsByShard, null, null, counterIncludes: default, null, null, null, _metadataOnly);
        var missingResult = await _requestHandler.DatabaseContext.ShardExecutor.ExecuteParallelForShardsAsync(missingIncludeIdsByShard.Keys.ToArray(), missingIncludesOp, _token);

        var blittableIncludes = result.Includes as List<BlittableJsonReaderObject>;
        var documentIncludes = result.Includes as List<Document>;

        foreach (var (_, missing) in missingResult.Result.Documents)
        {
            if (missing == null)
                continue;

            if (blittableIncludes != null)
                blittableIncludes.Add(missing);
            else if (documentIncludes != null)
            {
                if (missing.TryGet(Constants.Documents.Metadata.Key, out BlittableJsonReaderObject metadata))
                {
                    if (metadata.TryGet(Constants.Documents.Metadata.Id, out LazyStringValue id))
                    {
                        documentIncludes.Add(new Document
                        {
                            Id = id,
                            Data = missing
                        });
                    }
                }
            }
            else
                throw new NotSupportedException($"Unknown includes type: {result.Includes.GetType().FullName}");
        }
    }

    protected async Task WaitForRaftIndexIfNeededAsync(long? raftCommandIndex)
    {
        if (_isAutoMapReduceQuery && raftCommandIndex.HasValue)
        {
            // we are waiting here for all nodes, we should wait for all of the orchestrators at least to apply that
            // so further queries would not throw index does not exist in case of a failover
            await _requestHandler.DatabaseContext.Cluster.WaitForExecutionOnAllNodesAsync(raftCommandIndex.Value, _token);
        }
    }
}
