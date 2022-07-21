using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Queries;
using Raven.Client.Documents.Session;
using Raven.Client.Exceptions.Sharding;
using Raven.Server.Documents.Includes;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.Queries.AST;
using Raven.Server.Documents.Queries.Results;
using Raven.Server.Documents.Queries.Results.Sharding;
using Raven.Server.Documents.Queries.Sharding;
using Raven.Server.Documents.Replication.Senders;
using Raven.Server.Documents.Sharding.Commands;
using Raven.Server.Documents.Sharding.Handlers;
using Raven.Server.ServerWide.Context;
using Sparrow;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Utils;
using Voron;

namespace Raven.Server.Documents.Sharding.Queries;

/// <summary>
/// A struct that we use to hold state and break the process
/// of handling a sharded query into distinct steps
/// </summary>
public class ShardedQueryProcessor : IDisposable
{
    private readonly TransactionOperationContext _context;
    private readonly ShardedQueriesHandler _parent;
    private readonly IndexQueryServerSide _query;
    private readonly bool _isMapReduceIndex;
    private readonly bool _isAutoMapReduceQuery;
    private readonly Dictionary<int, ShardedQueryCommand> _commands;
    //private readonly HashSet<int> _filteredShardIndexes;
    private readonly ShardedQueryResult _result;
    private readonly List<IDisposable> _disposables = new();

    private IncludeCompareExchangeValuesCommand _includeCompareExchangeValues;

    // User should also be able to define a query parameter ("Sharding.Context") which is an array
    // that contains the ids whose shards the query should be limited to. Advanced: Optimization if user
    // want to run a query and knows what shards it is on. Such as:
    // from Orders where State = $state and User = $user where all the orders are on the same share as the user

    public ShardedQueryProcessor(TransactionOperationContext context, ShardedQueriesHandler parent, IndexQueryServerSide query)
    {
        DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Grisha, DevelopmentHelper.Severity.Normal, "RavenDB-19084 Etag handling");

        _context = context;
        _result = new ShardedQueryResult();
        _parent = parent;
        _query = query;
        _isMapReduceIndex = _query.Metadata.IndexName != null && (_parent.DatabaseContext.Indexes.GetIndex(_query.Metadata.IndexName)?.Type.IsMapReduce() ?? false);
        _isAutoMapReduceQuery = _query.Metadata.IsDynamic && _query.Metadata.IsGroupBy;
        _commands = new Dictionary<int, ShardedQueryCommand>();

        DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Grisha, DevelopmentHelper.Severity.Normal, "RavenDB-19084 Add an option to select the shards for query in the client");
        //if (_query.QueryParameters != null && _query.QueryParameters.TryGet("@sharding-context", out BlittableJsonReaderArray filter))
        //{
        //    _filteredShardIndexes = new HashSet<int>();
        //    foreach (object item in filter)
        //    {
        //        _filteredShardIndexes.Add(_parent.ShardedDatabaseContext.GetShardIndex(_context, item.ToString()));
        //    }
        //}
        //else
        //{
        //    _filteredShardIndexes = null;
        //}
    }

    public long ResultsEtag => _result.ResultEtag;

    public void Initialize()
    {
        AssertUsingCustomSorters();

        // now we have the index query, we need to process that and decide how to handle this.
        // There are a few different modes to handle:
        var queryTemplate = _query.ToJson(_context);
        if (_query.Metadata.IsCollectionQuery)
        {
            // * For collection queries that specify ids, we can turn that into a set of loads that 
            //   will hit the known servers

            (List<Slice> ids, string _) = _query.ExtractIdsFromQuery(_parent.ServerStore, _context.Allocator, _parent.DatabaseContext.DatabaseName);
            if (ids != null)
            {
                GenerateLoadByIdQueries(ids, _commands, _context);
                return;
            }
        }

        DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Grisha, DevelopmentHelper.Severity.Normal,
            "RavenDB-19084 Use a single rewrite method in order to avoid cloning the query twice");

        // * For paging queries, we modify the limits on the query to include all the results from all
        //   shards if there is an offset. But if there isn't an offset, we can just get the limit from
        //   each node and then merge them
        RewriteQueryForPaging(ref queryTemplate);

        // * If we have a projection in a map-reduce index,
        //   the shards will send the query result and the orchestrator will re-reduce and apply the projection
        //   in that case we must send the query without the projection
        RewriteQueryForProjection(ref queryTemplate);

        // * For collection queries that specify startsWith by id(), we need to send to all shards
        // * For collection queries without any where clause, we need to send to all shards
        // * For indexes, we sent to all shards
        for (int i = 0; i < _parent.DatabaseContext.ShardCount; i++)
        {
            //if (_filteredShardIndexes?.Contains(i) == false)
            //    continue;

            DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Grisha, DevelopmentHelper.Severity.Normal, "RavenDB-19084 Use ShardedExecutor");
            _commands[i] = new ShardedQueryCommand(_parent, queryTemplate, _query.Metadata.IndexName);
        }
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

    private void RewriteQueryForPaging(ref BlittableJsonReaderObject queryTemplate)
    {
        if (_query.Offset is null or <= 0)
            return;

        const string limitToken = "__raven_limit";
        var clone = _query.Metadata.Query.ShallowCopy();
        clone.Offset = null; // sharded queries has to start from 0 on all nodes
        clone.Limit = new ValueExpression(limitToken, ValueTokenType.Parameter);
        queryTemplate.Modifications = new DynamicJsonValue(queryTemplate)
        {
            [nameof(IndexQuery.Query)] = clone.ToString()
        };
        DynamicJsonValue modifiedArgs;
        if (queryTemplate.TryGet(nameof(IndexQuery.QueryParameters), out BlittableJsonReaderObject args))
        {
            modifiedArgs = new DynamicJsonValue(args);
            args.Modifications = modifiedArgs;
        }
        else
        {
            queryTemplate.Modifications[nameof(IndexQuery.QueryParameters)] = modifiedArgs = new DynamicJsonValue();
        }

        var limit = ((_query.Limit ?? 0) + (_query.Offset ?? 0)) * (long)_parent.DatabaseContext.ShardCount;
        if (limit > int.MaxValue) // overflow
            limit = int.MaxValue;
        modifiedArgs[limitToken] = limit;

        queryTemplate = _context.ReadObject(queryTemplate, "modified-query");
    }

    private void RewriteQueryForProjection(ref BlittableJsonReaderObject queryTemplate)
    {
        var query = _query.Metadata.Query;
        if (query.Select?.Count > 0 == false &&
            query.SelectFunctionBody.FunctionText == null)
            return;

        if (_query.Metadata.IndexName == null || _isMapReduceIndex == false)
            return;

        if (query.Load is { Count: > 0 })
        {
            DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Grisha, DevelopmentHelper.Severity.Normal, "https://issues.hibernatingrhinos.com/issue/RavenDB-17887");
            throw new NotSupportedInShardingException("Loading a document inside a projection from a map-reduce index isn't supported");
        }

        var clone = query.ShallowCopy();
        clone.Select = null;
        clone.SelectFunctionBody = default;
        clone.DeclaredFunctions = null;

        queryTemplate.Modifications = new DynamicJsonValue(queryTemplate)
        {
            [nameof(IndexQuery.Query)] = clone.ToString()
        };

        queryTemplate = _context.ReadObject(queryTemplate, "modified-query");
    }

    private void GenerateLoadByIdQueries(IEnumerable<Slice> ids, Dictionary<int, ShardedQueryCommand> cmds, TransactionOperationContext context)
    {
        const string listParameterName = "p0";

        var shards = ShardLocator.GetDocumentIdsByShards(context, _parent.DatabaseContext, ids);
        var sb = new StringBuilder();
        sb.Append("from '").Append(_query.Metadata.CollectionName).AppendLine("'")
            .AppendLine($"where id() in (${listParameterName})");

        if (_query.Metadata.Includes?.Length > 0)
        {
            sb.Append("include ").AppendJoin(", ", _query.Metadata.Includes).AppendLine();
        }

        DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Grisha, DevelopmentHelper.Severity.Minor, "RavenDB-19084 use DocumentQuery from Client API to build the query");

        var query = sb.ToString();

        foreach ((int shardId, ShardLocator.IdsByShard<Slice> documentIds) in shards)
        {
            //if (_filteredShardIndexes?.Contains(shardId) == false)
            //    continue;

            DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Grisha, DevelopmentHelper.Severity.Normal, "RavenDB-19084 have a way to turn the _query into a json file and then we'll modify that, instead of building it manually");

            var q = new DynamicJsonValue
            {
                [nameof(IndexQuery.QueryParameters)] = new DynamicJsonValue
                {
                    [listParameterName] = documentIds
                },
                [nameof(IndexQuery.Query)] = query
            };
            cmds[shardId] = new ShardedQueryCommand(_parent, _context.ReadObject(q, "query"), null);
        }
    }

    public async Task ExecuteShardedOperations()
    {
        var tasks = new List<Task>();

        foreach (var (shardNumber, cmd) in _commands)
        {
            DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Grisha, DevelopmentHelper.Severity.Normal, "RavenDB-19084 Use ShardedExecutor");

            _disposables.Add(_parent.ContextPool.AllocateOperationContext(out TransactionOperationContext context));
            var task = _parent.DatabaseContext.ShardExecutor.GetRequestExecutorAt(shardNumber).ExecuteAsync(cmd, context);
            tasks.Add(task);
        }

        await Task.WhenAll(tasks);

        long? index = null;
        foreach (var (_, cmd) in _commands)
        {
            _result.ResultEtag = Hashing.Combine(_result.ResultEtag, cmd.Result.ResultEtag);

            if (cmd.Result.RaftCommandIndex.HasValue)
            {
                if (index == null || cmd.Result.RaftCommandIndex > index)
                    index = cmd.Result.RaftCommandIndex;
            }
        }

        if (_query.Metadata.HasCmpXchgIncludes)
        {
            _includeCompareExchangeValues = IncludeCompareExchangeValuesCommand.ExternalScope(_parent.DatabaseContext, _query.Metadata.CompareExchangeValueIncludes);
            _disposables.Add(_includeCompareExchangeValues);

            _result.AddCompareExchangeValueIncludes(_includeCompareExchangeValues);
        }

        if (_isAutoMapReduceQuery && index.HasValue)
        {
            // we are waiting here for all nodes, we should wait for all of the orchestrators at least to apply that
            // so further queries would not throw index does not exist in case of a failover
            await _parent.DatabaseContext.Cluster.WaitForExecutionOnAllNodesAsync(index.Value);
        }
    }

    public ValueTask HandleIncludes()
    {
        HashSet<string> missing = null;
        foreach (var (_, cmd) in _commands)
        {
            if (cmd.Result.Includes is { Count: > 0 })
            {
                _result.Includes ??= new List<BlittableJsonReaderObject>();
                foreach (var id in cmd.Result.Includes.GetPropertyNames())
                {
                    if (cmd.Result.Includes.TryGet(id, out BlittableJsonReaderObject include) && include != null)
                    {
                        _result.Includes.Add(include);
                    }
                    else
                    {
                        (missing ??= new HashSet<string>()).Add(id);
                    }
                }
            }

            if (_includeCompareExchangeValues != null && cmd.Result.CompareExchangeValueIncludes != null)
                _includeCompareExchangeValues.AddResults(cmd.Result.CompareExchangeValueIncludes);
        }

        if (missing == null || missing.Count == 0)
        {
            return ValueTask.CompletedTask;
        }

        return HandleMissingIncludesAsync(missing);
    }

    private async ValueTask HandleMissingIncludesAsync(HashSet<string> missing)
    {
        var includeCommands = new Dictionary<int, ShardedQueryCommand>();

        const string listParameterName = "p0";

        var shards = ShardLocator.GetDocumentIdsByShards(_context, _parent.DatabaseContext, missing);

        var documentQuery = new DocumentQuery<dynamic>(null, null, "@all_docs", false);
        documentQuery.WhereIn("id()", new List<object>());
        var query = documentQuery.ToString();

        foreach ((int shardId, ShardLocator.IdsByShard<string> documentIds) in shards)
        {
            //if (_filteredShardIndexes?.Contains(shardId) == false)
            //    continue;

            DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Grisha, DevelopmentHelper.Severity.Normal, "have a way to turn the _query into a json file and then we'll modify that, instead of building it manually");

            var queryTemplate = new DynamicJsonValue
            {
                [nameof(IndexQuery.QueryParameters)] = new DynamicJsonValue
                {
                    [listParameterName] = documentIds.Ids
                },
                [nameof(IndexQuery.Query)] = query
            };

            includeCommands[shardId] = new ShardedQueryCommand(_parent, _context.ReadObject(queryTemplate, "query"), null);
        }

        var tasks = new List<Task>();
        foreach (var (shardNumber, cmd) in includeCommands)
        {
            DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Grisha, DevelopmentHelper.Severity.Normal, "RavenDB-19084 Use ShardedExecutor");
            _disposables.Add(_parent.ContextPool.AllocateOperationContext(out TransactionOperationContext context));
            tasks.Add(_parent.DatabaseContext.ShardExecutor.GetRequestExecutorAt(shardNumber).ExecuteAsync(cmd, context));
        }

        await Task.WhenAll(tasks);
        foreach (var (_, cmd) in includeCommands)
        {
            foreach (BlittableJsonReaderObject result in cmd.Result.Results)
            {
                _result.Includes.Add(result);
            }
        }
    }

    public void ApplyPaging()
    {
        if (_query.Offset > 0)
        {
            _result.Results.RemoveRange(0, _query.Offset ?? 0);
            if (_query.Limit != null && _result.Results.Count > _query.Limit)
            {
                _result.Results.RemoveRange(_query.Limit.Value, _result.Results.Count - _query.Limit.Value);
            }
        }
    }

    public void MergeResults()
    {
        DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Grisha, DevelopmentHelper.Severity.Normal, "RavenDB-19084 Use IShardedOperation");

        _result.Results = new List<BlittableJsonReaderObject>();

        // sorting only if:
        // 1. we have fields to sort
        // 2. it isn't a map-reduce index/query (the sorting will be done after the re-reduce)
        var sortResults = _query.Metadata.OrderBy?.Length > 0 && (_isMapReduceIndex || _isAutoMapReduceQuery) == false;

        foreach (var (_, cmd) in _commands)
        {
            _result.TotalResults += cmd.Result.TotalResults;
            _result.IsStale |= cmd.Result.IsStale;
            _result.SkippedResults += cmd.Result.SkippedResults;
            _result.IndexName = cmd.Result.IndexName;
            _result.IncludedPaths = cmd.Result.IncludedPaths;
            if (_result.IndexTimestamp < cmd.Result.IndexTimestamp)
            {
                _result.IndexTimestamp = cmd.Result.IndexTimestamp;
            }

            if (_result.LastQueryTime < cmd.Result.LastQueryTime)
            {
                _result.LastQueryTime = cmd.Result.LastQueryTime;
            }

            if (sortResults == false)
            {
                foreach (BlittableJsonReaderObject item in cmd.Result.Results)
                {
                    _result.Results.Add(item);
                }
            }
        }

        if (sortResults == false)
            return;

        // all the results from each command are already ordered
        var documentsComparer = new ShardedDocumentsComparer(_query.Metadata, _isMapReduceIndex || _isAutoMapReduceQuery);
        using (var mergedEnumerator = new MergedEnumerator<BlittableJsonReaderObject>(documentsComparer))
        {
            foreach (var command in _commands)
            {
                mergedEnumerator.AddEnumerator(GetEnumerator(command.Value.Result.Results));
            }

            static IEnumerator<BlittableJsonReaderObject> GetEnumerator(BlittableJsonReaderArray array)
            {
                foreach (BlittableJsonReaderObject item in array)
                {
                    yield return item;
                }
            }

            while (mergedEnumerator.MoveNext())
            {
                _result.Results.Add(mergedEnumerator.Current);
            }
        }
    }

    public void ReduceResults()
    {
        if (_isMapReduceIndex || _isAutoMapReduceQuery)
        {
            var merger = new ShardedMapReduceQueryResultsMerger(_result.Results, _parent.DatabaseContext.Indexes, _result.IndexName, _isAutoMapReduceQuery, _context);
            _result.Results = merger.Merge();

            if (_query.Metadata.OrderBy?.Length > 0 && (_isMapReduceIndex || _isAutoMapReduceQuery))
            {
                // apply ordering after the re-reduce of a map-reduce index
                _result.Results.Sort(new ShardedDocumentsComparer(_query.Metadata, isMapReduce: true));
            }
        }
    }

    public void ProjectAfterMapReduce()
    {
        if (_isMapReduceIndex == false && _isAutoMapReduceQuery == false
            || (_query.Metadata.Query.Select == null || _query.Metadata.Query.Select.Count == 0)
            && _query.Metadata.Query.SelectFunctionBody.FunctionText == null)
            return;

        var fieldsToFetch = new FieldsToFetch(_query, null);
        var retriever = new ShardedMapReduceResultRetriever(_parent.DatabaseContext.Indexes.ScriptRunnerCache, _query, null, SearchEngineType.Lucene, fieldsToFetch, null, _context, false, null, null, null,
            _parent.DatabaseContext.IdentityPartsSeparator);

        var currentResults = _result.Results;
        _result.Results = new List<BlittableJsonReaderObject>();

        foreach (var data in currentResults)
        {
            var retrieverInput = new RetrieverInput();
            (Document document, List<Document> documents) = retriever.GetProjectionFromDocument(new Document
            {
                Data = data
            }, ref retrieverInput, fieldsToFetch, _context, CancellationToken.None);

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
                var result = _context.ReadObject(documentData, "modified-map-reduce-result");
                _result.Results.Add(result);
            }
        }
    }

    public ShardedQueryResult GetResult()
    {
        return _result;
    }

    public void Dispose()
    {
        foreach (var toDispose in _disposables)
        {
            toDispose.Dispose();
        }
    }
}
