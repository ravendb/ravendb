using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Queries;
using Raven.Client.Documents.Session;
using Raven.Client.Exceptions.Sharding;
using Raven.Client.Http;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.Queries.AST;
using Raven.Server.Documents.Sharding.Handlers;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Utils;
using Voron;

namespace Raven.Server.Documents.Sharding.Queries;

public abstract class AbstractShardedQueryProcessor<TCommand, TResult, TCombinedResult> : IDisposable where TCommand : RavenCommand<TResult>
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

    protected Dictionary<int, TCommand> _commands;

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

    public void Initialize()
    {
        AssertQueryExecution();

        // now we have the index query, we need to process that and decide how to handle this.
        // There are a few different modes to handle:
        var queryTemplate = _query.ToJson(_context);

        Dictionary<int, BlittableJsonReaderObject> queryTemplates = null;

        if (_query.Metadata.IsCollectionQuery)
        {
            // * For collection queries that specify ids, we can turn that into a set of loads that 
            //   will hit the known servers

            (List<Slice> ids, string _) = _query.ExtractIdsFromQuery(_requestHandler.ServerStore, _context.Allocator, _requestHandler.DatabaseContext.DatabaseName);

            if (ids != null)
            {
                queryTemplates = GenerateLoadByIdQueries(ids);
            }
        }

        if (queryTemplates == null)
        {
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

            queryTemplates = new(_requestHandler.DatabaseContext.ShardCount);

            for (int i = 0; i < _requestHandler.DatabaseContext.ShardCount; i++)
            {
                queryTemplates.Add(i, queryTemplate);
            }
        }

        // * For collection queries that specify startsWith by id(), we need to send to all shards
        // * For collection queries without any where clause, we need to send to all shards
        // * For indexes, we sent to all shards
        _commands = CreateQueryCommands(queryTemplates);
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

    private void RewriteQueryForPaging(ref BlittableJsonReaderObject queryTemplate)
    {
        if (_query.Offset is null or <= 0)
            return;

        var clone = _query.Metadata.Query.ShallowCopy();
        
        clone.Offset = null; // sharded queries has to start from 0 on all nodes
        clone.Limit = new ValueExpression(LimitToken, ValueTokenType.Parameter);
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

        var limit = ((_query.Limit ?? 0) + (_query.Offset ?? 0)) * (long)_requestHandler.DatabaseContext.ShardCount;

        if (limit > int.MaxValue) // overflow
            limit = int.MaxValue;

        modifiedArgs[LimitToken] = limit;

        queryTemplate.Modifications.Remove(nameof(IndexQueryServerSide.Start));
        queryTemplate.Modifications.Remove(nameof(IndexQueryServerSide.PageSize));

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

    private Dictionary<int, BlittableJsonReaderObject> GenerateLoadByIdQueries(IEnumerable<Slice> ids)
    {
        const string listParameterName = "p0";

        var documentQuery = new DocumentQuery<dynamic>(null, null, _query.Metadata.CollectionName, false);
        documentQuery.WhereIn(Constants.Documents.Indexing.Fields.DocumentIdFieldName, new List<object>());

        if (_query.Metadata.Includes is { Length: > 0 })
        {
            foreach (var include in _query.Metadata.Includes)
            {
                documentQuery.Include(include);
            }
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
                [nameof(IndexQuery.Query)] = documentQuery.ToString()
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

    public abstract void Dispose();
}
