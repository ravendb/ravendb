using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Raven.Client;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Queries;
using Raven.Client.Documents.Session;
using Raven.Client.Documents.Session.Loaders;
using Raven.Client.Exceptions.Documents.Indexes;
using Raven.Client.Exceptions.Sharding;
using Raven.Client.Extensions;
using Raven.Client.Http;
using Raven.Client.Util;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.Indexes.MapReduce.Auto;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.Queries.AST;
using Raven.Server.Documents.Queries.Timings;
using Raven.Server.Documents.Sharding.Commands.Querying;
using Raven.Server.Documents.Sharding.Comparers;
using Raven.Server.Documents.Sharding.Handlers;
using Raven.Server.Documents.Sharding.Operations;
using Raven.Server.ServerWide.Context;
using Sparrow;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Utils;
using Voron;

namespace Raven.Server.Documents.Sharding.Queries;

public abstract class AbstractShardedQueryProcessor<TCommand, TResult, TCombinedResult> where TCommand : RavenCommand<TResult>
{
    const string LimitToken = "__raven_limit";

    private Dictionary<int, BlittableJsonReaderObject> _queryTemplates;
    private Dictionary<int, TCommand> _commands;
    private readonly bool _indexEntriesOnly;
    private readonly bool _ignoreLimit;
    private readonly string _raftUniqueRequestId;
    private readonly HashSet<int> _filteredShardIndexes;

    protected readonly TransactionOperationContext Context;
    protected readonly ShardedDatabaseRequestHandler RequestHandler;
    protected readonly IndexQueryServerSide Query;
    protected readonly CancellationToken Token;
    protected readonly bool IsAutoMapReduceQuery;
    protected readonly IndexType IndexType;
    protected readonly IndexSourceType IndexSourceType;
    protected readonly IndexDefinitionBaseServerSide indexDefinition;
    protected readonly long? ExistingResultEtag;
    protected readonly bool MetadataOnly;

    protected AbstractShardedQueryProcessor(
        TransactionOperationContext context,
        ShardedDatabaseRequestHandler requestHandler,
        IndexQueryServerSide query,
        bool metadataOnly,
        bool indexEntriesOnly,
        bool ignoreLimit,
        long? existingResultEtag,
        CancellationToken token)
    {
        RequestHandler = requestHandler;
        Query = query;
        MetadataOnly = metadataOnly;
        _indexEntriesOnly = indexEntriesOnly;
        _ignoreLimit = ignoreLimit;
        Token = token;
        Context = context;
        ExistingResultEtag = existingResultEtag;

        IndexInformationHolder index = null;
        if (Query.Metadata.IndexName != null)
            index = RequestHandler.DatabaseContext.Indexes.GetIndex(Query.Metadata.IndexName);

        IndexType = index?.Type ?? IndexType.None;
        IndexSourceType = index?.SourceType ?? IndexSourceType.None;
        indexDefinition = index?.Definition;

        IsAutoMapReduceQuery = Query.Metadata.IsDynamic && Query.Metadata.IsGroupBy;

        _raftUniqueRequestId = RequestHandler.GetRaftRequestIdFromQuery() ?? RaftIdGenerator.NewId();

        if (Query.QueryParameters != null && Query.QueryParameters.TryGetMember(Constants.Documents.Querying.Sharding.ShardContextParameterName, out object filter))
        {
            // User can define a query parameter ("__shardContext") which is an id or an array
            // that contains the ids whose shards the query should be limited to.
            // Advanced: Optimization if user wants to run a query and knows what shards it is on. Such as:
            // from Orders where State = $state and User = $user where all the orders are on the same share as the user

            _filteredShardIndexes = new HashSet<int>();
            switch (filter)
            {
                case LazyStringValue:
                case LazyCompressedStringValue:
                    _filteredShardIndexes.Add(RequestHandler.DatabaseContext.GetShardNumberFor(context, filter.ToString()));
                    break;
                case BlittableJsonReaderArray arr:
                    {
                        for (int i = 0; i < arr.Length; i++)
                        {
                            var it = arr.GetStringByIndex(i);
                            _filteredShardIndexes.Add(RequestHandler.DatabaseContext.GetShardNumberFor(context, it));
                        }
                        break;
                    }
                default:
                    throw new NotSupportedException($"Unknown type of a shard context query parameter: {filter.GetType().Name}");
            }
        }
        else
        {
            _filteredShardIndexes = null;
        }
    }

    public abstract Task<TCombinedResult> ExecuteShardedOperations(QueryTimingsScope scope);

    protected int[] GetShardNumbers(Dictionary<int, TCommand> commands)
    {
        return _filteredShardIndexes == null ? commands.Keys.ToArray() : commands.Keys.Intersect(_filteredShardIndexes).ToArray();
    }

    protected virtual QueryType GetQueryType()
    {
        if (IndexType.IsMapReduce() || IsAutoMapReduceQuery)
            return QueryType.MapReduce;

        if (Query.Metadata.IsCollectionQuery)
            return QueryType.Collection;

        return QueryType.Map;
    }

    protected bool IsProjectionFromMapReduceIndex =>
        (IndexType.IsMapReduce() == false && IsAutoMapReduceQuery == false
         || (Query.Metadata.Query.Select == null || Query.Metadata.Query.Select.Count == 0)
         && Query.Metadata.Query.SelectFunctionBody.FunctionText == null) == false;

    public virtual ValueTask InitializeAsync()
    {
        AssertQueryExecution();

        // now we have the index query, we need to process that and decide how to handle this.
        // There are a few different modes to handle:
        // - For collection queries that specify startsWith by id(), we need to send to all shards
        // - For collection queries without any where clause, we need to send to all shards
        // - For indexes, we sent to all shards

        BlittableJsonReaderObject queryTemplate;

        if (Query.SourceQueryJson is { Modifications: null } && Query.QueryParameters == null)
            queryTemplate = Query.SourceQueryJson;
        else
            queryTemplate = Query.ToJson(Context);

        if (Query.Metadata.IsCollectionQuery && Query.Metadata.DeclaredFunctions is null or { Count: 0 })
        {
            // * For collection queries that specify ids, we can turn that into a set of loads that 
            //   will hit the known servers

            (List<Slice> ids, string _) = Query.ExtractIdsFromQuery(RequestHandler.ServerStore, Context.Allocator, RequestHandler.DatabaseContext.CompareExchangeStorage);

            if (ids != null)
            {
                _queryTemplates = GenerateLoadByIdQueries(ids);
            }
        }

        if (_queryTemplates == null)
        {
            var rewrittenQuery = RewriteQueryIfNeeded(queryTemplate);

            _queryTemplates = new(RequestHandler.DatabaseContext.ShardCount);

            foreach (var shardNumber in RequestHandler.DatabaseContext.ShardsTopology.Keys)
            {
                _queryTemplates.Add(shardNumber, rewrittenQuery);
            }
        }

        return ValueTask.CompletedTask;
    }

    private Dictionary<int, TCommand> CreateQueryCommands(Dictionary<int, BlittableJsonReaderObject> preProcessedQueries, QueryTimingsScope scope)
    {
        var commands = new Dictionary<int, TCommand>(preProcessedQueries.Count);

        foreach (var (shard, query) in preProcessedQueries)
        {
            commands.Add(shard, CreateCommand(shard, query, scope));
        }

        return commands;
    }

    public Dictionary<int, TCommand> GetOperationCommands(QueryTimingsScope scope)
    {
        return _commands ??= CreateQueryCommands(_queryTemplates, scope);
    }

    protected abstract TCommand CreateCommand(int shardNumber, BlittableJsonReaderObject query, QueryTimingsScope scope);

    protected ShardedQueryCommand CreateShardedQueryCommand(int shardNumber, BlittableJsonReaderObject query, QueryTimingsScope scope)
    {
        return new ShardedQueryCommand(
            RequestHandler.ShardExecutor.Conventions,
            query,
            Query,
            scope?.For($"Shard_{shardNumber}"),
            MetadataOnly,
            _indexEntriesOnly,
            _ignoreLimit,
            Query.Metadata.IndexName,
            canReadFromCache: ExistingResultEtag != null,
            _raftUniqueRequestId,
            RequestHandler.ServerStore.Configuration.Sharding.OrchestratorTimeout.AsTimeSpan);
    }

    protected virtual void AssertQueryExecution()
    {
        AssertOrdering();
    }

    private void AssertOrdering()
    {
        if (Query.Metadata.OrderBy == null || Query.Metadata.OrderBy.Length == 0)
            return;

        foreach (var field in Query.Metadata.OrderBy)
        {
            if (field.OrderingType == OrderByFieldType.Custom)
                throw new NotSupportedInShardingException("Custom sorting is not supported in sharding as of yet");

            if (field.OrderingType == OrderByFieldType.Distance && IndexType.IsMapReduce())
                throw new NotSupportedInShardingException("Distance sorting is not supported for Map-Reduce indexes in sharding as of yet");
        }
    }

    private BlittableJsonReaderObject RewriteQueryIfNeeded(BlittableJsonReaderObject queryTemplate)
    {
        var queryChanges = DetectChangesForQueryRewrite(Query, out var groupByFields);

        if (queryChanges == QueryChanges.None)
            return queryTemplate;

        var clone = Query.Metadata.Query.ShallowCopy();

        DynamicJsonValue modifications = new(queryTemplate);

        if (queryChanges.HasFlag(QueryChanges.RewriteForFilterInMapReduce))
        {
            clone.Filter = null;
            clone.FilterLimit = null;

            RemoveLimitAndOffset();
        }

        if (queryChanges.HasFlag(QueryChanges.RewriteForLimitWithOrderByInMapReduce))
        {
            // we have a limit with order by
            clone.OrderBy = null;

            RemoveLimitAndOffset();
        }

        var query = Query.Metadata.Query;

        if (queryChanges.HasFlag(QueryChanges.UseCachedOrderByFieldsInMapReduce))
        {
            clone.OrderBy = query.CachedOrderBy;
        }
        else if (queryChanges.HasFlag(QueryChanges.UpdateOrderByFieldsInMapReduce))
        {
            var orderByExpressions = new List<(QueryExpression Expression, OrderByFieldType FieldType, bool Ascending)>();
            var orderByFields = new List<OrderByField>();

            if (Query.Metadata.OrderBy != null)
            {
                // add existing order by fields
                foreach (var orderByField in Query.Metadata.OrderBy)
                {
                    orderByExpressions.Add((new FieldExpression(new List<StringSegment> { orderByField.Name.Value }), orderByField.OrderingType, orderByField.Ascending));
                    orderByFields.Add(orderByField);
                }
            }

            // add the missing group by fields
            foreach (var groupByField in groupByFields)
            {
                if (Query.Metadata.OrderBy != null && Query.Metadata.OrderByFieldNames.Contains(groupByField))
                    continue;

                orderByExpressions.Add((new FieldExpression(new List<StringSegment> { groupByField }), OrderByFieldType.Implicit, Ascending: true));
                orderByFields.Add(new OrderByField(new QueryFieldName(groupByField, isQuoted: false), OrderByFieldType.Implicit, ascending: true));
            }

            clone.OrderBy = orderByExpressions;
            query.CachedOrderBy = orderByExpressions;

            Query.Metadata.CachedOrderBy = orderByFields.ToArray();
        }

        if (queryChanges.HasFlag(QueryChanges.RewriteForPaging))
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

            var limit = ((Query.Limit ?? 0) + (Query.Offset ?? 0));
            if (limit == 0)
                limit = Query.Start + Query.PageSize;

            if (limit > int.MaxValue) // overflow
                limit = int.MaxValue;

            modifiedArgs[LimitToken] = limit;

            modifications.Remove(nameof(IndexQueryServerSide.Start));
            modifications.Remove(nameof(IndexQueryServerSide.PageSize));
        }

        if (queryChanges.HasFlag(QueryChanges.RewriteForProjectionFromMapReduceIndex))
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
        else if (queryChanges.HasFlag(QueryChanges.RewriteForProjectionFromAutoMapReduceQuery))
        {
            var missingGroupByFieldsFromProjection = Query.Metadata.GroupByFieldNames.ToHashSet(); // need a new instance here
            var groupByFieldsCount = missingGroupByFieldsFromProjection.Count;
            var hasKeyField = false;

            for (int i = 0; i < clone.Select.Count; i++)
            {
                var expression = clone.Select[i].Expression;
                switch (expression)
                {
                    case FieldExpression fe:
                        missingGroupByFieldsFromProjection.Remove(fe.FieldValue);
                        break;
                    case MethodExpression me:
                        {
                            if (me.ToString() == Constants.Documents.Indexing.Fields.ReduceKeyValueFieldName)
                                hasKeyField = true;
                            break;
                        }
                }

                if (clone.Select[i].Alias is null)
                    continue;

                clone.Select[i] = (expression, null);
            }

            if (hasKeyField == false || groupByFieldsCount != 1)
            {
                // we must have the group by fields in order for the re-reduce to work
                // for example: select sum("Count"), key() as Name
                // if we have more than 1 group by field, it will returned as a compound value
                // which isn't good for us because we need the actual fields in order to re-reduce
                // the only case when we don't need this is when we have only 1 group by field
                foreach (var field in missingGroupByFieldsFromProjection)
                {
                    clone.Select.Add((new FieldExpression(new List<StringSegment> { field }), null));
                }
            }
        }

        modifications[nameof(IndexQuery.Query)] = clone.ToString();

        queryTemplate.Modifications = modifications;

        return Context.ReadObject(queryTemplate, "modified-query");

        void RemoveLimitAndOffset()
        {
            clone.Offset = null;
            clone.Limit = null;

            modifications.Remove(nameof(IndexQueryServerSide.Start));
            modifications.Remove(nameof(IndexQueryServerSide.PageSize));

            queryChanges &= ~QueryChanges.RewriteForPaging;
        }
    }

    private QueryChanges DetectChangesForQueryRewrite(IndexQueryServerSide indexQuery, out List<string> groupByFields)
    {
        var query = indexQuery.Metadata.Query;
        var queryChanges = QueryChanges.None;

        if (indexQuery.Offset is > 0 || indexQuery.Start > 0)
            queryChanges |= QueryChanges.RewriteForPaging;

        var isProjectionQuery = indexQuery.Metadata.HasFacet == false && (query.Select?.Count > 0 || query.SelectFunctionBody.FunctionText != null);

        groupByFields = null;

        if (IndexType.IsMapReduce() || IsAutoMapReduceQuery)
        {
            if (query.Filter != null)
                queryChanges |= QueryChanges.RewriteForFilterInMapReduce;

            if (query.CachedOrderBy != null)
            {
                queryChanges |= QueryChanges.UseCachedOrderByFieldsInMapReduce;
                groupByFields = GetGroupByFields();
            }
            else
            {
                if (query.Limit != null)
                {
                    groupByFields = GetGroupByFields();

                    if (query.CachedOrderBy != null)
                    {
                        queryChanges |= QueryChanges.UseCachedOrderByFieldsInMapReduce;
                    }
                    if (query.OrderBy == null)
                    {
                        if (groupByFields.Count > 0) // we can have `group by 1` - then we don't have any field names
                        {
                            // we have a limit but not an order by
                            // we need to add the group by fields in order to get correct results
                            queryChanges |= QueryChanges.UpdateOrderByFieldsInMapReduce;
                        }
                    }
                    else
                    {
                        // we have a limit and order by fields
                        // we need to get all results if the order by isn't done on the group by fields
                        foreach (var orderByField in indexQuery.Metadata.OrderByFieldNames)
                        {
                            if (groupByFields.Contains(orderByField) == false)
                            {
                                // we found an order by field that isn't a group by field, we must get all the results
                                queryChanges |= QueryChanges.RewriteForLimitWithOrderByInMapReduce;
                                break;
                            }
                        }

                        // we are missing some group by fields
                        if (queryChanges.HasFlag(QueryChanges.RewriteForLimitWithOrderByInMapReduce) == false &&
                            indexQuery.Metadata.OrderByFieldNames.Count != groupByFields.Count)
                        {
                            queryChanges |= QueryChanges.UpdateOrderByFieldsInMapReduce;
                        }
                    }
                }

                if (query.OrderBy is { Count: > 0 } &&
                    queryChanges.HasFlag(QueryChanges.UpdateOrderByFieldsInMapReduce) == false)
                {
                    // we have order by fields but haven't detect that we need to update them yet
                    // we need to do additional checks

                    foreach (var orderByField in query.OrderBy)
                    {
                        if (orderByField.Expression is FieldExpression fe)
                        {
                            // we have alias usage in order by - we need use original field name
                            if (indexQuery.Metadata.IsAliasedField(fe))
                            {
                                groupByFields = GetGroupByFields();
                                queryChanges |= QueryChanges.UpdateOrderByFieldsInMapReduce;

                                break;
                            }
                        }
                    }
                }
            }
        }

        queryChanges |= QueryChanges.RewriteForProjectionFromMapReduceIndex;
        queryChanges |= QueryChanges.RewriteForProjectionFromAutoMapReduceQuery;

        if (isProjectionQuery == false)
        {
            queryChanges &= ~QueryChanges.RewriteForProjectionFromMapReduceIndex;
            queryChanges &= ~QueryChanges.RewriteForProjectionFromAutoMapReduceQuery;
        }
        else
        {
            if (IndexType.IsMapReduce() == false)
                queryChanges &= ~QueryChanges.RewriteForProjectionFromMapReduceIndex;

            if (IsAutoMapReduceQuery == false)
            {
                queryChanges &= ~QueryChanges.RewriteForProjectionFromAutoMapReduceQuery;
            }
            else
            {
                if (query.Select?.Count > 0)
                {
                    var hasAlias = false;

                    foreach (var selectField in query.Select)
                    {
                        if (selectField.Alias is not null)
                        {
                            hasAlias = true;
                            break;
                        }
                    }

                    if (hasAlias == false)
                        queryChanges &= ~QueryChanges.RewriteForProjectionFromMapReduceIndex;
                }
            }
        }

        return queryChanges;
    }

    private List<string> GetGroupByFields()
    {
        List<string> groupByFields;

        if (IsAutoMapReduceQuery)
        {
            groupByFields = Query.Metadata.GroupByFieldNames;
        }
        else
        {
            Debug.Assert(IndexType.IsMapReduce(), "Query isn't on a map-reduce index or an auto map reduce");

            var index = RequestHandler.DatabaseContext.Indexes.GetIndex(Query.Metadata.IndexName);
            if (index == null)
                IndexDoesNotExistException.ThrowFor(Query.Metadata.IndexName);

            if (index.Type.IsAutoMapReduce())
            {
                groupByFields = ((AutoMapReduceIndexDefinition)index.Definition).GroupByFieldNames;
            }
            else if (index.Type.IsStaticMapReduce())
            {
                groupByFields = ((StaticIndexInformationHolder)index).Compiled.GroupByFieldNames;
            }
            else
            {
                throw new InvalidOperationException($"Index '{Query.Metadata.IndexName}' is not a map-reduce index");
            }
        }

        return groupByFields;
    }

    private Dictionary<int, BlittableJsonReaderObject> GenerateLoadByIdQueries(IEnumerable<Slice> ids)
    {
        const string listParameterName = "__p0";

        var documentQuery = new DocumentQuery<dynamic>(null, null, Query.Metadata.CollectionName, isGroupBy: false, fromAlias: Query.Metadata.Query.From.Alias?.ToString())
        {
            ParameterPrefix = "__p"
        };
        documentQuery.WhereIn(Constants.Documents.Indexing.Fields.DocumentIdFieldName, Enumerable.Empty<object>());

        IncludeBuilder includeBuilder = null;

        if (Query.Metadata.Includes is { Length: > 0 })
        {
            includeBuilder = new IncludeBuilder
            {
                DocumentsToInclude = new HashSet<string>(Query.Metadata.Includes)
            };
        }

        if (Query.Metadata.RevisionIncludes != null)
        {
            includeBuilder ??= new IncludeBuilder();

            includeBuilder.RevisionsToIncludeByChangeVector = Query.Metadata.RevisionIncludes.RevisionsChangeVectorsPaths;
            includeBuilder.RevisionsToIncludeByDateTime = Query.Metadata.RevisionIncludes.RevisionsBeforeDateTime;
        }

        if (Query.Metadata.TimeSeriesIncludes != null)
        {
            includeBuilder ??= new IncludeBuilder();

            includeBuilder.TimeSeriesToIncludeBySourceAlias = Query.Metadata.TimeSeriesIncludes.TimeSeries;
        }

        if (Query.Metadata.CounterIncludes != null)
        {
            includeBuilder ??= new IncludeBuilder();

            includeBuilder.CountersToIncludeBySourcePath = new Dictionary<string, (bool AllCounters, HashSet<string> CountersToInclude)>(StringComparer.OrdinalIgnoreCase);

            foreach (var counterIncludes in Query.Metadata.CounterIncludes.Counters)
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

        var queryData = Query.Metadata.QueryData;
        if (queryData != null)
            documentQuery.SelectFields<dynamic>(queryData);

        var queryText = documentQuery.ToString();

        Dictionary<int, BlittableJsonReaderObject> queryTemplates = new();

        var shards = ShardLocator.GetDocumentIdsByShards(Context, RequestHandler.DatabaseContext, ids);

        foreach ((int shardId, ShardLocator.IdsByShard<Slice> documentIds) in shards)
        {
            DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Grisha, DevelopmentHelper.Severity.Normal, "RavenDB-19084 have a way to turn the _query into a json file and then we'll modify that, instead of building it manually");

            var q = new DynamicJsonValue
            {
                [nameof(IndexQuery.QueryParameters)] = GetParameters(),
                [nameof(IndexQuery.Query)] = queryText
            };

            queryTemplates[shardId] = Context.ReadObject(q, "query");

            DynamicJsonValue GetParameters()
            {
                var djv = new DynamicJsonValue
                {
                    [listParameterName] = GetIds()
                };

                if (Query.QueryParameters is { Count: > 0 })
                {
                    var offsetParameterName = GetParameterName(Query.Metadata.Query.Offset);
                    var limitParameterName = GetParameterName(Query.Metadata.Query.Limit);

                    var propertyDetails = new BlittableJsonReaderObject.PropertyDetails();
                    for (var i = 0; i < Query.QueryParameters.Count; i++)
                    {
                        Query.QueryParameters.GetPropertyByIndex(i, ref propertyDetails);

                        string parameterName = propertyDetails.Name;
                        if (parameterName == offsetParameterName || parameterName == limitParameterName)
                            continue;

                        djv[parameterName] = propertyDetails.Value;
                    }
                }

                return djv;
            }

            IEnumerable<string> GetIds()
            {
                foreach (var idAsAlice in documentIds.Ids)
                {
                    yield return idAsAlice.ToString();
                }
            }

            string GetParameterName(ValueExpression valueExpression)
            {
                if (valueExpression == null)
                    return null;

                if (valueExpression.Value != ValueTokenType.Parameter)
                    return null;

                return valueExpression.Token.Value;
            }
        }

        return queryTemplates;
    }

    protected async ValueTask HandleMissingDocumentIncludesAsync<T, TIncludes>(TransactionOperationContext context, HttpRequest request, ShardedDatabaseContext databaseContext, HashSet<string> missingIncludes,
        QueryResult<List<T>, List<TIncludes>> result, bool metadataOnly, CancellationToken token)
    {
        var missingIncludeIdsByShard = ShardLocator.GetDocumentIdsByShards(context, databaseContext, missingIncludes);
        await HandleMissingDocumentIncludesInternalAsync(context, request, databaseContext, result, missingIds: null, metadataOnly, missingIncludeIdsByShard, token);
    }

    public static async ValueTask HandleMissingDocumentIncludesAsync<T, TIncludes>(JsonOperationContext context, HttpRequest request, ShardedDatabaseContext databaseContext, HashSet<string> missingIncludes,
        QueryResult<List<T>, List<TIncludes>> result, HashSet<string> missingIds, bool metadataOnly, CancellationToken token)
    {
        var missingIncludeIdsByShard = ShardLocator.GetDocumentIdsByShards(databaseContext, missingIncludes);
        await HandleMissingDocumentIncludesInternalAsync(context, request, databaseContext, result, missingIds, metadataOnly, missingIncludeIdsByShard, token);
    }

    private static async Task HandleMissingDocumentIncludesInternalAsync<T, TIncludes>(JsonOperationContext context, HttpRequest request, ShardedDatabaseContext databaseContext,
        QueryResult<List<T>, List<TIncludes>> result, HashSet<string> missingIds, bool metadataOnly, Dictionary<int, ShardLocator.IdsByShard<string>> missingIncludeIdsByShard,
        CancellationToken token)
    {
        var missingIncludesOp = new FetchDocumentsFromShardsOperation(context, request, databaseContext, missingIncludeIdsByShard, includePaths: null,
            includeRevisions: null, counterIncludes: default, timeSeriesIncludes: null,
            compareExchangeValueIncludes: null, etag: null, metadataOnly, clusterWideTx: false);
        var missingResult = await databaseContext.ShardExecutor.ExecuteParallelForShardsAsync(missingIncludeIdsByShard.Keys.ToArray(), missingIncludesOp, token);

        var blittableIncludes = result.Includes as List<BlittableJsonReaderObject>;
        var documentIncludes = result.Includes as List<Document>;

        foreach (var (docId, missing) in missingResult.Result.Documents)
        {
            if (missing == null)
            {
                continue;
            }

            missingIds?.Add(docId);

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
                            Data = missing,
                            ChangeVector = metadata.GetChangeVector()
                        });
                    }
                }
            }
            else
                throw new NotSupportedException($"Unknown includes type: {result.Includes.GetType().FullName}");
        }
    }

    protected async Task WaitForRaftIndexIfNeededAsync(long? autoIndexCreationRaftCommandIndex, QueryTimingsScope scope)
    {
        if (IsAutoMapReduceQuery && autoIndexCreationRaftCommandIndex.HasValue)
        {
            using (scope?.For(nameof(QueryTimingsScope.Names.Cluster)))
            {
                // we are waiting here for all nodes, we should wait for all of the orchestrators at least to apply that
                // so further queries would not throw index does not exist in case of a failover
                await RequestHandler.DatabaseContext.Cluster.WaitForExecutionOnAllNodesAsync(autoIndexCreationRaftCommandIndex.Value, Token);
            }
        }
    }

    protected IComparer<BlittableJsonReaderObject> GetComparer(IndexQueryServerSide query)
    {
        var queryType = GetQueryType();

        if (queryType == QueryType.MapReduce) // map-reduce index/query (the sorting will be done after the re-reduce)
            return ConstantComparer.Instance;

        if (query.Limit == 0) // Count() query
            return ConstantComparer.Instance;

        if (query.Metadata.OrderBy?.Length > 0)
            return new DocumentsComparer(query.Metadata.OrderBy, extractFromData: queryType == QueryType.IndexEntries, query.Metadata.HasOrderByRandom);

        if (queryType == QueryType.IndexEntries)
            return ConstantComparer.Instance;

        if (IndexSourceType is IndexSourceType.Counters or IndexSourceType.TimeSeries)
            return ConstantComparer.Instance;

        if (query.Metadata.SelectFields is { Length: > 0 }) // projection
        {
            if (queryType == QueryType.Collection) // no stored fields
                return DocumentLastModifiedComparer.Throwing;

            return DocumentLastModifiedComparer.NotThrowing; // we can have stored fields here, if all of them come from stored, then we do not have @last-modified
        }

        return DocumentLastModifiedComparer.Throwing;
    }

    [Flags]
    private enum QueryChanges
    {
        None = 0,

        RewriteForPaging = 1 << 0,
        RewriteForProjectionFromMapReduceIndex = 1 << 1,
        RewriteForProjectionFromAutoMapReduceQuery = 1 << 2,
        RewriteForFilterInMapReduce = 1 << 3,
        RewriteForLimitWithOrderByInMapReduce = 1 << 4,
        UpdateOrderByFieldsInMapReduce = 1 << 5,
        UseCachedOrderByFieldsInMapReduce = 1 << 6
    }

    protected enum QueryType
    {
        Map,
        MapReduce,
        Collection,
        IndexEntries
    }
}
