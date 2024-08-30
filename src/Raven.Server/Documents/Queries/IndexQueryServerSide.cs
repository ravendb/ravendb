using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Raven.Client;
using Raven.Client.Documents.Changes;
using Raven.Client.Documents.Queries;
using Raven.Client.Exceptions;
using Raven.Server.Documents.Indexes.Static.Spatial;
using Raven.Server.Documents.Queries.AST;
using Raven.Server.Documents.Queries.Timings;
using Raven.Server.Json;
using Raven.Server.NotificationCenter;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.TrafficWatch;
using Sparrow;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Server;
using Voron;

namespace Raven.Server.Documents.Queries
{
    public sealed class IndexQueryServerSide : IndexQuery<BlittableJsonReaderObject>
    {
        [JsonDeserializationIgnore]
        public QueryResultReturnOptions ReturnOptions;

        [JsonDeserializationIgnore]
        public long? Offset;

        [JsonDeserializationIgnore]
        public long? Limit;

        [JsonDeserializationIgnore]
        public long? FilterLimit { get; set; }

        [JsonDeserializationIgnore]
        public QueryMetadata Metadata { get; private set; }

        [JsonDeserializationIgnore]
        public QueryTimingsScope Timings { get; private set; }

        [JsonDeserializationIgnore]
        public SpatialDistanceFieldComparatorSource.SpatialDistanceFieldComparator Distances;


        public long Start;

        public long PageSize = int.MaxValue;

        /// <summary>
        /// puts the given string in TrafficWatch property of HttpContext.Items
        /// puts the given type in TrafficWatchChangeType property of HttpContext.Items
        /// </summary>
        public static void AddStringToHttpContext(HttpContext httpContext, string str, TrafficWatchChangeType type)
        {
            httpContext.Items["TrafficWatch"] = (str, type);
        }

        private IndexQueryServerSide()
        {
            // for deserialization
        }

        public IndexQueryServerSide(QueryMetadata metadata)
        {
            Metadata = metadata;
        }

        public string ServerSideQuery => Metadata.Query.ToString();

        public DocumentFields? DocumentFields;

        public List<string> Diagnostics;

        public string ClientVersion;

        public bool AddTimeSeriesNames;

        public bool DisableAutoIndexCreation;

        public bool IsStream;

        public string ClientQueryId;

        [JsonDeserializationIgnore]
        public BlittableJsonReaderObject SourceQueryJson { get; set; }

        [JsonDeserializationIgnore]
        public bool IsCountQuery => Limit == 0 && Offset == 0;

        private BlittableJsonReaderObject _asJson;

        public BlittableJsonReaderObject ToJson(JsonOperationContext context)
        {
            if (_asJson != null)
            {
                Debug.Assert(context == _asJson._context, $"context == _asJson._context, Query: {Query}");
                return _asJson;
            }

            var djv = new DynamicJsonValue
            {
                [nameof(Query)] = Query
            };

            if (QueryParameters != null)
                djv[nameof(QueryParameters)] = QueryParameters;

            if (Start > 0)
                djv[nameof(Start)] = Start;

            if (PageSize != int.MaxValue && PageSize != long.MaxValue)
                djv[nameof(PageSize)] = PageSize;

            if (WaitForNonStaleResults)
                djv[nameof(WaitForNonStaleResults)] = WaitForNonStaleResults;

            if (WaitForNonStaleResultsTimeout.HasValue)
                djv[nameof(WaitForNonStaleResultsTimeout)] = WaitForNonStaleResultsTimeout;

            if (SkipDuplicateChecking)
                djv[nameof(SkipDuplicateChecking)] = SkipDuplicateChecking;

            if (ProjectionBehavior.HasValue)
                djv[nameof(ProjectionBehavior)] = ProjectionBehavior;

            _asJson = context.ReadObject(djv, "query");

            return _asJson;
        }

        public IndexQueryServerSide(string query, BlittableJsonReaderObject queryParameters = null)
        {
            Query = Uri.UnescapeDataString(query);
            QueryParameters = queryParameters;
            Metadata = new QueryMetadata(Query, queryParameters, 0);
        }

        public static IndexQueryServerSide Create(HttpContext httpContext,
            BlittableJsonReaderObject json,
            QueryMetadataCache cache,
            RequestTimeTracker tracker,
            bool addSpatialProperties = false,
            string clientQueryId = null,
            QueryType queryType = QueryType.Select)
        {
            IndexQueryServerSide result = null;
            try
            {
                result = JsonDeserializationServer.IndexQuery(json);
                result.ClientQueryId = clientQueryId;

                if (result.PageSize == 0 && json.TryGet(nameof(PageSize), out int _) == false)
                    result.PageSize = int.MaxValue;

                if (string.IsNullOrWhiteSpace(result.Query))
                    throw new InvalidOperationException($"Index query does not contain '{nameof(Query)}' field.");

                result.SourceQueryJson = json;

                if (cache.TryGetMetadata(result, addSpatialProperties, out var metadataHash, out var metadata))
                {
                    result.Metadata = metadata;

                    SetupTimings(result);
                    SetupPagingFromQueryMetadata();
                    SetupTracker(result, tracker);
                    SetupClientVersion(result, httpContext);

                    AssertPaging(result);

                    return result;
                }

                result.Metadata = new QueryMetadata(result.Query, result.QueryParameters, metadataHash, addSpatialProperties, queryType);

                SetupTimings(result);
                SetupPagingFromQueryMetadata();
                SetupTracker(result, tracker);
                SetupClientVersion(result, httpContext);

                AssertPaging(result);

                return result;
            }
            catch (Exception e)
            {
                var errorMessage = e is InvalidOperationException
                    ? e.Message : result == null ? $"Failed to parse index query : {json}" : $"Failed to parse query: {result.Query}";

                if (tracker != null)
                    tracker.Query = errorMessage;

                if (TrafficWatchManager.HasRegisteredClients)
                    AddStringToHttpContext(httpContext, errorMessage, TrafficWatchChangeType.Queries);

                throw;
            }

            void SetupPagingFromQueryMetadata()
            {
                if (result.Metadata.Query.Offset != null)
                {
                    var start = QueryBuilderHelper.GetLongValue(result.Metadata.Query, result.Metadata, result.QueryParameters, result.Metadata.Query.Offset, 0);
                    result.Offset = start;
                    result.Start = result.Start != 0 || json.TryGet(nameof(Start), out long _)
                        ? Math.Max(start, result.Start)
                        : start;
                }

                if (result.Metadata.Query.Limit != null)
                {
                    var limit = Math.Min(int.MaxValue, QueryBuilderHelper.GetLongValue(result.Metadata.Query, result.Metadata, result.QueryParameters, result.Metadata.Query.Limit, int.MaxValue));
                    result.Limit = limit;
                    result.PageSize = Math.Min(limit, result.PageSize);
                }

                if (result.Metadata.Query.FilterLimit != null)
                {
                    result.FilterLimit = Math.Min(int.MaxValue, QueryBuilderHelper.GetLongValue(result.Metadata.Query, result.Metadata, result.QueryParameters, result.Metadata.Query.FilterLimit, int.MaxValue));
                }
            }
        }

        public static async Task<IndexQueryServerSide> CreateAsync(HttpContext httpContext, long start, long pageSize, JsonOperationContext context, RequestTimeTracker tracker, bool addSpatialProperties = false, string clientQueryId = null, string overrideQuery = null)
        {
            IndexQueryServerSide result = null;
            try
            {
                var isQueryOverwritten = !string.IsNullOrEmpty(overrideQuery);
                if ((httpContext.Request.Query.TryGetValue("query", out var query) == false || query.Count == 0 || string.IsNullOrWhiteSpace(query[0])) && isQueryOverwritten == false)
                    throw new InvalidOperationException("Missing mandatory query string parameter 'query'.");

                var actualQuery = isQueryOverwritten ? overrideQuery : query[0];
                result = new IndexQueryServerSide
                {
                    Query = Uri.UnescapeDataString(actualQuery),
                    // all defaults which need to have custom value
                    Start = start,
                    PageSize = pageSize,
                    ClientQueryId = clientQueryId
                };

                foreach (var item in httpContext.Request.Query)
                {
                    try
                    {
                        switch (item.Key)
                        {
                            case "query":
                                continue;
                            case "parameters":
                                await using (var stream = new MemoryStream(Encoding.UTF8.GetBytes(item.Value[0])))
                                {
                                    result.QueryParameters = await context.ReadForMemoryAsync(stream, "query parameters");
                                }
                                continue;
                            case "waitForNonStaleResults":
                                result.WaitForNonStaleResults = bool.Parse(item.Value[0]);
                                break;
                            case "waitForNonStaleResultsTimeoutInMs":
                                result.WaitForNonStaleResultsTimeout = TimeSpan.FromMilliseconds(long.Parse(item.Value[0]));
                                break;
                            case "skipDuplicateChecking":
                                result.SkipDuplicateChecking = bool.Parse(item.Value[0]);
                                break;
                            case "skipStatistics":
                                result.SkipStatistics = bool.Parse(item.Value[0]);
                                break;
                            case "projectionBehavior":
                                result.ProjectionBehavior = Enum.Parse<ProjectionBehavior>(item.Value[0], ignoreCase: true);
                                break;
                        }
                    }
                    catch (Exception e)
                    {
                        throw new ArgumentException($"Could not handle query string parameter '{item.Key}' (value: {item.Value}) for query: {result.Query}", e);
                    }
                }

                result.Metadata = new QueryMetadata(result.Query, result.QueryParameters, 0, addSpatialProperties);

                SetupTimings(result);
                SetupPagingFromQueryMetadata();
                SetupTracker(result, tracker);
                SetupClientVersion(result, httpContext);

                AssertPaging(result);

                return result;
            }
            catch (Exception e)
            {
                var errorMessage = e is InvalidOperationException || e is ArgumentException
                    ? e.Message
                    : $"Failed to parse query: {result.Query}";

                if (tracker != null)
                    tracker.Query = errorMessage;

                if (TrafficWatchManager.HasRegisteredClients)
                    AddStringToHttpContext(httpContext, errorMessage, TrafficWatchChangeType.Queries);

                throw;
            }

            void SetupPagingFromQueryMetadata()
            {
                if (result.Metadata.Query.Offset != null)
                {
                    var offset = QueryBuilderHelper.GetLongValue(result.Metadata.Query, result.Metadata, result.QueryParameters, result.Metadata.Query.Offset, 0);
                    result.Offset = offset;
                    result.Start = start + offset;
                }

                if (result.Metadata.Query.Limit != null)
                {
                    pageSize = Math.Min(int.MaxValue, QueryBuilderHelper.GetLongValue(result.Metadata.Query, result.Metadata, result.QueryParameters, result.Metadata.Query.Limit, int.MaxValue));
                    result.Limit = pageSize;
                    result.PageSize = Math.Min(result.PageSize, pageSize);
                }

                if (result.Metadata.Query.FilterLimit != null)
                {
                    result.FilterLimit = Math.Min(int.MaxValue, QueryBuilderHelper.GetLongValue(result.Metadata.Query, result.Metadata, result.QueryParameters, result.Metadata.Query.FilterLimit, int.MaxValue));
                }
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static void SetupTimings(IndexQueryServerSide indexQuery)
        {
            if (indexQuery.Metadata.HasTimings || (TrafficWatchManager.HasRegisteredClients && indexQuery.Metadata.IsCollectionQuery == false))
                indexQuery.Timings = new QueryTimingsScope(start: false);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static void SetupTracker(IndexQueryServerSide indexQuery, RequestTimeTracker tracker)
        {
            if (tracker != null)
            {
                tracker.Query = indexQuery.Query;
                tracker.QueryParameters = indexQuery.QueryParameters;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static void SetupClientVersion(IndexQueryServerSide indexQuery, HttpContext httpContext)
        {
            if (indexQuery.Metadata.HasFacet && httpContext.Request.Headers.TryGetValue(Constants.Headers.ClientVersion, out var clientVersion))
                indexQuery.ClientVersion = clientVersion;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static void AssertPaging(IndexQueryServerSide indexQuery)
        {
            if (indexQuery.Offset < 0)
                throw new InvalidQueryException($"{nameof(Offset)} ({nameof(Start)}) cannot be negative, but was {indexQuery.Offset}.", indexQuery.Query, indexQuery.QueryParameters);

            if (indexQuery.Limit < 0)
                throw new InvalidQueryException($"{nameof(Limit)} ({nameof(PageSize)}) cannot be negative, but was {indexQuery.Limit}.", indexQuery.Query, indexQuery.QueryParameters);

            if (indexQuery.FilterLimit <= 0)
                throw new InvalidQueryException($"{nameof(FilterLimit)} cannot be negative or zero, but was {indexQuery.FilterLimit}.", indexQuery.Query, indexQuery.QueryParameters);

            if (indexQuery.Start < 0)
                throw new InvalidQueryException($"{nameof(Start)} cannot be negative, but was {indexQuery.Start}.", indexQuery.Query, indexQuery.QueryParameters);

            if (indexQuery.PageSize < 0)
                throw new InvalidQueryException($"{nameof(PageSize)} cannot be negative, but was {indexQuery.PageSize}.", indexQuery.Query, indexQuery.QueryParameters);
        }

        public (List<Slice> Ids, string StartsWith) ExtractIdsFromQuery(ServerStore serverStore, ByteStringContext allocator, AbstractCompareExchangeStorage compareExchangeStorage)
        {
            if (Metadata.Query.Where == null)
                return (null, null);

            if (Metadata.IndexFieldNames.Contains(QueryFieldName.DocumentId) == false)
                return (null, null);

            IDisposable releaseServerContext = null;
            IDisposable closeServerTransaction = null;
            TransactionOperationContext serverContext = null;

            try
            {
                if (Metadata.HasCmpXchg)
                {
                    releaseServerContext = serverStore.ContextPool.AllocateOperationContext(out serverContext);
                    closeServerTransaction = serverContext.OpenReadTransaction();
                }

                using (closeServerTransaction)
                {
                    var idsRetriever = new RetrieveDocumentIdsVisitor(serverContext, compareExchangeStorage, Metadata, allocator);

                    idsRetriever.Visit(Metadata.Query.Where, QueryParameters);

                    return (idsRetriever.Ids?.OrderBy(x => x, SliceComparer.Instance).ToList(), idsRetriever.StartsWith);
                }
            }
            finally
            {
                releaseServerContext?.Dispose();
            }
        }

        private sealed class RetrieveDocumentIdsVisitor : WhereExpressionVisitor
        {
            private readonly Query _query;
            private readonly TransactionOperationContext _serverContext;
            private readonly AbstractCompareExchangeStorage _compareExchangeStorage;
            private readonly QueryMetadata _metadata;
            private readonly ByteStringContext _allocator;
            public string StartsWith;

            public HashSet<Slice> Ids { get; private set; }

            public RetrieveDocumentIdsVisitor(TransactionOperationContext serverContext, AbstractCompareExchangeStorage compareExchangeStorage, QueryMetadata metadata, ByteStringContext allocator) : base(metadata.Query.QueryText)
            {
                _query = metadata.Query;
                _serverContext = serverContext;
                _compareExchangeStorage = compareExchangeStorage;
                _metadata = metadata;
                _allocator = allocator;
            }

            public override void VisitBooleanMethod(QueryExpression leftSide, QueryExpression rightSide, OperatorType operatorType, BlittableJsonReaderObject parameters)
            {
                VisitFieldToken(leftSide, rightSide, parameters, operatorType);
            }

            public override void VisitFieldToken(QueryExpression fieldName, QueryExpression value, BlittableJsonReaderObject parameters, OperatorType? operatorType)
            {
                if (fieldName is MethodExpression me)
                {
                    var methodType = QueryMethod.GetMethodType(me.Name.Value);
                    switch (methodType)
                    {
                        case MethodType.Id:
                            if (value is ValueExpression ve)
                            {
                                var id = QueryBuilderHelper.GetValue(_query, _metadata, parameters, ve);

                                Debug.Assert(id.Type == ValueTokenType.String || id.Type == ValueTokenType.Null);

                                AddId(id.Value?.ToString());
                            }
                            if (value is MethodExpression right)
                            {
                                var id = LuceneQueryBuilder.EvaluateMethod(_query, _metadata, _serverContext, _compareExchangeStorage, right, ref parameters);
                                if (id is ValueExpression v)
                                    AddId(v.Token.Value);
                            }
                            break;
                    }
                }
            }

            public override void VisitBetween(QueryExpression fieldName, QueryExpression firstValue, QueryExpression secondValue, BlittableJsonReaderObject parameters)
            {
                if (fieldName is MethodExpression me && string.Equals("id", me.Name.Value, StringComparison.OrdinalIgnoreCase) && firstValue is ValueExpression fv && secondValue is ValueExpression sv)
                {
                    throw new InvalidQueryException("Collection query does not support filtering by id() using Between operator. Supported operators are: =, IN",
                        QueryText, parameters);
                }
            }

            public override void VisitIn(QueryExpression fieldName, List<QueryExpression> values, BlittableJsonReaderObject parameters)
            {
                // Handles the case of IN with empty list
                Ids ??= new HashSet<Slice>(SliceComparer.Instance);

                if (fieldName is MethodExpression me && string.Equals("id", me.Name.Value, StringComparison.OrdinalIgnoreCase))
                {
                    foreach (var item in values)
                    {
                        if (item is ValueExpression iv)
                        {
                            foreach (var id in QueryBuilderHelper.GetValues(_query, _metadata, parameters, iv))
                            {
                                AddId(id.Value?.ToString());
                            }
                        }
                    }
                }
                else
                {
                    throw new InvalidQueryException("Collection query does not support filtering by id() using Between operator. Supported operators are: =, IN",
                        QueryText, parameters);
                }
            }

            public override void VisitMethodTokens(StringSegment name, List<QueryExpression> arguments, BlittableJsonReaderObject parameters)
            {
                var expression = arguments[^1];
                if (expression is BinaryExpression be && be.Operator == OperatorType.Equal)
                {
                    VisitFieldToken(new MethodExpression("id", new List<QueryExpression>()), be.Right, parameters, be.Operator);
                }
                else if (expression is InExpression ie)
                {
                    VisitIn(new MethodExpression("id", new List<QueryExpression>()), ie.Values, parameters);
                }
                else if (string.Equals(name.Value, "startsWith", StringComparison.OrdinalIgnoreCase))
                {
                    if (expression is ValueExpression iv)
                    {
                        var prefix = QueryBuilderHelper.GetValue(_query, _metadata, parameters, iv);
                        StartsWith = prefix.Value?.ToString();
                    }
                }
                else
                {
                    ThrowNotSupportedCollectionQueryOperator(expression.Type.ToString(), parameters);
                }
            }

            private void AddId(string id)
            {
                Slice key;
                if (string.IsNullOrEmpty(id) == false)
                {
                    Slice.From(_allocator, id, out key);
                    _allocator.ToLowerCase(ref key.Content);
                }
                else
                {
                    // this is a rare case
                    // we are allocating here, because we are releasing all of the ids later on
                    // if we will use Slices.Empty, then we will release that on a different context
                    Slice.From(_allocator, string.Empty, out key);
                }

                Ids ??= new HashSet<Slice>(SliceComparer.Instance);

                Ids.Add(key);
            }

            [DoesNotReturn]
            private void ThrowNotSupportedCollectionQueryOperator(string @operator, BlittableJsonReaderObject parameters)
            {
                throw new InvalidQueryException(
                    $"Collection query does not support filtering by {Constants.Documents.Indexing.Fields.DocumentIdFieldName} using {@operator} operator. Supported operators are: =, IN",
                    QueryText, parameters);
            }
        }

        public sealed class QueryResultReturnOptions
        {
            public bool MissingIncludeAsNull;

            public bool RawFacetResults;

            public bool AddOrderByFieldsMetadata;

            public bool AddDataHashMetadata;

            public static QueryResultReturnOptions CreateForSharding(IndexQueryServerSide query)
            {
                return new QueryResultReturnOptions
                {
                    MissingIncludeAsNull = true,
                    RawFacetResults = true,
                    AddDataHashMetadata = query.Metadata.IsDistinct,
                    AddOrderByFieldsMetadata = query.Metadata.OrderBy?.Length > 0 && (query.Limit is null || query.Limit > 0) // for sharded queries, we'll send the order by fields separately
                };
            }
        }
    }
}
