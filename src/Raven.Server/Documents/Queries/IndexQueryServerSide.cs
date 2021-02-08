using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using Microsoft.AspNetCore.Http;
using Raven.Client;
using Raven.Client.Documents.Changes;
using Raven.Client.Documents.Queries;
using Raven.Client.Exceptions;
using Raven.Server.Documents.Indexes.Static.Spatial;
using Raven.Server.Documents.Queries.AST;
using Raven.Server.Documents.Queries.Timings;
using Raven.Server.Extensions;
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
    public class IndexQueryServerSide : IndexQuery<BlittableJsonReaderObject>
    {
        [JsonDeserializationIgnore]
        public int? Offset;

        [JsonDeserializationIgnore]
        public int? Limit;

        [JsonDeserializationIgnore]
        public QueryMetadata Metadata { get; private set; }

        [JsonDeserializationIgnore]
        public QueryTimingsScope Timings { get; private set; }

        [JsonDeserializationIgnore]
        public SpatialDistanceFieldComparatorSource.SpatialDistanceFieldComparator Distances;

        public new int Start
        {
#pragma warning disable 618
            get => base.Start;
            set => base.Start = value;
#pragma warning restore 618
        }

        public new int PageSize
        {
#pragma warning disable 618
            get => base.PageSize;
            set => base.PageSize = value;
#pragma warning restore 618
        }

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

        public bool IsPartOfGraphQuery;

        public string ServerSideQuery => Metadata.Query.ToString();

        public DocumentFields? DocumentFields;

        public List<string> Diagnostics;

        public string ClientVersion;

        public bool AddTimeSeriesNames;

        public bool IsStream;

        private BlittableJsonReaderObject _asJson;

        public BlittableJsonReaderObject ToJson(JsonOperationContext context)
        {
            if (_asJson != null) 
                return _asJson;

            _asJson = context.ReadObject(new DynamicJsonValue
            {
                [nameof(Query)] = Query,
                [nameof(Start)] = Start,
                [nameof(PageSize)] = PageSize,
                [nameof(QueryParameters)] = QueryParameters,
                [nameof(WaitForNonStaleResults)] = WaitForNonStaleResults,
                [nameof(WaitForNonStaleResultsTimeout)] = WaitForNonStaleResultsTimeout,
                [nameof(SkipDuplicateChecking)] = SkipDuplicateChecking,
                [nameof(ProjectionBehavior)] = ProjectionBehavior,
                
            }, "query");
            
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
            DocumentDatabase database = null,
            QueryType queryType = QueryType.Select)
        {
            IndexQueryServerSide result = null;
            try
            {
                result = JsonDeserializationServer.IndexQuery(json);
                result._asJson = json;

                if (result.PageSize == 0 && json.TryGet(nameof(PageSize), out int _) == false)
                    result.PageSize = int.MaxValue;

                if (string.IsNullOrWhiteSpace(result.Query))
                    throw new InvalidOperationException($"Index query does not contain '{nameof(Query)}' field.");

                if (cache.TryGetMetadata(result, out var metadataHash, out var metadata))
                {
                    result.Metadata = metadata;
                    SetupPagingFromQueryMetadata();
                    return result;
                }

                result.Metadata = new QueryMetadata(result.Query, result.QueryParameters, metadataHash, queryType, database);
                if (result.Metadata.HasTimings)
                    result.Timings = new QueryTimingsScope(start: false);

                SetupPagingFromQueryMetadata();

                if (tracker != null)
                    tracker.Query = result.Query;

                if (result.Metadata.HasFacet && httpContext.Request.Headers.TryGetValue(Constants.Headers.ClientVersion, out var clientVersion))
                    result.ClientVersion = clientVersion;

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
                    var start = (int)QueryBuilder.GetLongValue(result.Metadata.Query, result.Metadata, result.QueryParameters, result.Metadata.Query.Offset, 0);
                    result.Offset = start;
                    result.Start = result.Start != 0 || json.TryGet(nameof(Start), out int _)
                        ? Math.Min(start, result.Start)
                        : start;
                }

                if (result.Metadata.Query.Limit != null)
                {
                    var limit = (int)QueryBuilder.GetLongValue(result.Metadata.Query, result.Metadata, result.QueryParameters, result.Metadata.Query.Limit, int.MaxValue);
                    result.Limit = limit;
                    result.PageSize = Math.Min(limit, result.PageSize);
                }
            }
        }

        public static IndexQueryServerSide Create(HttpContext httpContext, int start, int pageSize, JsonOperationContext context, RequestTimeTracker tracker, string overrideQuery = null)
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
                                using (var stream = new MemoryStream(Encoding.UTF8.GetBytes(item.Value[0])))
                                {
                                    result.QueryParameters = context.Read(stream, "query parameters");
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

                result.Metadata = new QueryMetadata(result.Query, result.QueryParameters, 0);
                if (result.Metadata.HasTimings)
                    result.Timings = new QueryTimingsScope(start: false);

                if (result.Metadata.Query.Offset != null)
                {
                    var offset = (int)QueryBuilder.GetLongValue(result.Metadata.Query, result.Metadata, result.QueryParameters, result.Metadata.Query.Offset, 0);
                    result.Offset = offset;
                    result.Start = start + offset;
                }

                if (result.Metadata.Query.Limit != null)
                {
                    pageSize = (int)QueryBuilder.GetLongValue(result.Metadata.Query, result.Metadata, result.QueryParameters, result.Metadata.Query.Limit, int.MaxValue);
                    result.Limit = pageSize;
                    result.PageSize = Math.Min(result.PageSize, pageSize);
                }

                if (tracker != null)
                    tracker.Query = result.Query;

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
        }

        private static void AssertPaging(IndexQueryServerSide indexQuery)
        {
            if (indexQuery.Offset < 0)
                throw new InvalidQueryException($"{nameof(Offset)} ({nameof(Start)}) cannot be negative, but was {indexQuery.Offset}.", indexQuery.Query, indexQuery.QueryParameters);

            if (indexQuery.Limit < 0)
                throw new InvalidQueryException($"{nameof(Limit)} ({nameof(PageSize)}) cannot be negative, but was {indexQuery.Limit}.", indexQuery.Query, indexQuery.QueryParameters);

            if (indexQuery.Start < 0)
                throw new InvalidQueryException($"{nameof(Start)} cannot be negative, but was {indexQuery.Start}.", indexQuery.Query, indexQuery.QueryParameters);

            if (indexQuery.PageSize < 0)
                throw new InvalidQueryException($"{nameof(PageSize)} cannot be negative, but was {indexQuery.PageSize}.", indexQuery.Query, indexQuery.QueryParameters);
        }
        
        public (List<string> Ids, string StartsWith) ExtractIdsFromQuery(ServerStore serverStore, string databaseName)
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
                    var idsRetriever = new RetrieveDocumentIdsVisitor(serverContext,serverStore, databaseName,  Metadata);

                    idsRetriever.Visit(Metadata.Query.Where, QueryParameters);

                    var sortedIds = idsRetriever.Ids?.ToList();
                    sortedIds?.Sort(StringComparer.OrdinalIgnoreCase);
                    return (sortedIds, idsRetriever.StartsWith);
                }
            }
            finally
            {
                releaseServerContext?.Dispose();
            }
        }
        
        private class RetrieveDocumentIdsVisitor : WhereExpressionVisitor
        {
            private readonly Query _query;
            private readonly TransactionOperationContext _serverContext;
            private readonly ServerStore _serverStore;
            private readonly string _databaseName;
            private readonly QueryMetadata _metadata;
            public string StartsWith;

            public HashSet<string> Ids { get; private set; }

            public RetrieveDocumentIdsVisitor(TransactionOperationContext serverContext, ServerStore serverStore, string databaseName,  QueryMetadata metadata) : base(metadata.Query.QueryText)
            {
                _query = metadata.Query;
                _serverContext = serverContext;
                _serverStore = serverStore;
                _databaseName = databaseName;
                _metadata = metadata;
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
                                var id = QueryBuilder.GetValue(_query, _metadata, parameters, ve);

                                Debug.Assert(id.Type == ValueTokenType.String || id.Type == ValueTokenType.Null);

                                AddId(id.Value?.ToString());
                            }
                            if (value is MethodExpression right)
                            {
                                var id = QueryBuilder.EvaluateMethod(_query, _metadata, _serverContext, _databaseName, _serverStore, right, ref parameters);
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
                Ids ??= new HashSet<string>(StringComparer.OrdinalIgnoreCase);

                if (fieldName is MethodExpression me && string.Equals("id", me.Name.Value, StringComparison.OrdinalIgnoreCase))
                {
                    foreach (var item in values)
                    {
                        if (item is ValueExpression iv)
                        {
                            foreach (var id in QueryBuilder.GetValues(_query, _metadata, parameters, iv))
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
                        var prefix = QueryBuilder.GetValue(_query, _metadata, parameters, iv);
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
                Ids ??= new HashSet<string>(StringComparer.OrdinalIgnoreCase);

                Ids.Add(id);
            }

            private void ThrowNotSupportedCollectionQueryOperator(string @operator, BlittableJsonReaderObject parameters)
            {
                throw new InvalidQueryException(
                    $"Collection query does not support filtering by {Constants.Documents.Indexing.Fields.DocumentIdFieldName} using {@operator} operator. Supported operators are: =, IN",
                    QueryText, parameters);
            }
        }
    }
}
