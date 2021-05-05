using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Text.RegularExpressions;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Queries;
using Raven.Client.Documents.Queries.Facets;
using Raven.Client.Documents.Session.Tokens;
using Raven.Client.Exceptions.Documents.Indexes;
using Raven.Client.Extensions;
using Raven.Client.Util;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Logging;

namespace Raven.Client.Documents.Session.Operations
{
    public class QueryOperation
    {
        private readonly InMemoryDocumentSessionOperations _session;
        private readonly string _indexName;
        private readonly IndexQuery _indexQuery;
        private readonly bool _metadataOnly;
        private readonly bool _indexEntriesOnly;
        private readonly bool _isProjectInto;
        private QueryResult _currentQueryResults;
        private readonly FieldsToFetchToken _fieldsToFetch;
        private Stopwatch _sp;
        private static readonly Logger Logger = LoggingSource.Instance.GetLogger<QueryOperation>("Client");
        private static readonly PropertyInfo[] _facetResultProperties = typeof(FacetResult).GetProperties();

        public QueryResult CurrentQueryResults => _currentQueryResults;

        public QueryOperation(
            InMemoryDocumentSessionOperations session,
            string indexName,
            IndexQuery indexQuery,
            FieldsToFetchToken fieldsToFetch,
            bool disableEntitiesTracking,
            bool metadataOnly = false,
            bool indexEntriesOnly = false,
            bool isProjectInto = false)
        {
            _session = session;
            _indexName = indexName;
            _indexQuery = indexQuery;
            _fieldsToFetch = fieldsToFetch;
            NoTracking = disableEntitiesTracking;
            _metadataOnly = metadataOnly;
            _indexEntriesOnly = indexEntriesOnly;
            _isProjectInto = isProjectInto;

            AssertPageSizeSet();
        }

        public QueryCommand CreateRequest()
        {
            _session.IncrementRequestCount();
            LogQuery();
            return new QueryCommand(_session, _indexQuery, _metadataOnly, _indexEntriesOnly);
        }

        public void SetResult(QueryResult queryResult)
        {
            EnsureIsAcceptableAndSaveResult(queryResult);
        }

        private void AssertPageSizeSet()
        {
            if (_session.Conventions.ThrowIfQueryPageSizeIsNotSet == false)
                return;

            if (_indexQuery.PageSizeSet)
                return;

            throw new InvalidOperationException("Attempt to query without explicitly specifying a page size. You can use .Take() methods to set maximum number of results. By default the page size is set to int.MaxValue and can cause severe performance degradation.");
        }

        private void StartTiming()
        {
            _sp = Stopwatch.StartNew();
        }

        public void LogQuery()
        {
            if (Logger.IsInfoEnabled)
                Logger.Info($"Executing query '{_indexQuery.Query}' on index '{_indexName}' in '{_session.StoreIdentifier}'");
        }

        public IDisposable EnterQueryContext()
        {
            StartTiming();

            if (_indexQuery.WaitForNonStaleResults == false)
                return null;

            return _session.DocumentStore.DisableAggressiveCaching(_session.DatabaseName);
        }

        internal T[] CompleteAsArray<T>()
        {
            _session.AssertNotDisposed(); // ensure that the user didn't do an async query then closed the session early
            var queryResult = _currentQueryResults.CreateSnapshot();

            var result = new T[queryResult.Results.Length];

            CompleteInternal<T>(queryResult, Add);

            return result;

            void Add(int index, T r)
            {
                result[index] = r;
            }
        }

        public List<T> Complete<T>()
        {
            _session.AssertNotDisposed(); // ensure that the user didn't do an async query then closed the session early
            var queryResult = _currentQueryResults.CreateSnapshot();

            var result = new List<T>(queryResult.Results.Length);

            CompleteInternal<T>(queryResult, Add);

            return result;

            void Add(int index, T r)
            {
                result.Add(r);
            }
        }

        private void CompleteInternal<T>(QueryResult queryResult, Action<int, T> addToResult)
        {
            queryResult.Results.BlittableValidation();

            if (NoTracking == false)
                _session.RegisterIncludes(queryResult.Includes);

            for (int i = 0; i < queryResult.Results.Length; i++)
            {
                var document = (BlittableJsonReaderObject)queryResult.Results[i];
                BlittableJsonReaderObject metadata;

                try
                {
                    metadata = document.GetMetadata();
                }
                catch (InvalidOperationException)
                {
                    if (document.Count != _facetResultProperties.Length)
                        throw;

                    foreach (var prop in _facetResultProperties)
                    {
                        if (document.TryGetMember(prop.Name, out _) == false)
                            throw;
                    }

                    throw new InvalidOperationException("Raw query with aggregation by facet should be called by " +
                                                        $"{nameof(IRawDocumentQuery<T>.ExecuteAggregation)} or {nameof(IAsyncRawDocumentQuery<T>.ExecuteAggregationAsync)} method.");
                }

                metadata.TryGetId(out var id);

                addToResult(i, Deserialize<T>(id, document, metadata, _fieldsToFetch, NoTracking, _session, _isProjectInto));
            }

            if (NoTracking == false)
            {
                _session.RegisterMissingIncludes(queryResult.Results, queryResult.Includes, queryResult.IncludedPaths);
                if (queryResult.CounterIncludes != null)
                {
                    _session.RegisterCounters(
                        queryResult.CounterIncludes,
                        queryResult.IncludedCounterNames);
                }
                if (queryResult.TimeSeriesIncludes != null)
                {
                    _session.RegisterTimeSeries(
                        queryResult.TimeSeriesIncludes);
                }
                if (queryResult.CompareExchangeValueIncludes != null)
                    _session.GetClusterSession().RegisterCompareExchangeValues(queryResult.CompareExchangeValueIncludes);
            }
        }

        internal static T Deserialize<T>(string id, BlittableJsonReaderObject document, BlittableJsonReaderObject metadata, FieldsToFetchToken fieldsToFetch, bool disableEntitiesTracking, InMemoryDocumentSessionOperations session, bool isProjectInto)
        {
            if (metadata.TryGetProjection(out var projection) == false || projection == false)
                return session.TrackEntity<T>(id, document, metadata, disableEntitiesTracking);

            var type = typeof(T);

            if (fieldsToFetch?.Projections != null && fieldsToFetch.Projections.Length == 1) // we only select a single field
            {
                var typeInfo = type;
                var projectionField = fieldsToFetch.Projections[0];

                if (fieldsToFetch.SourceAlias != null)
                {
                    if (projectionField.StartsWith(fieldsToFetch.SourceAlias))
                    {
                        // remove source-alias from projection name
                        projectionField = projectionField.Substring(fieldsToFetch.SourceAlias.Length + 1);
                    }

                    if (Regex.IsMatch(projectionField, "'([^']*)"))
                    {
                        // projection field is quoted, remove quotes
                        projectionField = projectionField.Substring(1, projectionField.Length - 2);
                    }
                }

                if (type == typeof(string) || typeInfo.IsValueType || typeInfo.IsEnum)
                {
                    return document.TryGet(projectionField, out T value) == false
                        ? default
                        : value;
                }

                var isTimeSeriesField = fieldsToFetch.Projections[0].StartsWith(Constants.TimeSeries.QueryFunction);

                if (isProjectInto == false || isTimeSeriesField)
                {
                    if (document.TryGetMember(projectionField, out object inner) == false)
                        return default;

                    if (isTimeSeriesField ||
                        fieldsToFetch.FieldsToFetch != null &&
                        fieldsToFetch.FieldsToFetch[0] == fieldsToFetch.Projections[0])
                    {
                        if (inner is BlittableJsonReaderObject innerJson)
                        {
                            //extraction from original type
                            document = innerJson;
                        }
                        else if (inner is BlittableJsonReaderArray bjra &&
                                 JavascriptConversionExtensions.LinqMethodsSupport.IsCollection(type))
                        {
                            return DeserializeInnerArray<T>(document, fieldsToFetch.FieldsToFetch?[0], session, bjra);
                        }
                        else if (inner == null)
                        {
                            return default;
                        }
                    }
                }
            }

            if (type == typeof(BlittableJsonReaderObject))
                return (T)(object)document;

            session.OnBeforeConversionToEntityInvoke(id, typeof(T), ref document);
            var result = (T)session.Conventions.Serialization.DeserializeEntityFromBlittable(type, document);
            session.OnAfterConversionToEntityInvoke(id, document, result);

            return result;
        }

        private static ConcurrentDictionary<Type, (Type, PropertyInfo)> _wrapperTypes;
        private const string DummyPropertyName = "Result";

        private static (Type, PropertyInfo) AddWrapperTypeAndPropertyToCache<T>()
        {
            var wrapperType = new
            {
                Result = Activator.CreateInstance<T>()
            }.GetType();

            return (wrapperType, wrapperType.GetProperty(DummyPropertyName));
        }

        private static T DeserializeInnerArray<T>(BlittableJsonReaderObject document, string fieldToFetch, InMemoryDocumentSessionOperations session,
            BlittableJsonReaderArray blittableArray)
        {
            document.Modifications = new DynamicJsonValue(document)
            {
                [DummyPropertyName] = blittableArray
            };

            document.Modifications.Remove(fieldToFetch);

            _wrapperTypes ??= new ConcurrentDictionary<Type, (Type, PropertyInfo)>();

            var (wrapperType, property) = _wrapperTypes.GetOrAdd(typeof(T), AddWrapperTypeAndPropertyToCache<T>());
            var deserialized = session.Conventions.Serialization.DeserializeEntityFromBlittable(wrapperType, document);

            return (T)property.GetValue(deserialized);
        }

        public bool NoTracking { get; set; }

        public void EnsureIsAcceptableAndSaveResult(QueryResult result)
        {
            if (_sp == null)
            {
                EnsureIsAcceptableAndSaveResult(result, duration: null);
            }
            else
            {
                _sp.Stop();
                EnsureIsAcceptableAndSaveResult(result, _sp.Elapsed);
            }
        }

        internal void EnsureIsAcceptableAndSaveResult(QueryResult result, TimeSpan? duration)
        {
            if (result == null)
                throw new IndexDoesNotExistException("Could not find index " + _indexName);

            EnsureIsAcceptable(result, _indexQuery.WaitForNonStaleResults, duration, _session);

            SaveQueryResult(result);
        }

        private void SaveQueryResult(QueryResult result)
        {
            _currentQueryResults = result;

            if (Logger.IsInfoEnabled)
            {
                var isStale = result.IsStale ? "stale " : "";

                StringBuilder parameters = null;

                if (_indexQuery.QueryParameters != null && _indexQuery.QueryParameters.Count > 0)
                {
                    parameters = new StringBuilder();

                    parameters.Append("(parameters: ");

                    var first = true;

                    foreach (var parameter in _indexQuery.QueryParameters)
                    {
                        if (first == false)
                            parameters.Append(", ");

                        parameters.Append(parameter.Key)
                            .Append(" = ")
                            .Append(parameter.Value);

                        first = false;
                    }

                    parameters.Append(") ");
                }

                Logger.Info($"Query '{_indexQuery.Query}' {parameters}returned {result.Results.Items.Count()} {isStale}results (total index results: {result.TotalResults})");
            }
        }

        public static void EnsureIsAcceptable(QueryResult result, bool waitForNonStaleResults, Stopwatch duration, InMemoryDocumentSessionOperations session)
        {
            if (duration == null)
            {
                EnsureIsAcceptable(result, waitForNonStaleResults, (TimeSpan?)null, session);
            }
            else
            {
                duration.Stop();
                EnsureIsAcceptable(result, waitForNonStaleResults, duration.Elapsed, session);
            }
        }

        public static void EnsureIsAcceptable(QueryResult result, bool waitForNonStaleResults, TimeSpan? duration, InMemoryDocumentSessionOperations session)
        {
            if (waitForNonStaleResults && result.IsStale)
            {
                var elapsed = duration == null ? "" : $" {duration.Value.TotalMilliseconds:#,#;;0} ms";
                var msg = $"Waited{elapsed} for the query to return non stale result.";

                throw new TimeoutException(msg);
            }
        }

        public IndexQuery IndexQuery => _indexQuery;
    }
}
