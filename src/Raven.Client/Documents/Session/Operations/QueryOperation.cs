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
        private QueryResult _currentQueryResults;
        private readonly FieldsToFetchToken _fieldsToFetch;
        private Stopwatch _sp;
        private static readonly Logger Logger = LoggingSource.Instance.GetLogger<QueryOperation>("Client");

        public QueryResult CurrentQueryResults => _currentQueryResults;

        public QueryOperation(
            InMemoryDocumentSessionOperations session,
            string indexName,
            IndexQuery indexQuery,
            FieldsToFetchToken fieldsToFetch,
            bool disableEntitiesTracking,
            bool metadataOnly = false,
            bool indexEntriesOnly = false)
        {
            _session = session;
            _indexName = indexName;
            _indexQuery = indexQuery;
            _fieldsToFetch = fieldsToFetch;
            NoTracking = disableEntitiesTracking;
            _metadataOnly = metadataOnly;
            _indexEntriesOnly = indexEntriesOnly;

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

        public List<T> Complete<T>()
        {
            _session.AssertNotDisposed(); // ensure that the user didn't do an async query then closed the session early
            var queryResult = _currentQueryResults.CreateSnapshot();
            queryResult.Results.BlittableValidation();

            if (NoTracking == false)
                _session.RegisterIncludes(queryResult.Includes);

            var list = new List<T>();
            foreach (BlittableJsonReaderObject document in queryResult.Results)
            {
                var metadata = document.GetMetadata();

                metadata.TryGetId(out var id);

                list.Add(Deserialize<T>(id, document, metadata, _fieldsToFetch, NoTracking, _session));
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
            }

            return list;
        }

        internal static T Deserialize<T>(string id, BlittableJsonReaderObject document, BlittableJsonReaderObject metadata, FieldsToFetchToken fieldsToFetch, bool disableEntitiesTracking, InMemoryDocumentSessionOperations session)
        {
            if (metadata.TryGetProjection(out var projection) == false || projection == false)
                return session.TrackEntity<T>(id, document, metadata, disableEntitiesTracking);

            if (fieldsToFetch?.Projections != null && fieldsToFetch.Projections.Length == 1) // we only select a single field
            {
                var type = typeof(T);
                var typeInfo = type.GetTypeInfo();
                var projectionField = fieldsToFetch.Projections[0];

                if (fieldsToFetch.SourceAlias != null )
                {
                    if (projectionField.StartsWith(fieldsToFetch.SourceAlias))
                    {
                        // remove source-alias from projection name
                        projectionField = projectionField.Substring(fieldsToFetch.SourceAlias.Length + 1);
                    }
                    if (Regex.IsMatch(projectionField, "'([^']*)")) 
                    {
                        // projection field is quoted, remove quotes
                        projectionField = projectionField.Substring(1, projectionField.Length -2);
                    }
                }

                if (type == typeof(string) || typeInfo.IsValueType || typeInfo.IsEnum)
                {
                    return document.TryGet(projectionField, out T value) == false
                        ? default
                        : value;
                }

                if (document.TryGetMember(projectionField, out object inner) == false)
                    return default;

                if (fieldsToFetch.FieldsToFetch != null && fieldsToFetch.FieldsToFetch[0] == fieldsToFetch.Projections[0])
                {
                    if (inner is BlittableJsonReaderObject innerJson)
                    {
                        //extraction from original type
                        document = innerJson;
                    }
                    else if (inner is BlittableJsonReaderArray bjra && 
                             JavascriptConversionExtensions.LinqMethodsSupport.IsCollection(type))
                    {
                        return DeserializeInnerArray<T>(document, fieldsToFetch.FieldsToFetch[0], session, bjra);
                    }
                }
            }

            var result = (T)session.Conventions.DeserializeEntityFromBlittable(typeof(T), document);

            if (string.IsNullOrEmpty(id) == false)
            {
                // we need to make an additional check, since it is possible that a value was explicitly stated
                // for the identity property, in which case we don't want to override it.
                var identityProperty = session.Conventions.GetIdentityProperty(typeof(T));
                if (identityProperty != null && (document.TryGetMember(identityProperty.Name, out object value) == false || value == null))
                    session.GenerateEntityIdOnTheClient.TrySetIdentity(result, id);
            }

            return result;
        }

        private static ConcurrentDictionary<Type, Type> _wrapperTypes;
        private const string DummyPropertyName = "Result";

        private static T DeserializeInnerArray<T>(BlittableJsonReaderObject document, string fieldToFetch, InMemoryDocumentSessionOperations session,
            BlittableJsonReaderArray blittableArray)
        {
            document.Modifications = new DynamicJsonValue(document)
            {
                [DummyPropertyName] = blittableArray
            };

            document.Modifications.Remove(fieldToFetch);

            _wrapperTypes = _wrapperTypes ?? new ConcurrentDictionary<Type, Type>();

            var wrapperType = _wrapperTypes.GetOrAdd(typeof(T), new
            {
                Result = Activator.CreateInstance<T>()
            }.GetType());

            var property = wrapperType.GetProperty(DummyPropertyName);
            var deserialized = session.Conventions.DeserializeEntityFromBlittable(wrapperType, document);

            return (T)property.GetValue(deserialized);
        }

        [Obsolete("Use NoTracking instead")]
        public bool DisableEntitiesTracking
        {
            get => NoTracking;
            set => NoTracking = value;
        }

        public bool NoTracking { get; set; }

        public void EnsureIsAcceptableAndSaveResult(QueryResult result)
        {
            if (result == null)
                throw new IndexDoesNotExistException("Could not find index " + _indexName);

            EnsureIsAcceptable(result, _indexQuery.WaitForNonStaleResults, _sp, _session);

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
            if (waitForNonStaleResults && result.IsStale)
            {
                duration?.Stop();
                var msg = $"Waited for {duration?.ElapsedMilliseconds:#,#;;0} ms for the query to return non stale result.";

#if TESTING_HANGS
// this code is here because slow tests sometimes how impossible situation
// with thread pauses that are very long, likely because of so much work
// on the system

                Console.WriteLine(msg);
                Console.WriteLine(session.DocumentStore.Database);

                Process.Start(new ProcessStartInfo("cmd", $"/c start \"Stop & look at studio\" \"{session.DocumentStore.Urls[0]}\"")); // Works ok on windows

                Console.ReadLine();
#endif
                throw new TimeoutException(msg);
            }
        }

        public IndexQuery IndexQuery => _indexQuery;
    }
}
