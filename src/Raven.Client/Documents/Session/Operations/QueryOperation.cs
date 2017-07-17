using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Reflection;
using System.Text.RegularExpressions;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Exceptions.Indexes;
using Raven.Client.Documents.Queries;
using Raven.Client.Documents.Transformers;
using Raven.Client.Extensions;
using Sparrow.Json;
using Sparrow.Logging;

namespace Raven.Client.Documents.Session.Operations
{
    public class QueryOperation
    {
        private readonly InMemoryDocumentSessionOperations _session;
        private readonly string _indexName;
        private readonly string _collectionName;
        private readonly IndexQuery _indexQuery;
        private readonly bool _waitForNonStaleResults;
        private readonly bool _metadataOnly;
        private readonly bool _indexEntriesOnly;
        private readonly TimeSpan? _timeout;
        private readonly Func<IndexQuery, IEnumerable<object>, IEnumerable<object>> _transformResults;
        private readonly HashSet<string> _includes;
        private QueryResult _currentQueryResults;
        private readonly string[] _projectionFields;
        private Stopwatch _sp;
        private static readonly Logger Logger = LoggingSource.Instance.GetLogger<QueryOperation>("Raven.NewClient.Client");

        public QueryResult CurrentQueryResults => _currentQueryResults;

        public QueryOperation(InMemoryDocumentSessionOperations session, string indexName, string collectionName, IndexQuery indexQuery,
                              string[] projectionFields, bool waitForNonStaleResults, TimeSpan? timeout,
                              Func<IndexQuery, IEnumerable<object>, IEnumerable<object>> transformResults,
                              HashSet<string> includes, bool disableEntitiesTracking, bool metadataOnly = false, bool indexEntriesOnly = false)
        {
            _session = session;
            _indexName = indexName;
            _collectionName = collectionName;
            _indexQuery = indexQuery;
            _waitForNonStaleResults = waitForNonStaleResults;
            _timeout = timeout;
            _transformResults = transformResults;
            _includes = includes;
            _projectionFields = projectionFields;
            DisableEntitiesTracking = disableEntitiesTracking;
            _metadataOnly = metadataOnly;
            _indexEntriesOnly = indexEntriesOnly;

            AssertNotQueryById();
            AssertPageSizeSet();
        }

        public QueryCommand CreateRequest()
        {
            _session.IncrementRequestCount();
            LogQuery();

            return new QueryCommand(_session.Conventions, _session.Context, _indexQuery, _metadataOnly, _indexEntriesOnly);
        }

        public void SetResult(QueryResult queryResult)
        {
            EnsureIsAcceptableAndSaveResult(queryResult);
        }

        private static readonly Regex IdOnly = new Regex(@"^__document_id \s* : \s* ([\w_\-/\\\.]+) \s* $",
            RegexOptions.Compiled |
            RegexOptions.IgnorePatternWhitespace);

        private void AssertNotQueryById()
        {
            // this applies to dynamic indexes only
            if (_collectionName != null)
                return;

            var match = IdOnly.Match(_indexQuery.Query);
            if (match.Success == false)
                return;

            if (_session.Conventions.AllowQueriesOnId)
                return;

            var value = match.Groups[1].Value;

            throw new InvalidOperationException("Attempt to query by id only is blocked, you should use call session.Load(\"" + value + "\"); instead of session.Query().Where(x=>x.Id == \"" + value + "\");" + Environment.NewLine + "You can turn this error off by specifying documentStore.Conventions.AllowQueriesOnId = true;, but that is not recommend and provided for backward compatibility reasons only.");
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

            if (_waitForNonStaleResults == false)
                return null;

            return _session.DocumentStore.DisableAggressiveCaching();
        }

        public IList<T> Complete<T>()
        {
            var queryResult = _currentQueryResults.CreateSnapshot();
            foreach (BlittableJsonReaderObject include in queryResult.Includes)
            {
                if (include == null)
                    continue;

                var newDocumentInfo = DocumentInfo.GetNewDocumentInfo(include);
                _session.IncludedDocumentsById[newDocumentInfo.Id] = newDocumentInfo;
            }

            var usedTransformer = string.IsNullOrEmpty(_indexQuery.Transformer) == false;
            List<T> list;
            if (usedTransformer)
            {
                list = TransformerHelper.ParseResultsForQueryOperation<T>(_session, queryResult).ToList();
            }
            else
            {
                list = new List<T>();
                foreach (BlittableJsonReaderObject document in queryResult.Results)
                {
                    var metadata = document.GetMetadata();

                    string id;
                    metadata.TryGetId(out id);

                    list.Add(Deserialize<T>(id, document, metadata, _projectionFields, DisableEntitiesTracking, _session));
                }
            }

            if (DisableEntitiesTracking == false)
                _session.RegisterMissingIncludes(queryResult.Results, _includes);

            if (_transformResults == null)
                return list;

            return _transformResults(_indexQuery, list.Cast<object>()).Cast<T>().ToList();
        }

        internal static T Deserialize<T>(string id, BlittableJsonReaderObject document, BlittableJsonReaderObject metadata, string[] projectionFields, bool disableEntitiesTracking, InMemoryDocumentSessionOperations session)
        {
            if (projectionFields == null || projectionFields.Length == 0)
                return session.TrackEntity<T>(id, document, metadata, disableEntitiesTracking);

            if (projectionFields.Length == 1) // we only select a single field
            {
                var type = typeof(T);
                var typeInfo = type.GetTypeInfo();
                if (type == typeof(string) || typeInfo.IsValueType || typeInfo.IsEnum)
                {
                    var projectionField = projectionFields[0];
                    T value;
                    return document.TryGet(projectionField, out value) == false
                        ? default(T)
                        : value;
                }
            }

            var result = (T)session.Conventions.DeserializeEntityFromBlittable(typeof(T), document);

            if (string.IsNullOrEmpty(id) == false)
            {
                // we need to make an additional check, since it is possible that a value was explicitly stated
                // for the identity property, in which case we don't want to override it.
                object value;
                var identityProperty = session.Conventions.GetIdentityProperty(typeof(T));
                if (identityProperty != null && (document.TryGetMember(identityProperty.Name, out value) == false || value == null))
                    session.GenerateEntityIdOnTheClient.TrySetIdentity(result, id);
            }

            return result;
        }

        public bool DisableEntitiesTracking { get; set; }

        public void EnsureIsAcceptableAndSaveResult(QueryResult result)
        {
            if(result == null)
                throw new IndexDoesNotExistException("Could not find index " + _indexName);

            if (_waitForNonStaleResults && result.IsStale)
            {
                if (_sp.Elapsed > _timeout)
                {
                    _sp.Stop();
                    var msg = $"Waited for {_sp.ElapsedMilliseconds:#,#;;0}ms for the query to return non stale result.";
                    
                    throw new TimeoutException(msg);
                }
            }

            _currentQueryResults = result;
            _currentQueryResults.EnsureSnapshot();

            if (Logger.IsInfoEnabled)
            {
                var isStale = result.IsStale ? "stale " : "";
                Logger.Info($"Query returned {result.Results.Items.Count()}/{result.TotalResults} {isStale}results");
            }
        }

        public IndexQuery IndexQuery => _indexQuery;
    }
}