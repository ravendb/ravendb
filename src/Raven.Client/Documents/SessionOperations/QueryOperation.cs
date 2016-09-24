using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text.RegularExpressions;
using Raven.Abstractions.Data;
using Raven.Client.Data;
using Raven.Client.Documents.Commands;
using Raven.Imports.Newtonsoft.Json.Linq;
using Raven.Imports.Newtonsoft.Json.Utilities;
using Raven.Json.Linq;
using Sparrow.Json;
using Sparrow.Logging;

namespace Raven.Client.Documents.SessionOperations
{
    public class QueryOperation
    {
        private readonly InMemoryDocumentSessionOperations _session;
        private readonly string indexName;
        private readonly IndexQuery indexQuery;
        private readonly bool waitForNonStaleResults;
        private bool disableEntitiesTracking;
        private readonly TimeSpan? timeout;
        private readonly Func<IndexQuery, IEnumerable<object>, IEnumerable<object>> transformResults;
        private readonly HashSet<string> includes;
        private QueryResult currentQueryResults;
        private readonly string[] projectionFields;
        private bool firstRequest = true;


        public QueryResult CurrentQueryResults
        {
            get { return currentQueryResults; }
        }

        public string IndexName
        {
            get { return indexName; }
        }

        public IndexQuery IndexQuery
        {
            get { return indexQuery; }
        }

        private Stopwatch sp;
        private static readonly Logger _logger = LoggingSource.Instance.GetLogger<QueryOperation>("Raven.Client");

        public QueryOperation(InMemoryDocumentSessionOperations session, string indexName, IndexQuery indexQuery,
                              string[] projectionFields, bool waitForNonStaleResults, TimeSpan? timeout,
                              Func<IndexQuery, IEnumerable<object>, IEnumerable<object>> transformResults,
                              HashSet<string> includes, bool disableEntitiesTracking)
        {
            this._session = session;
            this.indexName = indexName;
            this.indexQuery = indexQuery;
            this.waitForNonStaleResults = waitForNonStaleResults;
            this.timeout = timeout;
            this.transformResults = transformResults;
            this.includes = includes;
            this.projectionFields = projectionFields;
            this.disableEntitiesTracking = disableEntitiesTracking;

            AssertNotQueryById();
        }

        public QueryCommand CreateRequest()
        {
            _session.IncrementRequestCount();
            if (_logger.IsInfoEnabled)
                _logger.Info($"Querying for the following expression:...");
            return new QueryCommand
            {
                _convention = _session.Conventions,
                indexQuery = this.indexQuery,
                index = indexName,
                includes = new string[0],
                MetadataOnly = false,
                IndexEntriesOnly = false
            };
        }

        public void SetResult(QueryResult queryResult)
        {
            EnsureIsAcceptable(queryResult);

            if (queryResult.Includes != null && queryResult.Includes.Any())
            {
                foreach (BlittableJsonReaderObject document in queryResult.Includes)
                {
                    if (document == null)
                    {
                        // _session.RegisterMissing(includeIds[i]);
                        continue;
                    }

                    BlittableJsonReaderObject metadata;
                    if (document.TryGet(Constants.Metadata.Key, out metadata) == false)
                        throw new InvalidOperationException("Document must have a metadata");
                    string id;
                    long? etag;
                    if (metadata.TryGet(Constants.Metadata.Id, out id) == false)
                        throw new InvalidOperationException("Document must have an id");
                    if (metadata.TryGet(Constants.Metadata.Etag, out etag) == false)
                        throw new InvalidOperationException("Document must have an ETag");
                    var newDocumentInfo = new InMemoryDocumentSessionOperations.DocumentInfo
                    {
                        //TODO
                        Id = id,
                        Document = document,
                        Metadata = metadata,
                        Entity = null,
                        ETag = etag
                    };

                    _session.DocumentsById[id] = newDocumentInfo;
                }
            }

            for (var i = 0; i < queryResult.Results.Length; i++)
            {
                var document = (BlittableJsonReaderObject)queryResult.Results[i];
                if (document == null)
                {
                    // _session.RegisterMissing();
                    continue;
                }

                BlittableJsonReaderObject metadata;
                if (document.TryGet(Constants.Metadata.Key, out metadata) == false)
                    throw new InvalidOperationException("Document must have a metadata");
                string id;
                long? etag;
                if (metadata.TryGet(Constants.Metadata.Id, out id) == false)
                    throw new InvalidOperationException("Document must have an id");
                if (metadata.TryGet(Constants.Metadata.Etag, out etag) == false)
                    throw new InvalidOperationException("Document must have an ETag");
                var newDocumentInfo = new InMemoryDocumentSessionOperations.DocumentInfo
                {
                    //TODO
                    Id = id,
                    Document = document,
                    Metadata = metadata,
                    Entity = null,
                    ETag = etag
                };

                _session.DocumentsById[id] = newDocumentInfo;
            }
        }

        private static readonly Regex idOnly = new Regex(@"^__document_id \s* : \s* ([\w_\-/\\\.]+) \s* $",
            RegexOptions.Compiled |
            RegexOptions.IgnorePatternWhitespace);

        private void AssertNotQueryById()
        {
            // this applies to dynamic indexes only
            if (!indexName.StartsWith("dynamic/", StringComparison.OrdinalIgnoreCase) &&
                !string.Equals(indexName, "dynamic", StringComparison.OrdinalIgnoreCase))
                return;

            var match = idOnly.Match(IndexQuery.Query);
            if (match.Success == false)
                return;

            if (_session.Conventions.AllowQueriesOnId)
                return;

            var value = match.Groups[1].Value;

            throw new InvalidOperationException("Attempt to query by id only is blocked, you should use call session.Load(\"" + value + "\"); instead of session.Query().Where(x=>x.Id == \"" + value + "\");" + Environment.NewLine + "You can turn this error off by specifying documentStore.Conventions.AllowQueriesOnId = true;, but that is not recommend and provided for backward compatibility reasons only.");
        }

        private void StartTiming()
        {
            sp = Stopwatch.StartNew();
        }

        public void LogQuery()
        {
            if (_logger.IsInfoEnabled)
                _logger.Info($"Executing query '{indexQuery.Query}' on index '{indexName}' in '{_session.StoreIdentifier}'");
        }

        public IDisposable EnterQueryContext()
        {
            if (firstRequest)
            {
                StartTiming();
                firstRequest = false;
            }

            if (waitForNonStaleResults == false)
                return null;

            return _session.DocumentStore.DisableAggressiveCaching();
        }

        public IList<T> Complete<T>()
        {
            var queryResult = currentQueryResults.CreateSnapshot();
            foreach (BlittableJsonReaderObject include in queryResult.Includes)
            {
                BlittableJsonReaderObject metadata;
                if (include.TryGet(Constants.Metadata.Key, out metadata) == false)
                    throw new InvalidOperationException("Document must have a metadata");
                string id;
                long? etag;
                if (metadata.TryGet(Constants.Metadata.Id, out id) == false)
                    throw new InvalidOperationException("Document must have an id");
                if (metadata.TryGet(Constants.Metadata.Etag, out etag) == false)
                    throw new InvalidOperationException("Document must have an ETag");

                object entity = _session.ConvertToEntity(typeof(T), id, include);
                //_session.TrackIncludedDocument(entity);
            }

            var usedTransformer = string.IsNullOrEmpty(indexQuery.Transformer) == false;
            var list = new List<T>();
            foreach (BlittableJsonReaderObject result in queryResult.Results)
            {
                if (result == null)
                {
                    list.Add(default(T));
                    continue;
                }

                /*if (usedTransformer)
                {
                    var values = result.Value<RavenJArray>("$values");
                    foreach (RavenJObject value in values)
                        list.Add(Deserialize<T>(value));

                    continue;
                }*/

                BlittableJsonReaderObject metadata;
                if (result.TryGet(Constants.Metadata.Key, out metadata) == false)
                    throw new InvalidOperationException("Document must have a metadata");
                string id;
                long? etag;
                if (metadata.TryGet(Constants.Metadata.Id, out id) == false)
                    throw new InvalidOperationException("Document must have an id");
                if (metadata.TryGet(Constants.Metadata.Etag, out etag) == false)
                    throw new InvalidOperationException("Document must have an ETag");

                object entity = _session.ConvertToEntity(typeof(T), id, result);

                //list.Add(Deserialize<T>(result));
                list.Add((T)entity);
            }

            if (disableEntitiesTracking == false)
                //_session.RegisterMissingIncludes(queryResult.Results.Where(x => x != null), indexQuery.Includes);

            if (transformResults == null)
                return list;

            return transformResults(indexQuery, list.Cast<object>()).Cast<T>().ToList();
        }

        public bool DisableEntitiesTracking
        {
            get { return disableEntitiesTracking; }
            set { disableEntitiesTracking = value; }
        }

        public T Deserialize<T>(RavenJObject result)
        {
            /*var metadata = result.Value<RavenJObject>("@metadata");
            if ((projectionFields == null || projectionFields.Length <= 0) &&
                (metadata != null && string.IsNullOrEmpty(metadata.Value<string>("@id")) == false))
            {
                return _session.TrackEntity<T>(metadata.Value<string>("@id"),
                                                        result,
                                                        metadata, disableEntitiesTracking);
            }

            if (typeof(T) == typeof(RavenJObject))
                return (T)(object)result;

            if (typeof(T) == typeof(object) && string.IsNullOrEmpty(result.Value<string>("$type")))
            {
                return (T)(object)new DynamicJsonObject(result);
            }

            var documentId = result.Value<string>(Constants.Indexing.Fields.DocumentIdFieldName); //check if the result contain the reserved name

            if (!string.IsNullOrEmpty(documentId) && typeof(T) == typeof(string) && // __document_id is present, and result type is a string
                                                                                    // We are projecting one field only (although that could be derived from the
                                                                                    // previous check, one could never be too careful
                projectionFields != null && projectionFields.Length == 1 &&
                HasSingleValidProperty(result, metadata) // there are no more props in the result object
                )
            {
                return (T)(object)documentId;
            }

            _session.HandleInternalMetadata(result);

            var deserializedResult = DeserializedResult<T>(result);

            if (string.IsNullOrEmpty(documentId) == false)
            {
                // we need to make an additional check, since it is possible that a value was explicitly stated
                // for the identity property, in which case we don't want to override it.
                var identityProperty = _session.Conventions.GetIdentityProperty(typeof(T));
                if (identityProperty != null &&
                    (result[identityProperty.Name] == null ||
                     result[identityProperty.Name].Type == JTokenType.Null))
                {
                    _session.GenerateEntityIdOnTheClient.TrySetIdentity(deserializedResult, documentId);
                }
            }

            return deserializedResult;*/
            return (T)new object();
        }

        private bool HasSingleValidProperty(RavenJObject result, RavenJObject metadata)
        {
            if (metadata == null && result.Count == 1)
                return true;// { Foo: val }

            if ((metadata != null && result.Count == 2))
                return true; // { @metadata: {}, Foo: val }

            if ((metadata != null && result.Count == 3))
            {
                var entityName = metadata.Value<string>(Constants.Headers.RavenEntityName);

                var idPropName = _session.Conventions.FindIdentityPropertyNameFromEntityName(entityName);

                if (result.ContainsKey(idPropName))
                {
                    // when we try to project the id by name
                    var token = result.Value<RavenJToken>(idPropName);

                    if (token == null || token.Type == JTokenType.Null)
                        return true; // { @metadata: {}, Foo: val, Id: null }
                }
            }

            return false;
        }


        private T DeserializedResult<T>(RavenJObject result)
        {
            if (projectionFields != null && projectionFields.Length == 1) // we only select a single field
            {
                var type = typeof(T);
                if (type == typeof(string) || typeof(T).IsValueType() || typeof(T).IsEnum())
                {
                    return result.Value<T>(projectionFields[0]);
                }
            }

            var jsonSerializer = _session.Conventions.CreateSerializer();
            var ravenJTokenReader = new RavenJTokenReader(result);

            var resultTypeString = result.Value<string>("$type");
            if (string.IsNullOrEmpty(resultTypeString))
            {
                return (T)jsonSerializer.Deserialize(ravenJTokenReader, typeof(T));
            }

            var resultType = Type.GetType(resultTypeString, false);
            if (resultType == null) // couldn't find the type, let us give it our best shot
            {
                return (T)jsonSerializer.Deserialize(ravenJTokenReader, typeof(T));
            }

            return (T)jsonSerializer.Deserialize(ravenJTokenReader, resultType);
        }

        public void ForceResult(QueryResult result)
        {
            currentQueryResults = result;
            currentQueryResults.EnsureSnapshot();
        }

        public void EnsureIsAcceptable(QueryResult result)
        {
            if (waitForNonStaleResults && result.IsStale)
            {
                sp.Stop();

                throw new TimeoutException(
                    string.Format("Waited for {0:#,#;;0}ms for the query to return non stale result.",
                        sp.ElapsedMilliseconds));
            }

            currentQueryResults = result;
            currentQueryResults.EnsureSnapshot();

            if (_logger.IsInfoEnabled)
            {
                var isStale = result.IsStale ? "stale " : "";
                _logger.Info($"Query returned {result.Results.Items.Count()}/{result.TotalResults} {isStale}results");
            }
        }
    }
}