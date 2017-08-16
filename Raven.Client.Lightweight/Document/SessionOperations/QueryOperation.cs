using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Reflection;
using System.Text.RegularExpressions;
using Raven.Client.Connection;
using Raven.Imports.Newtonsoft.Json.Linq;
using Raven.Abstractions.Data;
using Raven.Abstractions.Linq;
using Raven.Abstractions.Logging;
using Raven.Client.Exceptions;
using Raven.Json.Linq;
using Raven.Abstractions.Extensions;

namespace Raven.Client.Document.SessionOperations
{
    public class QueryOperation
    {
#if !DNXCORE50
        private readonly static ILog log = LogManager.GetCurrentClassLogger();
#else
        private readonly static ILog log = LogManager.GetLogger(typeof(QueryOperation));
#endif

        private readonly InMemoryDocumentSessionOperations sessionOperations;
        private readonly string indexName;
        private readonly IndexQuery indexQuery;
        private readonly bool waitForNonStaleResults;
        private bool disableEntitiesTracking;
        private readonly TimeSpan timeout;
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

        public QueryOperation(InMemoryDocumentSessionOperations sessionOperations, string indexName, IndexQuery indexQuery,
                              string[] projectionFields, bool waitForNonStaleResults, TimeSpan timeout,
                              Func<IndexQuery, IEnumerable<object>, IEnumerable<object>> transformResults,
                              HashSet<string> includes, bool disableEntitiesTracking)
        {
            this.indexQuery = indexQuery;
            this.waitForNonStaleResults = waitForNonStaleResults;
            this.timeout = timeout;
            this.transformResults = transformResults;
            this.includes = includes;
            this.projectionFields = projectionFields;
            this.sessionOperations = sessionOperations;
            this.indexName = indexName;
            this.disableEntitiesTracking = disableEntitiesTracking;

            AssertNotQueryById();
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

            if (sessionOperations.Conventions.AllowQueriesOnId)
                return;

            var value = match.Groups[1].Value.Replace("\\/","/");

            throw new InvalidOperationException("Attempt to query by id only is blocked, you should use call session.Load(\"" + value + "\"); instead of session.Query().Where(x=>x.Id == \"" + value + "\");" + Environment.NewLine + "You can turn this error off by specifying documentStore.Conventions.AllowQueriesOnId = true;, but that is not recommend and provided for backward compatibility reasons only.");
        }

        private void StartTiming()
        {
            sp = Stopwatch.StartNew();
        }

        public void LogQuery()
        {
            if (log.IsDebugEnabled)
            log.Debug("Executing query '{0}' on index '{1}' in '{2}'",
                                          indexQuery.Query, indexName, sessionOperations.StoreIdentifier);
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

            return sessionOperations.DocumentStore.DisableAggressiveCaching();
        }

        public bool ShouldQueryAgain(Exception e)
        {
            if (e is NonAuthoritativeInformationException == false)
                return false;

            return sp.Elapsed <= sessionOperations.NonAuthoritativeInformationTimeout;
        }

        public IList<T> Complete<T>()
        {
            var queryResult = currentQueryResults.CreateSnapshot();
            foreach (var include in SerializationHelper.RavenJObjectsToJsonDocuments(queryResult.Includes))
            {
                sessionOperations.TrackIncludedDocument(include);
            }

            var list = queryResult.Results
                .Select(x => x != null ? Deserialize<T>(x) : default(T))
                .ToList();

            if (disableEntitiesTracking == false)
                sessionOperations.RegisterMissingIncludes(queryResult.Results.Where(x => x != null), includes);

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
            var metadata = result.Value<RavenJObject>("@metadata");
            if ((projectionFields == null || projectionFields.Length <= 0) &&
                (metadata != null && string.IsNullOrEmpty(metadata.Value<string>("@id")) == false))
            {
                return sessionOperations.TrackEntity<T>(metadata.Value<string>("@id"),
                                                        result,
                                                        metadata, disableEntitiesTracking);
            }

            if (typeof(T) == typeof(RavenJObject))
                return (T)(object)result;

            if (typeof(T) == typeof(object) && string.IsNullOrEmpty(result.Value<string>("$type")))
            {
                return (T)(object)new DynamicJsonObject(result);
            }

            var documentId = result.Value<string>(Constants.DocumentIdFieldName); //check if the result contain the reserved name

            if (!string.IsNullOrEmpty(documentId) && typeof(T) == typeof(string) && // __document_id is present, and result type is a string
                // We are projecting one field only (although that could be derived from the
                // previous check, one could never be too careful
                projectionFields != null && projectionFields.Length == 1 && 
                HasSingleValidProperty(result, metadata) // there are no more props in the result object
                )
            {
                return (T)(object)documentId;
            }

            sessionOperations.HandleInternalMetadata(result);

            var deserializedResult = DeserializedResult<T>(result);

            if (string.IsNullOrEmpty(documentId) == false)
            {
                // we need to make an additional check, since it is possible that a value was explicitly stated
                // for the identity property, in which case we don't want to override it.
                var identityProperty = sessionOperations.Conventions.GetIdentityProperty(typeof(T));
                if (identityProperty != null &&
                    (result[identityProperty.Name] == null ||
                     result[identityProperty.Name].Type == JTokenType.Null))
                {
                    sessionOperations.GenerateEntityIdOnTheClient.TrySetIdentity(deserializedResult, documentId);
                }
            }

            return deserializedResult;
        }

        private bool HasSingleValidProperty(RavenJObject result, RavenJObject metadata)
        {
            if (metadata == null && result.Count == 1)
                return true;// { Foo: val }

            if ((metadata != null && result.Count == 2))
                return true; // { @metadata: {}, Foo: val }

            if ((metadata != null && result.Count == 3))
            {
                var entityName = metadata.Value<string>(Constants.RavenEntityName);

                var idPropName = sessionOperations.Conventions.FindIdentityPropertyNameFromEntityName(entityName);

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
            var type = typeof(T);
            if (type == typeof(string) || typeof(T).IsValueType() || typeof(T).IsEnum())
            {
                if (projectionFields != null && projectionFields.Length == 1) // we only select a single field
                {
                    return result.Value<T>(projectionFields[0]);
                }
                switch (result.Count)
                {
                    case 1:
                        return result.Value<T>(result.Keys.First());
                    case 2:
                        if(result.ContainsKey(Constants.Metadata))
                            return result.Value<T>(result.Keys.First(x=>x!=Constants.Metadata));
                        break;
                }
            }

            var jsonSerializer = sessionOperations.Conventions.CreateSerializer();
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

        public bool IsAcceptable(QueryResult result)
        {
            if (sessionOperations.AllowNonAuthoritativeInformation == false &&
                result.NonAuthoritativeInformation)
            {
                if (sp.Elapsed > sessionOperations.NonAuthoritativeInformationTimeout)
                {
                    sp.Stop();
                    throw new TimeoutException(string.Format("Waited for {0:#,#;;0}ms for the query to return authoritative result.", sp.ElapsedMilliseconds));
                }

                if (log.IsDebugEnabled)
                log.Debug(
                        "Non authoritative query results on authoritative query '{0}' on index '{1}' in '{2}', query will be retried, index etag is: {3}",
                            indexQuery.Query,
                            indexName,
                            sessionOperations.StoreIdentifier,
                            result.IndexEtag);
                return false;
            }
            if (waitForNonStaleResults && result.IsStale)
            {
                if (sp.Elapsed > timeout)
                {
                    sp.Stop();

                    throw new TimeoutException(string.Format("Waited for {0:#,#;;0}ms for the query to return non stale result.", sp.ElapsedMilliseconds));
                }

                if (log.IsDebugEnabled)
                log.Debug(
                        "Stale query results on non stale query '{0}' on index '{1}' in '{2}', query will be retried, index etag is: {3}",
                            indexQuery.Query,
                            indexName,
                            sessionOperations.StoreIdentifier,
                            result.IndexEtag);
                return false;
            }
            currentQueryResults = result;
            currentQueryResults.EnsureSnapshot();
            if (log.IsDebugEnabled)
            log.Debug("Query returned {0}/{1} {2}results", result.Results.Count,
                                              result.TotalResults, result.IsStale ? "stale " : "");
            return true;
        }
    }
}
