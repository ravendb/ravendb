using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using Raven.Imports.Newtonsoft.Json.Linq;
using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Raven.Abstractions.Linq;
using Raven.Abstractions.Logging;
using Raven.Client.Exceptions;
using Raven.Json.Linq;

namespace Raven.Client.Document.SessionOperations
{
	public class QueryOperation
	{
		private static readonly ILog log = LogProvider.GetCurrentClassLogger();
		private readonly InMemoryDocumentSessionOperations sessionOperations;
		private readonly string indexName;
		private readonly IndexQuery indexQuery;
		private readonly HashSet<KeyValuePair<string, Type>> sortByHints;
		private readonly Action<string, string> setOperationHeaders;
		private readonly bool waitForNonStaleResults;
		private readonly TimeSpan timeout;
		private readonly Func<IndexQuery, IEnumerable<object>, IEnumerable<object>> transformResults;
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

		public QueryOperation(InMemoryDocumentSessionOperations sessionOperations,
			string indexName,
			IndexQuery indexQuery,
			string[] projectionFields,
			HashSet<KeyValuePair<string, Type>> sortByHints,
			bool waitForNonStaleResults,
			Action<string, string> setOperationHeaders,
			TimeSpan timeout,
			Func<IndexQuery, IEnumerable<object>, IEnumerable<object>> transformResults)
		{
			this.indexQuery = indexQuery;
			this.sortByHints = sortByHints;
			this.waitForNonStaleResults = waitForNonStaleResults;
			this.setOperationHeaders = setOperationHeaders;
			this.timeout = timeout;
			this.transformResults = transformResults;
			this.projectionFields = projectionFields;
			this.sessionOperations = sessionOperations;
			this.indexName = indexName;


			AddOperationHeaders();
		}

		private void StartTiming()
		{
			sp = Stopwatch.StartNew();
		}

		public void LogQuery()
		{
			log.DebugFormat("Executing query '{0}' on index '{1}' in '{2}'",
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
			foreach (var include in queryResult.Includes)
			{
				var metadata = include.Value<RavenJObject>("@metadata");

				sessionOperations.TrackEntity<object>(metadata.Value<string>("@id"),
											   include,
											   metadata);
			}
			var list = queryResult.Results
				.Select(Deserialize<T>)
				.ToList();

			if (transformResults == null)
				return list;

			return transformResults(indexQuery, list.Cast<object>()).Cast<T>().ToList();
		}

		private T Deserialize<T>(RavenJObject result)
		{
			var metadata = result.Value<RavenJObject>("@metadata");
			if ((projectionFields == null || projectionFields.Length <= 0)  &&
				(metadata != null && string.IsNullOrEmpty(metadata.Value<string>("@id")) == false) )
			{
				return sessionOperations.TrackEntity<T>(metadata.Value<string>("@id"),
				                                        result,
				                                        metadata);
			}

			if (typeof(T) == typeof(RavenJObject))
				return (T)(object)result;

#if !NET35
			if (typeof(T) == typeof(object) && string.IsNullOrEmpty(result.Value<string>("$type")))
			{
				return (T)(object)new DynamicJsonObject(result);
			}
#endif

			var documentId = result.Value<string>(Constants.DocumentIdFieldName); //check if the result contain the reserved name

			if (!string.IsNullOrEmpty(documentId) && typeof(T) == typeof(string) && // __document_id is present, and result type is a string
			    projectionFields != null && projectionFields.Length == 1 && // We are projecting one field only (although that could be derived from the
			    // previous check, one could never be too careful
			    ((metadata != null && result.Count == 2) || (metadata == null && result.Count == 1)) // there are no more props in the result object
				)
			{
				return (T)(object)documentId;
			}

			HandleInternalMetadata(result);

			var deserializedResult = DeserializedResult<T>(result);

			if (string.IsNullOrEmpty(documentId) == false)
			{
				// we need to make an addtional check, since it is possible that a value was explicitly stated
				// for the identity property, in which case we don't want to override it.
				var identityProperty = sessionOperations.Conventions.GetIdentityProperty(typeof(T));
				if (identityProperty == null ||
				    (result[identityProperty.Name] == null ||
				     result[identityProperty.Name].Type == JTokenType.Null))
				{
					sessionOperations.TrySetIdentity(deserializedResult, documentId);
				}
			}

			return deserializedResult;
		}


		private T DeserializedResult<T>(RavenJObject result)
		{
			if (projectionFields != null && projectionFields.Length == 1) // we only select a single field
			{
				var type = typeof(T);
				if (type == typeof(string) || typeof(T).IsValueType || typeof(T).IsEnum)
				{
					return result.Value<T>(projectionFields[0]);
				}
			}

			var jsonSerializer = sessionOperations.Conventions.CreateSerializer();
			var ravenJTokenReader = new RavenJTokenReader(result);

			var resultTypeString = result.Value<string>("$type");
			if(string.IsNullOrEmpty(resultTypeString) )
			{
				return (T)jsonSerializer.Deserialize(ravenJTokenReader, typeof(T));
			}

			var resultType = Type.GetType(resultTypeString, false);
			if(resultType == null) // couldn't find the type, let us give it our best shot
			{
				return (T)jsonSerializer.Deserialize(ravenJTokenReader, typeof(T));
			}

			return (T) jsonSerializer.Deserialize(ravenJTokenReader, resultType);
		}

		private void HandleInternalMetadata(RavenJObject result)
		{
			// Implant a property with "id" value ... if not exists
			var metadata = result.Value<RavenJObject>("@metadata");
			if (metadata == null || string.IsNullOrEmpty(metadata.Value<string>("@id")))
			{
				// if the item has metadata, then nested items will not have it, so we can skip recursing down
				foreach (var nested in result.Select(property => property.Value))
				{
					var jObject = nested as RavenJObject;
					if (jObject != null)
						HandleInternalMetadata(jObject);
					var jArray = nested as RavenJArray;
					if (jArray == null)
						continue;
					foreach (var item in jArray.OfType<RavenJObject>())
					{
						HandleInternalMetadata(item);
					}
				}
				return;
			}

			var entityName = metadata.Value<string>(Constants.RavenEntityName);

			var idPropName = sessionOperations.Conventions.FindIdentityPropertyNameFromEntityName(entityName);
			if (result.ContainsKey(idPropName))
				return;

			result[idPropName] = new RavenJValue(metadata.Value<string>("@id"));
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
					throw new TimeoutException(
						string.Format("Waited for {0:#,#;;0}ms for the query to return authoritative result.",
									  sp.ElapsedMilliseconds));
				}
				log.DebugFormat(
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
					throw new TimeoutException(
						string.Format("Waited for {0:#,#;;0}ms for the query to return non stale result.",
									  sp.ElapsedMilliseconds));
				}
				log.DebugFormat(
						"Stale query results on non stale query '{0}' on index '{1}' in '{2}', query will be retried, index etag is: {3}",
						indexQuery.Query,
						indexName,
						sessionOperations.StoreIdentifier,
						result.IndexEtag);
				return false;
			}
			currentQueryResults = result;
			currentQueryResults.EnsureSnapshot();
			log.DebugFormat("Query returned {0}/{1} {2}results", result.Results.Count,
											  result.TotalResults, result.IsStale ? "stale " : "");
			return true;
		}

		private void AddOperationHeaders()
		{
			foreach (var sortByHint in sortByHints)
			{
				if (sortByHint.Value == null)
					continue;

				setOperationHeaders(
					string.Format("SortHint-{0}", Uri.EscapeDataString(sortByHint.Key.Trim('-'))),
					FromPrimitiveTypestring(sortByHint.Value.Name).ToString());
			}
		}

		private static SortOptions FromPrimitiveTypestring(string type)
		{
			switch (type)
			{
				case "Int16":
					return SortOptions.Short;
				case "Int32":
					return SortOptions.Int;
				case "Int64":
					return SortOptions.Long;
				case "Double":
				case "Decimal":
					return SortOptions.Double;
				case "Single":
					return SortOptions.Float;
				case "String":
					return SortOptions.String;
				default:
					return SortOptions.String;
			}
		}

	}
}
