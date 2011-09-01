using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using NLog;
using Newtonsoft.Json.Linq;
using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Raven.Abstractions.Linq;
using Raven.Client.Exceptions;
using Raven.Json.Linq;

namespace Raven.Client.Document.SessionOperations
{
	public class QueryOperation
	{
		private static readonly Logger log = LogManager.GetCurrentClassLogger();
		private readonly InMemoryDocumentSessionOperations sessionOperations;
		private readonly string indexName;
		private readonly IndexQuery indexQuery;
		private readonly HashSet<KeyValuePair<string, Type>> sortByHints;
		private readonly Action<string, string> setOperationHeaders;
		private readonly bool waitForNonStaleResults;
		private readonly TimeSpan timeout;
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
			TimeSpan timeout)
		{
			this.indexQuery = indexQuery;
			this.sortByHints = sortByHints;
			this.waitForNonStaleResults = waitForNonStaleResults;
			this.setOperationHeaders = setOperationHeaders;
			this.timeout = timeout;
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
			if (e is NonAuthoritiveInformationException == false)
				return false;

			return sp.Elapsed <= sessionOperations.NonAuthoritiveInformationTimeout;
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

			return list;
		}

		private T Deserialize<T>(RavenJObject result)
		{
			var metadata = result.Value<RavenJObject>("@metadata");
			if (
				// we asked for a projection directly from the index
				projectionFields != null && projectionFields.Length > 0
				// we got a document without an @id
				// we aren't querying a document, we are probably querying a map reduce index result or a projection
			   || metadata == null || string.IsNullOrEmpty(metadata.Value<string>("@id")))
			{
				if (typeof(T) == typeof(RavenJObject))
					return (T)(object)result;

#if !NET_3_5
				if (typeof(T) == typeof(object))
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
			return sessionOperations.TrackEntity<T>(metadata.Value<string>("@id"),
										  result,
										  metadata);
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
			return (T)sessionOperations.Conventions.CreateSerializer().Deserialize(new RavenJTokenReader(result), typeof(T));
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

		public bool IsAcceptable(QueryResult result)
		{
			if (waitForNonStaleResults && result.IsStale)
			{
				if (sp.Elapsed > timeout)
				{
					sp.Stop();
					throw new TimeoutException(
						string.Format("Waited for {0:#,#}ms for the query to return non stale result.",
									  sp.ElapsedMilliseconds));
				}
				log.Debug(
						"Stale query results on non stale query '{0}' on index '{1}' in '{2}', query will be retried",
						indexQuery.Query, indexName, sessionOperations.StoreIdentifier);
				return false;
			}
			currentQueryResults = result;
			currentQueryResults.EnsureSnapshot();
			log.Debug("Query returned {0}/{1} {2}results", result.Results.Count,
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