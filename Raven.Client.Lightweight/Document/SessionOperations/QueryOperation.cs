using System;
using System.Collections.Generic;
using System.Diagnostics;
using NLog;
using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;

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

#if !SILVERLIGHT
		private Stopwatch sp;
#else
		private	DateTime startTime;
#endif

		public QueryOperation(InMemoryDocumentSessionOperations sessionOperations, 
			string indexName, 
			IndexQuery indexQuery,
			HashSet<KeyValuePair<string, Type>> sortByHints,
			bool waitForNonStaleResults, 
			Action<string,string> setOperationHeaders,
			TimeSpan timeout)
		{
			this.indexQuery = indexQuery;
			this.sortByHints = sortByHints;
			this.waitForNonStaleResults = waitForNonStaleResults;
			this.setOperationHeaders = setOperationHeaders;
			this.timeout = timeout;
			this.sessionOperations = sessionOperations;
			this.indexName = indexName;


			AddOperationHeaders();

#if !SILVERLIGHT
			sp = Stopwatch.StartNew();
#else
			startTime = DateTime.Now;
#endif
		}

		public void LogQuery()
		{
			log.Debug("Executing query '{0}' on index '{1}' in '{2}'",
										  indexQuery.Query, indexName, sessionOperations.StoreIdentifier);
		}

		public IDisposable EnterQueryContext()
		{
			if (waitForNonStaleResults == false)
				return null;

			return sessionOperations.DocumentStore.DisableAggressiveCaching();
		}

		public bool ShouldQueryAgain(QueryResult result)
		{
			if (waitForNonStaleResults && result.IsStale)
			{
#if !SILVERLIGHT
				if (sp.Elapsed > timeout)
#else
				var elapsed = (DateTime.Now - startTime);
				if (elapsed > timeout)
#endif
				{
#if !SILVERLIGHT
					sp.Stop();
#endif
					throw new TimeoutException(
						string.Format("Waited for {0:#,#}ms for the query to return non stale result.",
#if !SILVERLIGHT
									  sp.ElapsedMilliseconds));
#else
						              elapsed.TotalMilliseconds));
#endif
				}
				log.Debug(
						"Stale query results on non stable query '{0}' on index '{1}' in '{2}', query will be retried",
						indexQuery.Query, indexName, sessionOperations.StoreIdentifier);
				return true;
			}
			log.Debug("Query returned {0}/{1} {2}results", result.Results.Count,
											  result.TotalResults, result.IsStale ? "stale " : "");
			return false;
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