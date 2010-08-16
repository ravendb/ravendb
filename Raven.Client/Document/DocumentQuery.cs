using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading;
using Newtonsoft.Json.Linq;
using Raven.Client.Client;
using Raven.Client.Linq;
using Raven.Database.Data;
using Raven.Database.Indexing;

namespace Raven.Client.Document
{
	public class DocumentQuery<T> : IDocumentQuery<T>
	{
		private readonly IDatabaseCommands databaseCommands;
		private readonly string indexName;
		private readonly string[] projectionFields;
		private readonly DocumentSession session;
		private DateTime? cutoff;
		private string[] orderByFields = new string[0];
		private int pageSize = 128;
		private QueryResult queryResult;
		private StringBuilder queryText = new StringBuilder();
		private int start;
		private TimeSpan timeout;
		private bool waitForNonStaleResults;
		private readonly HashSet<string> includes = new HashSet<string>();

		// spatial hack
		protected double lat, lng, radius;

		public DocumentQuery(DocumentSession session, IDatabaseCommands databaseCommands, string indexName,
		                     string[] projectionFields)
		{
			this.databaseCommands = databaseCommands;
			this.projectionFields = projectionFields;
			this.indexName = indexName;
			this.session = session;
		}

		#region IDocumentQuery<T> Members

		public IDocumentQuery<TProjection> SelectFields<TProjection>(string[] fields)
		{
			return new DocumentQuery<TProjection>(session, databaseCommands, indexName, fields)
			{
				pageSize = pageSize,
				queryText = new StringBuilder(queryText.ToString()),
				start = start,
				timeout = timeout,
				cutoff = cutoff,
				waitForNonStaleResults = waitForNonStaleResults,
				orderByFields = orderByFields,
			};
		}

		public IDocumentQuery<T> WaitForNonStaleResults(TimeSpan waitTimeout)
		{
			waitForNonStaleResults = true;
			timeout = waitTimeout;
			return this;
		}

		public QueryResult QueryResult
		{
			get { return queryResult ?? (queryResult = GetQueryResult()); }
		}

		public IEnumerable<string> ProjectionFields
		{
			get { return this.projectionFields ?? Enumerable.Empty<string>(); }
		}

		public IDocumentQuery<T> AddOrder(string fieldName, bool descending)
		{
			fieldName = descending ? "-" + fieldName : fieldName;
			orderByFields = orderByFields.Concat(new[] {fieldName}).ToArray();
			return this;
		}

		public IEnumerator<T> GetEnumerator()
		{
			foreach (var include in QueryResult.Includes)
			{
				var metadata = include.Value<JObject>("@metadata");
				
				session.TrackEntity<object>(metadata.Value<string>("@id"),
										 include,
										 metadata);
			}
			return QueryResult.Results
				.Select(Deserialize)
				.GetEnumerator();
		}


		IEnumerator IEnumerable.GetEnumerator()
		{
			return GetEnumerator();
		}

		public IDocumentQuery<T> Include(string path)
		{
			includes.Add(path);
			return this;
		}

		public IDocumentQuery<T> Take(int count)
		{
			pageSize = count;
			return this;
		}

		public IDocumentQuery<T> Skip(int count)
		{
			start = count;
			return this;
		}

		public IDocumentQuery<T> Where(string whereClause)
		{
			if (queryText.Length > 0)
			{
				queryText.Append(" ");
			}

			queryText.Append(whereClause);
			return this;
		}

		/// <summary>
		/// 	Matches exact value
		/// </summary>
		/// <param name = "fieldName"></param>
		/// <param name = "value"></param>
		/// <returns></returns>
		/// <remarks>
		/// 	Defaults to NotAnalyzed
		/// </remarks>
		public IDocumentQuery<T> WhereEquals(string fieldName, object value)
		{
			return this.WhereEquals(fieldName, value, false, false);
		}

		/// <summary>
		/// 	Matches exact value
		/// </summary>
		/// <param name = "fieldName"></param>
		/// <param name = "value"></param>
		/// <param name = "isAnalyzed"></param>
		/// <returns></returns>
		/// <remarks>
		/// 	Defaults to allow wildcards only if analyzed
		/// </remarks>
		public IDocumentQuery<T> WhereEquals(string fieldName, object value, bool isAnalyzed)
		{
			return this.WhereEquals(fieldName, value, isAnalyzed, isAnalyzed);
		}

		/// <summary>
		/// 	Matches exact value
		/// </summary>
		/// <returns></returns>
		public IDocumentQuery<T> WhereEquals(string fieldName, object value, bool isAnalyzed, bool allowWildcards)
		{
			if (queryText.Length > 0)
			{
				queryText.Append(" ");
			}

			queryText.Append(fieldName);
			queryText.Append(":");
			queryText.Append(TransformToEqualValue(value, isAnalyzed, isAnalyzed));

			return this;
		}

		/// <summary>
		/// 	Matches substrings of the field
		/// </summary>
		/// <param name = "fieldName"></param>
		/// <param name = "value"></param>
		/// <returns></returns>
		public IDocumentQuery<T> WhereContains(string fieldName, object value)
		{
			return this.WhereEquals(fieldName, value, true, true);
		}

		public IDocumentQuery<T> WhereStartsWith(string fieldName, object value)
		{
			// NOTE: doesn't fully match StartsWith semantics
			return this.WhereEquals(fieldName, String.Concat(value, "*"), true, true);
		}

		public IDocumentQuery<T> WhereEndsWith(string fieldName, object value)
		{
			// http://lucene.apache.org/java/2_4_0/queryparsersyntax.html#Wildcard%20Searches
			// You cannot use a * or ? symbol as the first character of a search

			// NOTE: doesn't fully match EndsWith semantics
			return this.WhereEquals(fieldName, String.Concat("*", value), true, true);
		}

		public IDocumentQuery<T> WhereBetween(string fieldName, object start, object end)
		{
			if (queryText.Length > 0)
			{
				queryText.Append(" ");
			}

			queryText.Append(fieldName).Append(":{");
			queryText.Append(start == null ? "*" : TransformToRangeValue(start));
			queryText.Append(" TO ");
			queryText.Append(end == null ? "NULL" : TransformToRangeValue(end));
			queryText.Append("}");

			return this;
		}

		public IDocumentQuery<T> WhereBetweenOrEqual(string fieldName, object start, object end)
		{
			if (queryText.Length > 0)
			{
				queryText.Append(" ");
			}

			queryText.Append(fieldName).Append(":[");
			queryText.Append(start == null ? "*" : TransformToRangeValue(start));
			queryText.Append(" TO ");
			queryText.Append(end == null ? "NULL" : TransformToRangeValue(end));
			queryText.Append("]");

			return this;
		}

		public IDocumentQuery<T> WhereGreaterThan(string fieldName, object value)
		{
			return this.WhereBetween(fieldName, value, null);
		}

		public IDocumentQuery<T> WhereGreaterThanOrEqual(string fieldName, object value)
		{
			return this.WhereBetweenOrEqual(fieldName, value, null);
		}

		public IDocumentQuery<T> WhereLessThan(string fieldName, object value)
		{
			return this.WhereBetween(fieldName, null, value);
		}

		public IDocumentQuery<T> WhereLessThanOrEqual(string fieldName, object value)
		{
			return this.WhereBetweenOrEqual(fieldName, null, value);
		}

		public IDocumentQuery<T> AndAlso()
		{
			if (this.queryText.Length < 1)
			{
				throw new InvalidOperationException("Missing where clause");
			}

			queryText.Append(" AND");
			return this;
		}

		public IDocumentQuery<T> OrElse()
		{
			if (this.queryText.Length < 1)
			{
				throw new InvalidOperationException("Missing where clause");
			}

			queryText.Append(" OR");
			return this;
		}

		/// <summary>
		/// 	Specifies a boost weight to the last where clause.
		/// 	The higher the boost factor, the more relevant the term will be.
		/// </summary>
		/// <param name = "boost">boosting factor where 1.0 is default, less than 1.0 is lower weight, greater than 1.0 is higher weight</param>
		/// <returns></returns>
		/// <remarks>
		/// 	http://lucene.apache.org/java/2_4_0/queryparsersyntax.html#Boosting%20a%20Term
		/// </remarks>
		public IDocumentQuery<T> Boost(decimal boost)
		{
			if (this.queryText.Length < 1)
			{
				throw new InvalidOperationException("Missing where clause");
			}

			if (boost <= 0m)
			{
				throw new ArgumentOutOfRangeException("Boost factor must be a positive number");
			}

			if (boost != 1m)
			{
				// 1.0 is the default
				this.queryText.Append("^").Append(boost);
			}

			return this;
		}

		/// <summary>
		/// 	Specifies a fuzziness factor to the single word term in the last where clause
		/// </summary>
		/// <param name = "fuzzy">0.0 to 1.0 where 1.0 means closer match</param>
		/// <returns></returns>
		/// <remarks>
		/// 	http://lucene.apache.org/java/2_4_0/queryparsersyntax.html#Fuzzy%20Searches
		/// </remarks>
		public IDocumentQuery<T> Fuzzy(decimal fuzzy)
		{
			if (this.queryText.Length < 1)
			{
				throw new InvalidOperationException("Missing where clause");
			}

			if (fuzzy < 0m || fuzzy > 1m)
			{
				throw new ArgumentOutOfRangeException("Fuzzy distance must be between 0.0 and 1.0");
			}

			var ch = this.queryText[this.queryText.Length - 1];
			if (ch == '"' || ch == ']')
			{
				// this check is overly simplistic
				throw new InvalidOperationException("Fuzzy factor can only modify single word terms");
			}

			this.queryText.Append("~");
			if (fuzzy != 0.5m)
			{
				// 0.5 is the default
				this.queryText.Append(fuzzy);
			}

			return this;
		}

		/// <summary>
		/// 	Specifies a proximity distance for the phrase in the last where clause
		/// </summary>
		/// <param name = "proximity">number of words within</param>
		/// <returns></returns>
		/// <remarks>
		/// 	http://lucene.apache.org/java/2_4_0/queryparsersyntax.html#Proximity%20Searches
		/// </remarks>
		public IDocumentQuery<T> Proximity(int proximity)
		{
			if (this.queryText.Length < 1)
			{
				throw new InvalidOperationException("Missing where clause");
			}

			if (proximity < 1)
			{
				throw new ArgumentOutOfRangeException("Proximity distance must be positive number");
			}

			if (this.queryText[this.queryText.Length - 1] != '"')
			{
				// this check is overly simplistic
				throw new InvalidOperationException("Proximity distance can only modify a phrase");
			}

			this.queryText.Append("~").Append(proximity);

			return this;
		}

		public IDocumentQuery<T> WithinRadiusOfLatLng(double radius, double lat, double lng)
		{
			this.radius = radius;
			this.lat = lat;
			this.lng = lng;

			return this;
		}



		public IDocumentQuery<T> OrderBy(params string[] fields)
		{
			orderByFields = orderByFields.Concat(fields).ToArray();
			return this;
		}

		public IDocumentQuery<T> WaitForNonStaleResultsAsOfNow()
		{
			waitForNonStaleResults = true;
			cutoff = DateTime.UtcNow;
			return this;
		}

		public IDocumentQuery<T> WaitForNonStaleResultsAsOfNow(TimeSpan waitTimeout)
		{
			waitForNonStaleResults = true;
			cutoff = DateTime.UtcNow;
			timeout = waitTimeout;
			return this;
		}

		public IDocumentQuery<T> WaitForNonStaleResultsAsOf(DateTime cutOff)
		{
			waitForNonStaleResults = true;
			this.cutoff = cutOff.ToUniversalTime();
			return this;
		}

		public IDocumentQuery<T> WaitForNonStaleResultsAsOf(DateTime cutOff, TimeSpan waitTimeout)
		{
			waitForNonStaleResults = true;
			this.cutoff = cutOff.ToUniversalTime();
			timeout = waitTimeout;
			return this;
		}

		public IDocumentQuery<T> WaitForNonStaleResults()
		{
			waitForNonStaleResults = true;
			timeout = TimeSpan.FromSeconds(15);
			return this;
		}

		#endregion

		protected QueryResult GetQueryResult()
		{
			session.IncrementRequestCount();
			var sp = Stopwatch.StartNew();
			while (true)
			{
				var query = queryText.ToString();

				Trace.WriteLine(string.Format("Executing query '{0}' on index '{1}' in '{2}'",
				                              query, indexName, session.StoreIdentifier));

				IndexQuery indexQuery = null;

				if (lat != 0 && lng != 0 && radius != 0)
				{
					indexQuery = new SpatialIndexQuery
					{
						Query = query,
						PageSize = pageSize,
						Start = start,
						Cutoff = cutoff,
						SortedFields = orderByFields.Select(x => new SortedField(x)).ToArray(),
						FieldsToFetch = projectionFields,
						Latitude = lat,
						Longitude = lng,
						Radius = radius
					};
				}
				else
				{
					indexQuery = new IndexQuery
					{
					    Query = query,
					    PageSize = pageSize,
					    Start = start,
					    Cutoff = cutoff,
					    SortedFields = orderByFields.Select(x => new SortedField(x)).ToArray(),
					    FieldsToFetch = projectionFields
					};
				}

				var result = databaseCommands.Query(indexName, indexQuery, includes.ToArray());
				if (waitForNonStaleResults && result.IsStale)
				{
					if (sp.Elapsed > timeout)
					{
						sp.Stop();
						throw new TimeoutException(string.Format("Waited for {0:#,#}ms for the query to return non stale result.",
						                                         sp.ElapsedMilliseconds));
					}
					Trace.WriteLine(
						string.Format("Stale query results on non stable query '{0}' on index '{1}' in '{2}', query will be retired",
						              query, indexName, session.StoreIdentifier));
					Thread.Sleep(100);
					continue;
				}
				Trace.WriteLine(string.Format("Query returned {0}/{1} results", result.Results.Count, result.TotalResults));
				return result;
			}
		}

		private T Deserialize(JObject result)
		{
			var metadata = result.Value<JObject>("@metadata");
			if (projectionFields != null && projectionFields.Length > 0 // we asked for a projection directly from the index
				|| metadata == null) // we aren't querying a document, we are probably querying a map reduce index result
			{
				return (T) session.Conventions.CreateSerializer().Deserialize(new JTokenReader(result), typeof (T));
			}
			return session.TrackEntity<T>(metadata.Value<string>("@id"),
			                              result,
			                              metadata);
		}

		private static string TransformToEqualValue(object value, bool isAnalyzed, bool allowWildcards)
		{
			if (value == null)
			{
				return "[[NULL_VALUE]]";
			}

			if (value is bool)
			{
				return (bool) value ? "true" : "false";
			}

			if (value is DateTime)
			{
				return DateTools.DateToString((DateTime) value, DateTools.Resolution.MILLISECOND);
			}

			if (!(value is string))
			{
				return Convert.ToString(value, CultureInfo.InvariantCulture);
			}

			var escaped = RavenQuery.Escape(Convert.ToString(value, CultureInfo.InvariantCulture), allowWildcards && isAnalyzed);

			return isAnalyzed ? escaped : String.Concat("[[", escaped, "]]");
		}

		private static string TransformToRangeValue(object value)
		{
			if (value == null)
				return "[[NULL_VALUE]]";

			if (value is int)
				return NumberUtil.NumberToString((int) value);
			if (value is long)
				return NumberUtil.NumberToString((long) value);
			if (value is decimal)
				return NumberUtil.NumberToString((double) (decimal) value);
			if (value is double)
				return NumberUtil.NumberToString((double) value);
			if (value is float)
				return NumberUtil.NumberToString((float) value);
			if (value is DateTime)
				return DateTools.DateToString((DateTime) value, DateTools.Resolution.MILLISECOND);

			return RavenQuery.Escape(value.ToString(), false);
		}

		public override string ToString()
		{
			return this.queryText.ToString();
		}
	}
}