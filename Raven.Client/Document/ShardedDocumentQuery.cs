using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using Newtonsoft.Json.Linq;
using Raven.Database.Data;

namespace Raven.Client.Document
{
	/// <summary>
	/// A query that is executed against sharded instances
	/// </summary>
	public class ShardedDocumentQuery<T> : IDocumentQuery<T>
	{
		private readonly IList<IDocumentSession> shardSessions;
		private readonly IDocumentQuery<T>[] queries;
		private QueryResult queryResult;

		/// <summary>
		/// Initializes a new instance of the <see cref="ShardedDocumentQuery&lt;T&gt;"/> class.
		/// </summary>
		/// <param name="indexName">Name of the index.</param>
		/// <param name="shardSessions">The shard sessions.</param>
		public ShardedDocumentQuery(string indexName, IList<IDocumentSession> shardSessions)
		{
			this.shardSessions = shardSessions;
			queries = new IDocumentQuery<T>[shardSessions.Count];
			for (int i = 0; i < shardSessions.Count; i++)
			{
				queries[i] = shardSessions[i].LuceneQuery<T>(indexName);
			}
		}

		/// <summary>
		/// Initializes a new instance of the <see cref="ShardedDocumentQuery&lt;T&gt;"/> class.
		/// </summary>
		/// <param name="queries">The queries.</param>
		/// <param name="shardSessions">The shard sessions.</param>
		private ShardedDocumentQuery(IDocumentQuery<T>[] queries, IList<IDocumentSession> shardSessions)
	    {
	        this.queries = queries;
			this.shardSessions = shardSessions;
	    }

		/// <summary>
		/// Gets the query result.
		/// </summary>
		/// <returns></returns>
	    protected QueryResult GetQueryResult()
		{
			var queryResults = queries.Select(x => x.QueryResult).ToArray();
	        return new QueryResult
	        {
	            IsStale = queryResults.Any(x => x.IsStale),
	            Results = queryResults.SelectMany(x => x.Results).ToList(),
	            TotalResults = queryResults.Sum(x => x.TotalResults),
	            SkippedResults = queryResults.Sum(x => x.SkippedResults)
	        };
		}

		private void ApplyForAll(Func<IDocumentQuery<T>, IDocumentQuery<T>> transformQuery)
		{
			for (int i = 0; i < queries.Length; i++)
			{
				queries[i] = transformQuery(queries[i]);
			}
		}

		/// <summary>
		/// Gets the enumerator.
		/// </summary>
		/// <returns></returns>
		public IEnumerator<T> GetEnumerator()
		{
			var jsonSerializer =
                // we assume the same json contract resolver across the entire shared sessions set
				shardSessions.First().Conventions.CreateSerializer();
			return QueryResult.Results
				.Select(j => (T)jsonSerializer.Deserialize(new JTokenReader(j), typeof(T)))
				.GetEnumerator();
		}

		/// <summary>
		/// Fors the each query.
		/// </summary>
		/// <param name="action">The action.</param>
		public void ForEachQuery(Action<IDocumentSession,IDocumentQuery<T>> action)
		{
			for (int i = 0; i < shardSessions.Count; i++)
			{
				action(shardSessions[i], queries[i]);
			}
		}

		IEnumerator IEnumerable.GetEnumerator()
		{
			return GetEnumerator();
		}

		/// <summary>
		/// Includes the specified path in the query, loading the document specified in that path
		/// </summary>
		/// <param name="path">The path.</param>
		public IDocumentQuery<T> Include(string path)
		{
			ApplyForAll(x => x.Include(path));
			return this;
		}

        /// <summary>
        /// Includes the specified path in the query, loading the document specified in that path
        /// </summary>
        /// <param name="path">The path.</param>
        public IDocumentQuery<T> Include(Expression<Func<T, object>> path)
        {
            ApplyForAll(x => x.Include(path));
            return this;
        }

		/// <summary>
		/// Negate the next operation
		/// </summary>
		public IDocumentQuery<T> Not
		{
			get
			{
				ApplyForAll(query => query.Not);
				return this;
			}
		}

		/// <summary>
		/// Takes the specified count.
		/// </summary>
		/// <param name="count">The count.</param>
		/// <returns></returns>
		public IDocumentQuery<T> Take(int count)
		{
			ApplyForAll(query => query.Take(count));
			return this;
		}

		/// <summary>
		/// Skips the specified count.
		/// </summary>
		/// <param name="count">The count.</param>
		/// <returns></returns>
		public IDocumentQuery<T> Skip(int count)
		{
			ApplyForAll(query => query.Skip(count));
			return this;
		}

		/// <summary>
		/// Filter the results from the index using the specified where clause.
		/// </summary>
		/// <param name="whereClause">The where clause.</param>
		public IDocumentQuery<T> Where(string whereClause)
		{
			ApplyForAll(query => query.Where(whereClause));
			return this;
		}

		/// <summary>
		/// 	Matches exact value
		/// </summary>
		/// <remarks>
		/// 	Defaults to NotAnalyzed
		/// </remarks>
		public IDocumentQuery<T> WhereEquals(string fieldName, object value)
		{
			ApplyForAll(query => query.WhereEquals(fieldName, value));
			return this;
		}

		/// <summary>
		/// 	Matches exact value
		/// </summary>
		/// <remarks>
		/// 	Defaults to allow wildcards only if analyzed
		/// </remarks>
		public IDocumentQuery<T> WhereEquals(string fieldName, object value, bool isAnalyzed)
		{
			ApplyForAll(query => query.WhereEquals(fieldName, value, isAnalyzed));
			return this;
		}

		/// <summary>
		/// 	Matches exact value
		/// </summary>
		public IDocumentQuery<T> WhereEquals(string fieldName, object value, bool isAnalyzed, bool allowWildcards)
		{
			ApplyForAll(query => query.WhereEquals(fieldName, value, isAnalyzed, allowWildcards));
			return this;
		}

		/// <summary>
		/// 	Matches substrings of the field
		/// </summary>
		public IDocumentQuery<T> WhereContains(string fieldName, object value)
		{
			ApplyForAll(query => query.WhereContains(fieldName, value));
			return this;
		}
		/// <summary>
		/// Matches fields which starts with the specified value.
		/// </summary>
		/// <param name="fieldName">Name of the field.</param>
		/// <param name="value">The value.</param>
		public IDocumentQuery<T> WhereStartsWith(string fieldName, object value)
		{
			ApplyForAll(query => query.WhereStartsWith(fieldName, value));
			return this;
		}

		/// <summary>
		/// Matches fields which ends with the specified value.
		/// </summary>
		/// <param name="fieldName">Name of the field.</param>
		/// <param name="value">The value.</param>
		public IDocumentQuery<T> WhereEndsWith(string fieldName, object value)
		{
			ApplyForAll(query => query.WhereEndsWith(fieldName, value));
			return this;
		}

		/// <summary>
		/// Matches fields where the value is between the specified start and end, exclusive
		/// </summary>
		/// <param name="fieldName">Name of the field.</param>
		/// <param name="start">The start.</param>
		/// <param name="end">The end.</param>
		public IDocumentQuery<T> WhereBetween(string fieldName, object start, object end)
		{
			ApplyForAll(query => query.WhereBetween(fieldName, start, end));
			return this;
		}

		/// <summary>
		/// Matches fields where the value is between the specified start and end, inclusive
		/// </summary>
		/// <param name="fieldName">Name of the field.</param>
		/// <param name="start">The start.</param>
		/// <param name="end">The end.</param>
		public IDocumentQuery<T> WhereBetweenOrEqual(string fieldName, object start, object end)
		{
			ApplyForAll(query => query.WhereBetweenOrEqual(fieldName, start, end));
			return this;
		}

		/// <summary>
		/// Matches fields where the value is greater than the specified value
		/// </summary>
		/// <param name="fieldName">Name of the field.</param>
		/// <param name="value">The value.</param>
		public IDocumentQuery<T> WhereGreaterThan(string fieldName, object value)
		{
			ApplyForAll(query => query.WhereGreaterThan(fieldName, value));
			return this;
		}

		/// <summary>
		/// Matches fields where the value is greater than or equal to the specified value
		/// </summary>
		/// <param name="fieldName">Name of the field.</param>
		/// <param name="value">The value.</param>
		public IDocumentQuery<T> WhereGreaterThanOrEqual(string fieldName, object value)
		{
			ApplyForAll(query => query.WhereGreaterThanOrEqual(fieldName, value));
			return this;
		}

		/// <summary>
		/// Matches fields where the value is less than the specified value
		/// </summary>
		/// <param name="fieldName">Name of the field.</param>
		/// <param name="value">The value.</param>
		public IDocumentQuery<T> WhereLessThan(string fieldName, object value)
		{
			ApplyForAll(query => query.WhereLessThan(fieldName, value));
			return this;
		}

		/// <summary>
		/// Matches fields where the value is less than or equal to the specified value
		/// </summary>
		/// <param name="fieldName">Name of the field.</param>
		/// <param name="value">The value.</param>
		public IDocumentQuery<T> WhereLessThanOrEqual(string fieldName, object value)
		{
			ApplyForAll(query => query.WhereLessThanOrEqual(fieldName, value));
			return this;
		}

		/// <summary>
		/// Add an AND to the query
		/// </summary>
		/// <returns></returns>
		public IDocumentQuery<T> AndAlso()
		{
			ApplyForAll(query => query.AndAlso());
			return this;
		}

		/// <summary>
		/// Add an OR to the query
		/// </summary>
		/// <returns></returns>
		public IDocumentQuery<T> OrElse()
		{
			ApplyForAll(query => query.OrElse());
			return this;
		}

		/// <summary>
		/// Specifies a boost weight to the last where clause.
		/// The higher the boost factor, the more relevant the term will be.
		/// </summary>
		/// <param name="boost">boosting factor where 1.0 is default, less than 1.0 is lower weight, greater than 1.0 is higher weight</param>
		/// <returns></returns>
		/// <remarks>
		/// http://lucene.apache.org/java/2_4_0/queryparsersyntax.html#Boosting%20a%20Term
		/// </remarks>
		public IDocumentQuery<T> Boost(decimal boost)
		{
			ApplyForAll(query => query.Boost(boost));
			return this;
		}

		/// <summary>
		/// Specifies a fuzziness factor to the single word term in the last where clause
		/// </summary>
		/// <param name="fuzzy">0.0 to 1.0 where 1.0 means closer match</param>
		/// <returns></returns>
		/// <remarks>
		/// http://lucene.apache.org/java/2_4_0/queryparsersyntax.html#Fuzzy%20Searches
		/// </remarks>
		public IDocumentQuery<T> Fuzzy(decimal fuzzy)
		{
			ApplyForAll(query => query.Fuzzy(fuzzy));
			return this;
		}

		/// <summary>
		/// Specifies a proximity distance for the phrase in the last where clause
		/// </summary>
		/// <param name="proximity">number of words within</param>
		/// <returns></returns>
		/// <remarks>
		/// http://lucene.apache.org/java/2_4_0/queryparsersyntax.html#Proximity%20Searches
		/// </remarks>
		public IDocumentQuery<T> Proximity(int proximity)
		{
			ApplyForAll(query => query.Proximity(proximity));
			return this;
		}

		/// <summary>
		/// Filter matches to be inside the specified radius
		/// </summary>
		/// <param name="radius">The radius.</param>
		/// <param name="lat">The latitude.</param>
		/// <param name="lng">The longitude.</param>
		public IDocumentQuery<T> WithinRadiusOf(double radius, double lat, double lng)
		{
			ApplyForAll(query => query.WithinRadiusOf(radius, lat, lng));
			return this;
		}

        /// <summary>
        /// Sorts the query results by distance.
        /// </summary>
		public IDocumentQuery<T> SortByDistance()
		{
			ApplyForAll(query => query.SortByDistance());
			return this;
		}

		/// <summary>
		/// Order the results by the specified fields
		/// </summary>
		/// <remarks>
		/// The fields are the names of the fields to sort, defaulting to sorting by ascending.
		/// You can prefix a field name with '-' to indicate sorting by descending or '+' to sort by ascending
		/// </remarks>
		/// <param name="fields">The fields.</param>
		public IDocumentQuery<T> OrderBy(params string[] fields)
		{
			ApplyForAll(query => query.OrderBy(fields));
			return this;
		}

		/// <summary>
		/// Instructs the query to wait for non stale results as of now.
		/// </summary>
		/// <returns></returns>
	    public IDocumentQuery<T> WaitForNonStaleResultsAsOfNow()
	    {
            ApplyForAll(query => query.WaitForNonStaleResultsAsOfNow());
            return this;
	    }

		/// <summary>
		/// Instructs the query to wait for non stale results as of now for the specified timeout.
		/// </summary>
		/// <param name="waitTimeout">The wait timeout.</param>
		/// <returns></returns>
		public IDocumentQuery<T> WaitForNonStaleResultsAsOfNow(TimeSpan waitTimeout)
	    {
            ApplyForAll(query => query.WaitForNonStaleResultsAsOfNow(waitTimeout));
            return this;
	    }

		/// <summary>
		/// Instructs the query to wait for non stale results as of the cutoff date.
		/// </summary>
		/// <param name="cutOff">The cut off.</param>
		/// <returns></returns>
		public IDocumentQuery<T> WaitForNonStaleResultsAsOf(DateTime cutOff)
	    {
            ApplyForAll(query => query.WaitForNonStaleResultsAsOf(cutOff));
            return this;
	    }

		/// <summary>
		/// Instructs the query to wait for non stale results as of the cutoff date for the specified timeout
		/// </summary>
		/// <param name="cutOff">The cut off.</param>
		/// <param name="waitTimeout">The wait timeout.</param>
		/// <returns></returns>
		public IDocumentQuery<T> WaitForNonStaleResultsAsOf(DateTime cutOff, TimeSpan waitTimeout)
	    {
            ApplyForAll(query => query.WaitForNonStaleResultsAsOf(cutOff, waitTimeout));
            return this;
	    }

		/// <summary>
		/// EXPERT ONLY: Instructs the query to wait for non stale results.
		/// This shouldn't be used outside of unit tests unless you are well aware of the implications
		/// </summary>
		public IDocumentQuery<T> WaitForNonStaleResults()
		{
			ApplyForAll(query => query.WaitForNonStaleResults());
			return this;
		}

		/// <summary>
		/// EXPERT ONLY: Instructs the query to wait for non stale results for the specified wait timeout.
		/// This shouldn't be used outside of unit tests unless you are well aware of the implications
		/// </summary>
		/// <param name="timeout">The wait timeout.</param>
		public IDocumentQuery<T> WaitForNonStaleResults(TimeSpan timeout)
		{
			ApplyForAll(query => query.WaitForNonStaleResults(timeout));
			return this;
		}

		/// <summary>
		/// Selects the specified fields directly from the index
		/// </summary>
		/// <typeparam name="TProjection">The type of the projection.</typeparam>
		/// <param name="fields">The fields.</param>
		public IDocumentQuery<TProjection> SelectFields<TProjection>(string[] fields)
	    {
	        return new ShardedDocumentQuery<TProjection>(
				queries.Select(x => x.SelectFields<TProjection>(fields)).ToArray(), shardSessions
	            );
	    }

		/// <summary>
		/// Gets the query result
		/// Execute the query the first time that this is called.
		/// </summary>
		/// <value>The query result.</value>
		public QueryResult QueryResult
		{
			get { return queryResult ?? (queryResult = GetQueryResult()); }
		}

		/// <summary>
		/// Adds an ordering for a specific field to the query
		/// </summary>
		/// <param name="fieldName">Name of the field.</param>
		/// <param name="descending">if set to <c>true</c> [descending].</param>
		public IDocumentQuery<T> AddOrder(string fieldName, bool descending)
		{
			ApplyForAll(ts => ts.AddOrder(fieldName, descending));
			return this;
		}
	}
}