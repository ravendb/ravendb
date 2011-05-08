#if !SILVERLIGHT
//-----------------------------------------------------------------------
// <copyright file="ShardedDocumentQuery.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
#if !NET_3_5
using System.Threading.Tasks;
#endif
using Newtonsoft.Json.Linq;
using Raven.Abstractions.Data;
using Raven.Client.Linq;
using Raven.Json.Linq;

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
				queries[i] = shardSessions[i].Advanced.LuceneQuery<T>(indexName);
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
				SkippedResults = queryResults.Sum(x => x.SkippedResults),
				IndexTimestamp = queryResults.Min(x => x.IndexTimestamp),
				IndexEtag = queryResults.Min(x => x.IndexEtag),
				IndexName = queryResults.Select(x => x.IndexName).FirstOrDefault()
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
				shardSessions.First().Advanced.Conventions.CreateSerializer();
			return QueryResult.Results
				.Select(j => (T)jsonSerializer.Deserialize(new RavenJTokenReader(j), typeof(T)))
				.GetEnumerator();
		}

		/// <summary>
		/// Fors the each query.
		/// </summary>
		/// <param name="action">The action.</param>
		public void ForEachQuery(Action<IDocumentSession, IDocumentQuery<T>> action)
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
		///   Negate the next operation
		/// </summary>
		public void NegateNext()
		{
			ApplyForAll(x =>
			{
				x.NegateNext();
				return null;
			});
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
		public IDocumentQuery<T> WhereEquals(WhereEqualsParams whereEqualsParams)
		{
			ApplyForAll(query => query.WhereEquals(whereEqualsParams));
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
		/// 	Matches substrings of the field
		/// </summary>
		public IDocumentQuery<T> WhereContains(string fieldName, params object[] values)
		{
			ApplyForAll(query => query.WhereContains(fieldName, values));
			return this;
		}

		/// <summary>
		/// 	Matches substrings of the field
		/// </summary>
		public IDocumentQuery<T> WhereContains(string fieldName, IEnumerable<object> values)
		{
			ApplyForAll(query => query.WhereContains(fieldName, values));
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
        /// Gets the document convention from the query session
        /// </summary>
	    public DocumentConvention DocumentConvention
	    {
	        get { return shardSessions[0].Advanced.Conventions; }
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

#if !NET_3_5
		/// <summary>
		/// Returns a list of results for a query asynchronously. 
		/// </summary>
		public Task<IList<T>> ToListAsync()
		{
			throw new NotImplementedException();
		}

		/// <summary>
		/// Gets the total count of records for this query
		/// </summary>
		public Task<int> CountAsync()
		{
			throw new NotImplementedException();
		}
#endif

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

		/// <summary>
		/// Adds an ordering for a specific field to the query and specifies the type of field for sorting purposes
		/// </summary>
		/// <param name="fieldName">Name of the field.</param>
		/// <param name="descending">if set to <c>true</c> [descending].</param>
		/// <param name="fieldType">the type of the field to be sorted.</param>
		public IDocumentQuery<T> AddOrder(string fieldName, bool descending, Type fieldType)
		{
			ApplyForAll(x => x.AddOrder(fieldName, descending, fieldType));
			return this;
		}

		/// <summary>
		/// This function exists solely to forbid in memory where clause on IDocumentQuery, because
		/// that is nearly always a mistake.
		/// </summary>
		[Obsolete(@"
You cannot issue an in memory filter - such as Where(x=>x.Name == ""Ayende"") - on IDocumentQuery. 
This is likely a bug, because this will execute the filter in memory, rather than in RavenDB.
Consider using session.Query<T>() instead of session.LuceneQuery<T>. The session.Query<T>() method fully supports Linq queries, while session.LuceneQuery<T>() is intended for lower level API access.
If you really want to do in memory filtering on the data returned from the query, you can use: session.LuceneQuery<T>().ToList().Where(x=>x.Name == ""Ayende"")
", true)]
		public IEnumerable<T> Where(Func<T, bool> predicate)
		{
			throw new NotSupportedException();
		}



		/// <summary>
		///   This function exists solely to forbid in memory where clause on IDocumentQuery, because
		///   that is nearly always a mistake.
		/// </summary>
		[Obsolete(
			@"
You cannot issue an in memory filter - such as Count(x=>x.Name == ""Ayende"") - on IDocumentQuery. 
This is likely a bug, because this will execute the filter in memory, rather than in RavenDB.
Consider using session.Query<T>() instead of session.LuceneQuery<T>. The session.Query<T>() method fully supports Linq queries, while session.LuceneQuery<T>() is intended for lower level API access.
If you really want to do in memory filtering on the data returned from the query, you can use: session.LuceneQuery<T>().ToList().Count(x=>x.Name == ""Ayende"")
"
			, true)]
		public int Count(Func<T, bool> predicate)
		{
			throw new NotSupportedException();
		}

		/// <summary>
		///   This function exists solely to forbid in memory where clause on IDocumentQuery, because
		///   that is nearly always a mistake.
		/// </summary>
		[Obsolete(
			@"
You cannot issue an in memory filter - such as Count() - on IDocumentQuery. 
This is likely a bug, because this will execute the filter in memory, rather than in RavenDB.
Consider using session.Query<T>() instead of session.LuceneQuery<T>. The session.Query<T>() method fully supports Linq queries, while session.LuceneQuery<T>() is intended for lower level API access.
If you really want to do in memory filtering on the data returned from the query, you can use: session.LuceneQuery<T>().ToList().Count()
"
			, true)]
		public int Count()
		{
			throw new NotSupportedException();
		}

		/// <summary>
		/// Simplified method for opening a new clause within the query
		/// </summary>
		/// <returns></returns>
		public IDocumentQuery<T> OpenSubclause()
		{
			ApplyForAll(x => x.OpenSubclause());
			return this;
		}

		///<summary>
		/// Instruct the index to group by the specified fields using the specified aggregation operation
		///</summary>
		/// <remarks>
		/// This is only valid on dynamic indexes queries
		/// </remarks>
		public IDocumentQuery<T> GroupBy(AggregationOperation aggregationOperation, params string[] fieldsToGroupBy)
		{
			ApplyForAll(x => x.GroupBy(aggregationOperation, fieldsToGroupBy));
			return this;
		}

		/// <summary>
		/// Callback to get the results of the query
		/// </summary>
		public void AfterQueryExecuted(Action<QueryResult> afterQueryExecuted)
		{
			foreach (var query in queries)
			{
				query.AfterQueryExecuted(afterQueryExecuted);				
			}
		}

		/// <summary>
		/// Called externally to raise the after query executed callback
		/// </summary>
		public void InvokeAfterQueryExecuted(QueryResult result)
		{
			foreach (var query in queries)
			{
				query.InvokeAfterQueryExecuted(result);
			}
		}

		/// <summary>
		/// Provide statistics about the query, such as total count of matching records
		/// </summary>
		public IDocumentQuery<T> Statistics(out RavenQueryStatistics stats)
		{
			throw new NotImplementedException();
		}

		/// <summary>
		/// Simplified method for closing a clause within the query
		/// </summary>
		/// <returns></returns>
		public IDocumentQuery<T> CloseSubclause()
		{
			ApplyForAll(x => x.CloseSubclause());
			return this;
		}
	}
}
#endif
