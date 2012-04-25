#if !SILVERLIGHT
using System;
using System.Collections;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using Raven.Abstractions.Data;
using Raven.Client.Connection;
using Raven.Client.Linq;
using Raven.Client.Listeners;
#if !NET_3_5
using Raven.Client.Connection.Async;
#endif

namespace Raven.Client.Document
{
	/// <summary>
	/// A query against a Raven index
	/// </summary>
	public class DocumentQuery<T> : AbstractDocumentQuery<T, DocumentQuery<T>>, IDocumentQuery<T>
	{
		/// <summary>
		/// Initializes a new instance of the <see cref="DocumentQuery{T}"/> class.
		/// </summary>
		public DocumentQuery(InMemoryDocumentSessionOperations session
#if !SILVERLIGHT
			, IDatabaseCommands databaseCommands
#endif 
#if !NET_3_5
			, IAsyncDatabaseCommands asyncDatabaseCommands
#endif
			, string indexName, string[] projectionFields, IDocumentQueryListener[] queryListeners)
			: base(session
#if !SILVERLIGHT
			, databaseCommands
#endif
#if !NET_3_5
			, asyncDatabaseCommands
#endif
			, indexName, projectionFields, queryListeners)
		{
		}

		/// <summary>
		/// Initializes a new instance of the <see cref="DocumentQuery{T}"/> class.
		/// </summary>
		public DocumentQuery(DocumentQuery<T> other)
			: base(other)
		{
			
		}


		/// <summary>
		/// Selects the specified fields directly from the index
		/// </summary>
		/// <typeparam name="TProjection">The type of the projection.</typeparam>
		/// <param name="fields">The fields.</param>
		public virtual IDocumentQuery<TProjection> SelectFields<TProjection>(string[] fields)
		{
			var documentQuery = new DocumentQuery<TProjection>(theSession,
#if !SILVERLIGHT
			                                                   theDatabaseCommands,
#endif
#if !NET_3_5
			                                                   theAsyncDatabaseCommands,
#endif
			                                                   indexName, fields,
			                                                   queryListeners)
			{
				pageSize = pageSize,
				theQueryText = new StringBuilder(theQueryText.ToString()),
				start = start,
				timeout = timeout,
				cutoff = cutoff,
				queryStats = queryStats,
				theWaitForNonStaleResults = theWaitForNonStaleResults,
				sortByHints = sortByHints,
				orderByFields = orderByFields,
				groupByFields = groupByFields,
				aggregationOp = aggregationOp,
				negate = negate,
				transformResultsFunc = transformResultsFunc,
				includes = new HashSet<string>(includes),
				isSpatialQuery = isSpatialQuery,
				lat = lat,
				lng = lng,
				radius = radius,
			};
			documentQuery.AfterQueryExecuted(afterQueryExecutedCallback);
			return documentQuery;
		}

		/// <summary>
		/// EXPERT ONLY: Instructs the query to wait for non stale results for the specified wait timeout.
		/// This shouldn't be used outside of unit tests unless you are well aware of the implications
		/// </summary>
		/// <param name="waitTimeout">The wait timeout.</param>
		IDocumentQuery<T> IDocumentQueryBase<T, IDocumentQuery<T>>.WaitForNonStaleResults(TimeSpan waitTimeout)
		{
			WaitForNonStaleResults(waitTimeout);
			return this;
		}

		/// <summary>
		/// Adds an ordering for a specific field to the query
		/// </summary>
		/// <param name="fieldName">Name of the field.</param>
		/// <param name="descending">if set to <c>true</c> [descending].</param>
		IDocumentQuery<T> IDocumentQueryBase<T, IDocumentQuery<T>>.AddOrder(string fieldName, bool descending)
		{
			AddOrder(fieldName, descending);
			return this;
		}

		/// <summary>
		/// Order the search results randomly
		/// </summary>
		IDocumentQuery<T> IDocumentQueryBase<T, IDocumentQuery<T>>.RandomOrdering()
		{
			RandomOrdering();
			return this;
		}

		/// <summary>
		/// Order the search results randomly using the specified seed
		/// this is useful if you want to have repeatable random queries
		/// </summary>
		IDocumentQuery<T> IDocumentQueryBase<T, IDocumentQuery<T>>.RandomOrdering(string seed)
		{
			RandomOrdering(seed);
			return this;
		}

		/// <summary>
		/// Adds an ordering for a specific field to the query and specifies the type of field for sorting purposes
		/// </summary>
		/// <param name="fieldName">Name of the field.</param>
		/// <param name="descending">if set to <c>true</c> [descending].</param>
		/// <param name="fieldType">the type of the field to be sorted.</param>
		IDocumentQuery<T> IDocumentQueryBase<T, IDocumentQuery<T>>.AddOrder(string fieldName, bool descending, Type fieldType)
		{
			AddOrder(fieldName, descending, fieldType);
			return this;
		}

		/// <summary>
		/// Simplified method for opening a new clause within the query
		/// </summary>
		/// <returns></returns>
		IDocumentQuery<T> IDocumentQueryBase<T, IDocumentQuery<T>>.OpenSubclause()
		{
			OpenSubclause();
			return this;
		}

		/// <summary>
		/// Simplified method for closing a clause within the query
		/// </summary>
		/// <returns></returns>
		IDocumentQuery<T> IDocumentQueryBase<T, IDocumentQuery<T>>.CloseSubclause()
		{
			CloseSubclause();
			return this;
		}

		/// <summary>
		/// Perform a search for documents which fields that match the searchTerms.
		/// If there is more than a single term, each of them will be checked independently.
		/// </summary>
		IDocumentQuery<T> IDocumentQueryBase<T, IDocumentQuery<T>>.Search(string fieldName, string searchTerms)
		{
			Search(fieldName, searchTerms);
			return this;
		}

		///<summary>
		/// Instruct the index to group by the specified fields using the specified aggregation operation
		///</summary>
		/// <remarks>
		/// This is only valid on dynamic indexes queries
		/// </remarks>
		IDocumentQuery<T> IDocumentQueryBase<T, IDocumentQuery<T>>.GroupBy(AggregationOperation aggregationOperation, params string[] fieldsToGroupBy)
		{
			GroupBy(aggregationOperation, fieldsToGroupBy);
			return this;
		}

		/// <summary>
		/// Partition the query so we can intersect different parts of the query
		/// across different index entries.
		/// </summary>
		IDocumentQuery<T> IDocumentQueryBase<T, IDocumentQuery<T>>.Intersect()
		{
			Intersect();
			return this;
		}

		/// <summary>
		/// Provide statistics about the query, such as total count of matching records
		/// </summary>
		IDocumentQuery<T> IDocumentQueryBase<T, IDocumentQuery<T>>.Statistics(out RavenQueryStatistics stats)
		{
			Statistics(out stats);
			return this;
		}


		/// <summary>
		/// Includes the specified path in the query, loading the document specified in that path
		/// </summary>
		/// <param name="path">The path.</param>
		IDocumentQuery<T> IDocumentQueryBase<T, IDocumentQuery<T>>.Include(string path)
		{
			Include(path);
			return this;
		}

		/// <summary>
		/// Includes the specified path in the query, loading the document specified in that path
		/// </summary>
		/// <param name="path">The path.</param>
		IDocumentQuery<T> IDocumentQueryBase<T, IDocumentQuery<T>>.Include(Expression<Func<T, object>> path)
		{
			Include(path);
			return this;
		}

		/// <summary>
		/// Negate the next operation
		/// </summary>
		IDocumentQuery<T> IDocumentQueryBase<T, IDocumentQuery<T>>.Not
		{
			get
			{
				NegateNext();
				return this;
			}
		}

		/// <summary>
		/// Takes the specified count.
		/// </summary>
		/// <param name="count">The count.</param>
		/// <returns></returns>
		IDocumentQuery<T> IDocumentQueryBase<T, IDocumentQuery<T>>.Take(int count)
		{
			Take(count);
			return this;
		}

		/// <summary>
		/// Skips the specified count.
		/// </summary>
		/// <param name="count">The count.</param>
		/// <returns></returns>
		IDocumentQuery<T> IDocumentQueryBase<T, IDocumentQuery<T>>.Skip(int count)
		{
			Skip(count);
			return this;
		}

		/// <summary>
		/// Filter the results from the index using the specified where clause.
		/// </summary>
		/// <param name="whereClause">The where clause.</param>
		IDocumentQuery<T> IDocumentQueryBase<T, IDocumentQuery<T>>.Where(string whereClause)
		{
			Where(whereClause);
			return this;
		}

		/// <summary>
		/// 	Matches exact value
		/// </summary>
		/// <remarks>
		/// 	Defaults to NotAnalyzed
		/// </remarks>
		IDocumentQuery<T> IDocumentQueryBase<T, IDocumentQuery<T>>.WhereEquals(string fieldName, object value)
		{
			WhereEquals(fieldName, value);
			return this;
		}

		/// <summary>
		/// 	Matches exact value
		/// </summary>
		/// <remarks>
		/// 	Defaults to allow wildcards only if analyzed
		/// </remarks>
		IDocumentQuery<T> IDocumentQueryBase<T, IDocumentQuery<T>>.WhereEquals(string fieldName, object value, bool isAnalyzed)
		{
			WhereEquals(fieldName, value, isAnalyzed);
			return this;
		}

		/// <summary>
		/// 	Matches exact value
		/// </summary>
		IDocumentQuery<T> IDocumentQueryBase<T, IDocumentQuery<T>>.WhereEquals(WhereParams whereEqualsParams)
		{
			WhereEquals(whereEqualsParams);
			return this;
		}

		/// <summary>
		/// 	Matches substrings of the field
		/// </summary>
		[Obsolete("Avoid using WhereConatins(), use Search() instead")]
		IDocumentQuery<T> IDocumentQueryBase<T, IDocumentQuery<T>>.WhereContains(string fieldName, object value)
		{
			WhereContains(fieldName, value);
			return this;
		}

		/// <summary>
		/// 	Matches substrings of the field
		/// </summary>
		[Obsolete("Avoid using WhereConatins(), use Search() instead")]
		IDocumentQuery<T> IDocumentQueryBase<T, IDocumentQuery<T>>.WhereContains(string fieldName, params object[] values)
		{
			WhereContains(fieldName, values);
			return this;
		}

		/// <summary>
		/// 	Matches substrings of the field
		/// </summary>
		[Obsolete("Avoid using WhereConatins(), use Search() instead")]
		IDocumentQuery<T> IDocumentQueryBase<T, IDocumentQuery<T>>.WhereContains(string fieldName, IEnumerable<object> values)
		{
			WhereContains(fieldName, values);
			return this;
		}


		/// <summary>
		/// Check that the field has one of the specified value
		/// </summary>
		IDocumentQuery<T> IDocumentQueryBase<T, IDocumentQuery<T>>.WhereIn(string fieldName, IEnumerable<object> values)
		{
			WhereIn(fieldName, values);
			return this;
		}

		/// <summary>
		/// Matches fields which starts with the specified value.
		/// </summary>
		/// <param name="fieldName">Name of the field.</param>
		/// <param name="value">The value.</param>
		IDocumentQuery<T> IDocumentQueryBase<T, IDocumentQuery<T>>.WhereStartsWith(string fieldName, object value)
		{
			WhereStartsWith(fieldName, value);
			return this;
		}

		/// <summary>
		/// Matches fields which ends with the specified value.
		/// </summary>
		/// <param name="fieldName">Name of the field.</param>
		/// <param name="value">The value.</param>
		IDocumentQuery<T> IDocumentQueryBase<T, IDocumentQuery<T>>.WhereEndsWith(string fieldName, object value)
		{
			WhereEndsWith(fieldName, value);
			return this;
		}

		/// <summary>
		/// Matches fields where the value is between the specified start and end, exclusive
		/// </summary>
		/// <param name="fieldName">Name of the field.</param>
		/// <param name="start">The start.</param>
		/// <param name="end">The end.</param>
		IDocumentQuery<T> IDocumentQueryBase<T, IDocumentQuery<T>>.WhereBetween(string fieldName, object start, object end)
		{
			WhereBetween(fieldName, start, end);
			return this;
		}

		/// <summary>
		/// Matches fields where the value is between the specified start and end, inclusive
		/// </summary>
		/// <param name="fieldName">Name of the field.</param>
		/// <param name="start">The start.</param>
		/// <param name="end">The end.</param>
		IDocumentQuery<T> IDocumentQueryBase<T, IDocumentQuery<T>>.WhereBetweenOrEqual(string fieldName, object start, object end)
		{
			WhereBetweenOrEqual(fieldName, start, end);
			return this;
		}

		/// <summary>
		/// Matches fields where the value is greater than the specified value
		/// </summary>
		/// <param name="fieldName">Name of the field.</param>
		/// <param name="value">The value.</param>
		IDocumentQuery<T> IDocumentQueryBase<T, IDocumentQuery<T>>.WhereGreaterThan(string fieldName, object value)
		{
			WhereGreaterThan(fieldName, value);
			return this;
		}

		/// <summary>
		/// Matches fields where the value is greater than or equal to the specified value
		/// </summary>
		/// <param name="fieldName">Name of the field.</param>
		/// <param name="value">The value.</param>
		IDocumentQuery<T> IDocumentQueryBase<T, IDocumentQuery<T>>.WhereGreaterThanOrEqual(string fieldName, object value)
		{
			WhereGreaterThanOrEqual(fieldName, value);
			return this;
		}

		/// <summary>
		/// Matches fields where the value is less than the specified value
		/// </summary>
		/// <param name="fieldName">Name of the field.</param>
		/// <param name="value">The value.</param>
		IDocumentQuery<T> IDocumentQueryBase<T, IDocumentQuery<T>>.WhereLessThan(string fieldName, object value)
		{
			WhereLessThan(fieldName, value);
			return this;
		}

		/// <summary>
		/// Matches fields where the value is less than or equal to the specified value
		/// </summary>
		/// <param name="fieldName">Name of the field.</param>
		/// <param name="value">The value.</param>
		IDocumentQuery<T> IDocumentQueryBase<T, IDocumentQuery<T>>.WhereLessThanOrEqual(string fieldName, object value)
		{
			WhereLessThanOrEqual(fieldName, value);
			return this;
		}

		/// <summary>
		/// Add an AND to the query
		/// </summary>
		IDocumentQuery<T> IDocumentQueryBase<T, IDocumentQuery<T>>.AndAlso()
		{
			AndAlso();
			return this;
		}

		/// <summary>
		/// Add an OR to the query
		/// </summary>
		IDocumentQuery<T> IDocumentQueryBase<T, IDocumentQuery<T>>.OrElse()
		{
			OrElse();
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
		IDocumentQuery<T> IDocumentQueryBase<T, IDocumentQuery<T>>.Boost(decimal boost)
		{
			Boost(boost);
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
		IDocumentQuery<T> IDocumentQueryBase<T, IDocumentQuery<T>>.Fuzzy(decimal fuzzy)
		{
			Fuzzy(fuzzy);
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
		IDocumentQuery<T> IDocumentQueryBase<T, IDocumentQuery<T>>.Proximity(int proximity)
		{
			Proximity(proximity);
			return this;
		}

		/// <summary>
		/// Filter matches to be inside the specified radius
		/// </summary>
		/// <param name="radius">The radius.</param>
		/// <param name="latitude">The latitude.</param>
		/// <param name="longitude">The longitude.</param>
		public IDocumentQuery<T> WithinRadiusOf(double radius, double latitude, double longitude)
		{
			return (IDocumentQuery<T>) GenerateQueryWithinRadiusOf(radius, latitude, longitude);
		}

		/// <summary>
		///   Filter matches to be inside the specified radius
		/// </summary>
		/// <param name = "radius">The radius.</param>
		/// <param name = "latitude">The latitude.</param>
		/// <param name = "longitude">The longitude.</param>
		protected override object GenerateQueryWithinRadiusOf(double radius, double latitude, double longitude)
		{
			isSpatialQuery = true;
			this.radius = radius;
			lat = latitude;
			lng = longitude;
			return this;
		}

		/// <summary>
		/// Sorts the query results by distance.
		/// </summary>
		IDocumentQuery<T> IDocumentQueryBase<T, IDocumentQuery<T>>.SortByDistance()
		{
			OrderBy(Constants.DistanceFieldName);
			return this;
		}

		/// <summary>
		/// Order the results by the specified fields
		/// The fields are the names of the fields to sort, defaulting to sorting by ascending.
		/// You can prefix a field name with '-' to indicate sorting by descending or '+' to sort by ascending
		/// </summary>
		/// <param name="fields">The fields.</param>
		IDocumentQuery<T> IDocumentQueryBase<T, IDocumentQuery<T>>.OrderBy(params string[] fields)
		{
			OrderBy(fields);
			return this;
		}

		/// <summary>
		/// Instructs the query to wait for non stale results as of now.
		/// </summary>
		/// <returns></returns>
		IDocumentQuery<T> IDocumentQueryBase<T, IDocumentQuery<T>>.WaitForNonStaleResultsAsOfNow()
		{
			WaitForNonStaleResultsAsOfNow();
			return this;
		}

		/// <summary>
		/// Instructs the query to wait for non stale results as of the last write made by any session belonging to the 
		/// current document store.
		/// This ensures that you'll always get the most relevant results for your scenarios using simple indexes (map only or dynamic queries).
		/// However, when used to query map/reduce indexes, it does NOT guarantee that the document that this etag belong to is actually considered for the results. 
		/// </summary>
		IDocumentQuery<T> IDocumentQueryBase<T, IDocumentQuery<T>>.WaitForNonStaleResultsAsOfLastWrite()
		{
			WaitForNonStaleResultsAsOfLastWrite();
			return this;
		}

		/// <summary>
		/// Instructs the query to wait for non stale results as of the last write made by any session belonging to the 
		/// current document store.
		/// This ensures that you'll always get the most relevant results for your scenarios using simple indexes (map only or dynamic queries).
		/// However, when used to query map/reduce indexes, it does NOT guarantee that the document that this etag belong to is actually considered for the results. 
		/// </summary>
		IDocumentQuery<T> IDocumentQueryBase<T, IDocumentQuery<T>>.WaitForNonStaleResultsAsOfLastWrite(TimeSpan waitTimeout)
		{
			WaitForNonStaleResultsAsOfLastWrite(waitTimeout);
			return this;
		}

		/// <summary>
		/// Instructs the query to wait for non stale results as of now for the specified timeout.
		/// </summary>
		/// <param name="waitTimeout">The wait timeout.</param>
		/// <returns></returns>
		IDocumentQuery<T> IDocumentQueryBase<T, IDocumentQuery<T>>.WaitForNonStaleResultsAsOfNow(TimeSpan waitTimeout)
		{
			WaitForNonStaleResultsAsOfNow(waitTimeout);
			return this;
		}

		/// <summary>
		/// Instructs the query to wait for non stale results as of the cutoff date.
		/// </summary>
		/// <param name="cutOff">The cut off.</param>
		/// <returns></returns>
		IDocumentQuery<T> IDocumentQueryBase<T, IDocumentQuery<T>>.WaitForNonStaleResultsAsOf(DateTime cutOff)
		{
			WaitForNonStaleResultsAsOf(cutOff);
			return this;
		}

		/// <summary>
		/// Instructs the query to wait for non stale results as of the cutoff date for the specified timeout
		/// </summary>
		/// <param name="cutOff">The cut off.</param>
		/// <param name="waitTimeout">The wait timeout.</param>
		IDocumentQuery<T> IDocumentQueryBase<T, IDocumentQuery<T>>.WaitForNonStaleResultsAsOf(DateTime cutOff, TimeSpan waitTimeout)
		{
			WaitForNonStaleResultsAsOf(cutOff, waitTimeout);
			return this;
		}

		/// <summary>
		/// EXPERT ONLY: Instructs the query to wait for non stale results.
		/// This shouldn't be used outside of unit tests unless you are well aware of the implications
		/// </summary>
		IDocumentQuery<T> IDocumentQueryBase<T, IDocumentQuery<T>>.WaitForNonStaleResults()
		{
			WaitForNonStaleResults();
			return this;
		}

		/// <summary>
		/// Returns an enumerator that iterates through a collection.
		/// </summary>
		/// <returns>
		/// An <see cref="T:System.Collections.IEnumerator"/> object that can be used to iterate through the collection.
		/// </returns>
		/// <filterpriority>2</filterpriority>
		IEnumerator IEnumerable.GetEnumerator()
		{
			return GetEnumerator();
		}

		/// <summary>
		///   Returns a <see cref = "System.String" /> that represents this instance.
		/// </summary>
		/// <returns>
		///   A <see cref = "System.String" /> that represents this instance.
		/// </returns>
		public override string ToString()
		{
			var trim = QueryText.ToString().Trim();
			if(isSpatialQuery)
				return string.Format(CultureInfo.InvariantCulture, "{0} Lat: {1} Lng: {2} Radius: {3}", trim, lat, lng, radius);
			return trim;
		}
	}
}
#endif