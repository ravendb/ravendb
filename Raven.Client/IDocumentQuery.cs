using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using Raven.Database.Data;

namespace Raven.Client
{
	/// <summary>
	/// A query against a Raven index
	/// </summary>
	/// <typeparam name="T"></typeparam>
	public interface IDocumentQuery<T> : IEnumerable<T>
	{
		/// <summary>
		/// Includes the specified path in the query, loading the document specified in that path
		/// </summary>
		/// <param name="path">The path.</param>
		IDocumentQuery<T> Include(string path);

        /// <summary>
        /// Includes the specified path in the query, loading the document specified in that path
        /// </summary>
        /// <param name="path">The path.</param>
        IDocumentQuery<T> Include(Expression<Func<T, object>> path);

		/// <summary>
		/// Negate the next operation
		/// </summary>
		IDocumentQuery<T> Not { get; }

		/// <summary>
		/// Takes the specified count.
		/// </summary>
		/// <param name="count">The count.</param>
		/// <returns></returns>
		IDocumentQuery<T> Take(int count);
		/// <summary>
		/// Skips the specified count.
		/// </summary>
		/// <param name="count">The count.</param>
		/// <returns></returns>
		IDocumentQuery<T> Skip(int count);
		/// <summary>
		/// Filter the results from the index using the specified where clause.
		/// </summary>
		/// <param name="whereClause">The where clause.</param>
		IDocumentQuery<T> Where(string whereClause);
		/// <summary>
		/// 	Matches exact value
		/// </summary>
		/// <remarks>
		/// 	Defaults to NotAnalyzed
		/// </remarks>
		IDocumentQuery<T> WhereEquals(string fieldName, object value);
		/// <summary>
		/// 	Matches exact value
		/// </summary>
		/// <remarks>
		/// 	Defaults to allow wildcards only if analyzed
		/// </remarks>
		IDocumentQuery<T> WhereEquals(string fieldName, object value, bool isAnalyzed);
		/// <summary>
		/// 	Matches exact value
		/// </summary>
		IDocumentQuery<T> WhereEquals(string fieldName, object value, bool isAnalyzed, bool allowWildcards);

		/// <summary>
		/// 	Matches substrings of the field
		/// </summary>
		IDocumentQuery<T> WhereContains(string fieldName, object value);


		/// <summary>
		/// Matches fields which starts with the specified value.
		/// </summary>
		/// <param name="fieldName">Name of the field.</param>
		/// <param name="value">The value.</param>
		IDocumentQuery<T> WhereStartsWith(string fieldName, object value);
		/// <summary>
		/// Matches fields which ends with the specified value.
		/// </summary>
		/// <param name="fieldName">Name of the field.</param>
		/// <param name="value">The value.</param>
		IDocumentQuery<T> WhereEndsWith(string fieldName, object value);

		/// <summary>
		/// Matches fields where the value is between the specified start and end, exclusive
		/// </summary>
		/// <param name="fieldName">Name of the field.</param>
		/// <param name="start">The start.</param>
		/// <param name="end">The end.</param>
		IDocumentQuery<T> WhereBetween(string fieldName, object start, object end);
		/// <summary>
		/// Matches fields where the value is between the specified start and end, inclusive
		/// </summary>
		/// <param name="fieldName">Name of the field.</param>
		/// <param name="start">The start.</param>
		/// <param name="end">The end.</param>
		IDocumentQuery<T> WhereBetweenOrEqual(string fieldName, object start, object end);
		/// <summary>
		/// Matches fields where the value is greater than the specified value
		/// </summary>
		/// <param name="fieldName">Name of the field.</param>
		/// <param name="value">The value.</param>
		IDocumentQuery<T> WhereGreaterThan(string fieldName, object value);
		/// <summary>
		/// Matches fields where the value is greater than or equal to the specified value
		/// </summary>
		/// <param name="fieldName">Name of the field.</param>
		/// <param name="value">The value.</param>
		IDocumentQuery<T> WhereGreaterThanOrEqual(string fieldName, object value);
		/// <summary>
		/// Matches fields where the value is less than the specified value
		/// </summary>
		/// <param name="fieldName">Name of the field.</param>
		/// <param name="value">The value.</param>
		IDocumentQuery<T> WhereLessThan(string fieldName, object value);
		/// <summary>
		/// Matches fields where the value is less than or equal to the specified value
		/// </summary>
		/// <param name="fieldName">Name of the field.</param>
		/// <param name="value">The value.</param>
		IDocumentQuery<T> WhereLessThanOrEqual(string fieldName, object value);
		/// <summary>
		/// Add an AND to the query
		/// </summary>
		IDocumentQuery<T> AndAlso();
		/// <summary>
		/// Add an OR to the query
		/// </summary>
		IDocumentQuery<T> OrElse();

		/// <summary>
		/// Specifies a boost weight to the last where clause.
		/// The higher the boost factor, the more relevant the term will be.
		/// </summary>
		/// <param name="boost">boosting factor where 1.0 is default, less than 1.0 is lower weight, greater than 1.0 is higher weight</param>
		/// <returns></returns>
		/// <remarks>
		/// http://lucene.apache.org/java/2_4_0/queryparsersyntax.html#Boosting%20a%20Term
		/// </remarks>
		IDocumentQuery<T> Boost(decimal boost);

		/// <summary>
		/// Specifies a fuzziness factor to the single word term in the last where clause
		/// </summary>
		/// <param name="fuzzy">0.0 to 1.0 where 1.0 means closer match</param>
		/// <returns></returns>
		/// <remarks>
		/// http://lucene.apache.org/java/2_4_0/queryparsersyntax.html#Fuzzy%20Searches
		/// </remarks>
		IDocumentQuery<T> Fuzzy(decimal fuzzy);

		/// <summary>
		/// Specifies a proximity distance for the phrase in the last where clause
		/// </summary>
		/// <param name="proximity">number of words within</param>
		/// <returns></returns>
		/// <remarks>
		/// http://lucene.apache.org/java/2_4_0/queryparsersyntax.html#Proximity%20Searches
		/// </remarks>
		IDocumentQuery<T> Proximity(int proximity);

		/// <summary>
		/// Filter matches to be inside the specified radius
		/// </summary>
		/// <param name="radius">The radius.</param>
		/// <param name="latitude">The latitude.</param>
		/// <param name="longitude">The longitude.</param>
		IDocumentQuery<T> WithinRadiusOf(double radius, double latitude, double longitude);

		IDocumentQuery<T> SortByDistance();

		/// <summary>
		/// Order the results by the specified fields
		/// </summary>
		/// <remarks>
		/// The fields are the names of the fields to sort, defaulting to sorting by ascending.
		/// You can prefix a field name with '-' to indicate sorting by descending or '+' to sort by ascending
		/// </remarks>
		/// <param name="fields">The fields.</param>
		IDocumentQuery<T> OrderBy(params string[] fields);

		/// <summary>
		/// Instructs the query to wait for non stale results as of now.
		/// </summary>
		/// <returns></returns>
        IDocumentQuery<T> WaitForNonStaleResultsAsOfNow();
		/// <summary>
		/// Instructs the query to wait for non stale results as of now for the specified timeout.
		/// </summary>
		/// <param name="waitTimeout">The wait timeout.</param>
		/// <returns></returns>
        IDocumentQuery<T> WaitForNonStaleResultsAsOfNow(TimeSpan waitTimeout);

		/// <summary>
		/// Instructs the query to wait for non stale results as of the cutoff date.
		/// </summary>
		/// <param name="cutOff">The cut off.</param>
		/// <returns></returns>
        IDocumentQuery<T> WaitForNonStaleResultsAsOf(DateTime cutOff);
		/// <summary>
		/// Instructs the query to wait for non stale results as of the cutoff date for the specified timeout
		/// </summary>
		/// <param name="cutOff">The cut off.</param>
		/// <param name="waitTimeout">The wait timeout.</param>
        IDocumentQuery<T> WaitForNonStaleResultsAsOf(DateTime cutOff, TimeSpan waitTimeout);

		/// <summary>
		/// EXPERT ONLY: Instructs the query to wait for non stale results.
		/// This shouldn't be used outside of unit tests unless you are well aware of the implications
		/// </summary>
		IDocumentQuery<T> WaitForNonStaleResults();
		/// <summary>
		/// EXPERT ONLY: Instructs the query to wait for non stale results for the specified wait timeout.
		/// This shouldn't be used outside of unit tests unless you are well aware of the implications
		/// </summary>
		/// <param name="waitTimeout">The wait timeout.</param>
        IDocumentQuery<T> WaitForNonStaleResults(TimeSpan waitTimeout);
		/// <summary>
		/// Selects the specified fields directly from the index
		/// </summary>
		/// <typeparam name="TProjection">The type of the projection.</typeparam>
		/// <param name="fields">The fields.</param>
		IDocumentQuery<TProjection> SelectFields<TProjection>(params string[] fields);

		/// <summary>
		/// Gets the query result
		/// Execute the query the first time that this is called.
		/// </summary>
		/// <value>The query result.</value>
		QueryResult QueryResult { get; }
		/// <summary>
		/// Adds an ordering for a specific field to the query
		/// </summary>
		/// <param name="fieldName">Name of the field.</param>
		/// <param name="descending">if set to <c>true</c> [descending].</param>
		IDocumentQuery<T> AddOrder(string fieldName, bool descending);
	}
}