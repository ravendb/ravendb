using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using Raven.Abstractions.Data;
using Raven.Client.Linq;

namespace Raven.Client.Document
{
	///<summary>
	/// Mostly used by the linq provider
	///</summary>
	public interface IAbstractDocumentQuery<T>
	{
		/// <summary>
		///   Get the name of the index being queried
		/// </summary>
		string IndexQueried { get; }

		/// <summary>
		/// Gets the document convention from the query session
		/// </summary>
		DocumentConvention DocumentConvention { get; }

		/// <summary>
		///   Instruct the query to wait for non stale result for the specified wait timeout.
		/// </summary>
		/// <param name = "waitTimeout">The wait timeout.</param>
		/// <returns></returns>
		void WaitForNonStaleResults(TimeSpan waitTimeout);

		/// <summary>
		///   Gets the fields for projection
		/// </summary>
		/// <returns></returns>
		IEnumerable<string> GetProjectionFields();

		/// <summary>
		/// Order the search results randomly
		/// </summary>
		void RandomOrdering();

		/// <summary>
		/// Order the search results randomly using the specified seed
		/// this is useful if you want to have repeatable random queries
		/// </summary>
		void RandomOrdering(string seed);

		/// <summary>
		///   Adds an ordering for a specific field to the query
		/// </summary>
		/// <param name = "fieldName">Name of the field.</param>
		/// <param name = "descending">if set to <c>true</c> [descending].</param>
		void AddOrder(string fieldName, bool descending);

		/// <summary>
		///   Adds an ordering for a specific field to the query and specifies the type of field for sorting purposes
		/// </summary>
		/// <param name = "fieldName">Name of the field.</param>
		/// <param name = "descending">if set to <c>true</c> [descending].</param>
		/// <param name = "fieldType">the type of the field to be sorted.</param>
		void AddOrder(string fieldName, bool descending, Type fieldType);

		/// <summary>
		///   Includes the specified path in the query, loading the document specified in that path
		/// </summary>
		/// <param name = "path">The path.</param>
		void Include(string path);

		/// <summary>
		///   Includes the specified path in the query, loading the document specified in that path
		/// </summary>
		/// <param name = "path">The path.</param>
		void Include(Expression<Func<T, object>> path);

		/// <summary>
		///   Takes the specified count.
		/// </summary>
		/// <param name = "count">The count.</param>
		/// <returns></returns>
		void Take(int count);

		/// <summary>
		///   Skips the specified count.
		/// </summary>
		/// <param name = "count">The count.</param>
		/// <returns></returns>
		void Skip(int count);

		/// <summary>
		///   Filter the results from the index using the specified where clause.
		/// </summary>
		/// <param name = "whereClause">The where clause.</param>
		void Where(string whereClause);

		/// <summary>
		///   Matches exact value
		/// </summary>
		/// <remarks>
		///   Defaults to NotAnalyzed
		/// </remarks>
		void WhereEquals(string fieldName, object value);

		/// <summary>
		///   Matches exact value
		/// </summary>
		/// <remarks>
		///   Defaults to allow wildcards only if analyzed
		/// </remarks>
		void WhereEquals(string fieldName, object value, bool isAnalyzed);

		/// <summary>
		///   Simplified method for opening a new clause within the query
		/// </summary>
		/// <returns></returns>
		void OpenSubclause();

		///<summary>
		///  Instruct the index to group by the specified fields using the specified aggregation operation
		///</summary>
		///<remarks>
		///  This is only valid on dynamic indexes queries
		///</remarks>
		void GroupBy(AggregationOperation aggregationOperation, params string[] fieldsToGroupBy);

		/// <summary>
		///   Simplified method for closing a clause within the query
		/// </summary>
		/// <returns></returns>
		void CloseSubclause();

		/// <summary>
		///   Matches exact value
		/// </summary>
		void WhereEquals(WhereParams whereParams);

		///<summary>
		/// Negate the next operation
		///</summary>
		void NegateNext();

		/// <summary>
		/// Check that the field has one of the specified value
		/// </summary>
		void WhereIn(string fieldName, IEnumerable<object> values);

		/// <summary>
		///   Matches fields which starts with the specified value.
		/// </summary>
		/// <param name = "fieldName">Name of the field.</param>
		/// <param name = "value">The value.</param>
		void WhereStartsWith(string fieldName, object value);

		/// <summary>
		///   Matches fields which ends with the specified value.
		/// </summary>
		/// <param name = "fieldName">Name of the field.</param>
		/// <param name = "value">The value.</param>
		void WhereEndsWith(string fieldName, object value);

		/// <summary>
		///   Matches fields where the value is between the specified start and end, exclusive
		/// </summary>
		/// <param name = "fieldName">Name of the field.</param>
		/// <param name = "start">The start.</param>
		/// <param name = "end">The end.</param>
		/// <returns></returns>
		void WhereBetween(string fieldName, object start, object end);

		/// <summary>
		///   Matches fields where the value is between the specified start and end, inclusive
		/// </summary>
		/// <param name = "fieldName">Name of the field.</param>
		/// <param name = "start">The start.</param>
		/// <param name = "end">The end.</param>
		/// <returns></returns>
		void WhereBetweenOrEqual(string fieldName, object start, object end);

		/// <summary>
		///   Matches fields where the value is greater than the specified value
		/// </summary>
		/// <param name = "fieldName">Name of the field.</param>
		/// <param name = "value">The value.</param>
		void WhereGreaterThan(string fieldName, object value);

		/// <summary>
		///   Matches fields where the value is greater than or equal to the specified value
		/// </summary>
		/// <param name = "fieldName">Name of the field.</param>
		/// <param name = "value">The value.</param>
		void WhereGreaterThanOrEqual(string fieldName, object value);

		/// <summary>
		///   Matches fields where the value is less than the specified value
		/// </summary>
		/// <param name = "fieldName">Name of the field.</param>
		/// <param name = "value">The value.</param>
		void WhereLessThan(string fieldName, object value);

		/// <summary>
		///   Matches fields where the value is less than or equal to the specified value
		/// </summary>
		/// <param name = "fieldName">Name of the field.</param>
		/// <param name = "value">The value.</param>
		void WhereLessThanOrEqual(string fieldName, object value);

		/// <summary>
		///   Add an AND to the query
		/// </summary>
		void AndAlso();

		/// <summary>
		///   Add an OR to the query
		/// </summary>
		void OrElse();

		/// <summary>
		///   Specifies a boost weight to the last where clause.
		///   The higher the boost factor, the more relevant the term will be.
		/// </summary>
		/// <param name = "boost">boosting factor where 1.0 is default, less than 1.0 is lower weight, greater than 1.0 is higher weight</param>
		/// <returns></returns>
		/// <remarks>
		///   http://lucene.apache.org/java/2_4_0/queryparsersyntax.html#Boosting%20a%20Term
		/// </remarks>
		void Boost(decimal boost);

		/// <summary>
		///   Specifies a fuzziness factor to the single word term in the last where clause
		/// </summary>
		/// <param name = "fuzzy">0.0 to 1.0 where 1.0 means closer match</param>
		/// <returns></returns>
		/// <remarks>
		///   http://lucene.apache.org/java/2_4_0/queryparsersyntax.html#Fuzzy%20Searches
		/// </remarks>
		void Fuzzy(decimal fuzzy);

		/// <summary>
		///   Specifies a proximity distance for the phrase in the last where clause
		/// </summary>
		/// <param name = "proximity">number of words within</param>
		/// <returns></returns>
		/// <remarks>
		///   http://lucene.apache.org/java/2_4_0/queryparsersyntax.html#Proximity%20Searches
		/// </remarks>
		void Proximity(int proximity);

		/// <summary>
		///   Order the results by the specified fields
		///   The fields are the names of the fields to sort, defaulting to sorting by ascending.
		///   You can prefix a field name with '-' to indicate sorting by descending or '+' to sort by ascending
		/// </summary>
		/// <param name = "fields">The fields.</param>
		void OrderBy(params string[] fields);

		/// <summary>
		///   Instructs the query to wait for non stale results as of now.
		/// </summary>
		/// <returns></returns>
		void WaitForNonStaleResultsAsOfNow();

		/// <summary>
		///   Instructs the query to wait for non stale results as of now for the specified timeout.
		/// </summary>
		/// <param name = "waitTimeout">The wait timeout.</param>
		/// <returns></returns>
		void WaitForNonStaleResultsAsOfNow(TimeSpan waitTimeout);

		/// <summary>
		///   Instructs the query to wait for non stale results as of the cutoff date.
		/// </summary>
		/// <param name = "cutOff">The cut off.</param>
		/// <returns></returns>
		void WaitForNonStaleResultsAsOf(DateTime cutOff);

		/// <summary>
		///   Instructs the query to wait for non stale results as of the cutoff date for the specified timeout
		/// </summary>
		/// <param name = "cutOff">The cut off.</param>
		/// <param name = "waitTimeout">The wait timeout.</param>
		void WaitForNonStaleResultsAsOf(DateTime cutOff, TimeSpan waitTimeout);

		/// <summary>
		///   EXPERT ONLY: Instructs the query to wait for non stale results.
		///   This shouldn't be used outside of unit tests unless you are well aware of the implications
		/// </summary>
		void WaitForNonStaleResults();

		/// <summary>
		/// Perform a search for documents which fields that match the searchTerms.
		/// If there is more than a single term, each of them will be checked independently.
		/// </summary>
		void Search(string fieldName, string searchTerms, EscapeQueryOptions escapeQueryOptions = EscapeQueryOptions.RawQuery);

		/// <summary>
		///   Returns a <see cref = "System.String" /> that represents this instance.
		/// </summary>
		/// <returns>
		///   A <see cref = "System.String" /> that represents this instance.
		/// </returns>
		string ToString();

		/// <summary>
		///   The last term that we asked the query to use equals on
		/// </summary>
		KeyValuePair<string, string> GetLastEqualityTerm();

		void Intersect();
		void AddRootType(Type type);
	}
}