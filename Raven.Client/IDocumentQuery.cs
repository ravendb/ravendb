using System;
using System.Collections.Generic;
using Raven.Database.Data;

namespace Raven.Client
{
	public interface IDocumentQuery<T> : IEnumerable<T>
	{
		IDocumentQuery<T> Take(int count);
		IDocumentQuery<T> Skip(int count);
		IDocumentQuery<T> Where(string whereClause);
		IDocumentQuery<T> WhereEqual(string fieldName, object value);
		IDocumentQuery<T> WhereEqual(string fieldName, object value, bool isAnalyzed);
		IDocumentQuery<T> WhereContains(string fieldName, object value);
		IDocumentQuery<T> WhereStartsWith(string fieldName, object value);
		IDocumentQuery<T> WhereEndsWith(string fieldName, object value);
		IDocumentQuery<T> WhereBetween(string fieldName, object start, object end);
		IDocumentQuery<T> WhereBetweenOrEqual(string fieldName, object start, object end);
		IDocumentQuery<T> WhereGreaterThan(string fieldName, object value);
		IDocumentQuery<T> WhereGreaterThanOrEqual(string fieldName, object value);
		IDocumentQuery<T> WhereLessThan(string fieldName, object value);
		IDocumentQuery<T> WhereLessThanOrEqual(string fieldName, object value);
		IDocumentQuery<T> AndAlso();
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

		IDocumentQuery<T> OrderBy(params string[] fields);

        IDocumentQuery<T> WaitForNonStaleResultsAsOfNow();
        IDocumentQuery<T> WaitForNonStaleResultsAsOfNow(TimeSpan waitTimeout);

        IDocumentQuery<T> WaitForNonStaleResultsAsOf(DateTime cutOff);
        IDocumentQuery<T> WaitForNonStaleResultsAsOf(DateTime cutOff, TimeSpan waitTimeout);

		IDocumentQuery<T> WaitForNonStaleResults();
        IDocumentQuery<T> WaitForNonStaleResults(TimeSpan waitTimeout);
		IDocumentQuery<TProjection> SelectFields<TProjection>(params string[] fields);

		QueryResult QueryResult { get; }
	}
}