using System.Collections.Generic;

namespace Raven.NewClient.Client.FileSystem
{
    public interface IAbstractFilesQuery<T>
    {
        /// <summary>
        /// Gets the files convention from the query session
        /// </summary>
        FilesConvention Conventions { get; }

        /// <summary>
        ///   Simplified method for opening a new clause within the query
        /// </summary>
        /// <returns></returns>
        void OpenSubclause();

        /// <summary>
        ///   Simplified method for closing a clause within the query
        /// </summary>
        /// <returns></returns>
        void CloseSubclause();

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
        void WhereEquals(WhereParams whereParams);

        /// <summary>
        /// Check that the field has one of the specified value
        /// </summary>
        void WhereIn(string fieldName, IEnumerable<object> values);

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

        void ContainsAny(string fieldName, IEnumerable<object> values);

        void ContainsAll(string fieldName, IEnumerable<object> values);

        /// <summary>
        ///   Add an AND to the query
        /// </summary>
        void AndAlso();

        /// <summary>
        ///   Add an OR to the query
        /// </summary>
        void OrElse();

        /// <summary>
        ///   Order the results by the specified fields
        ///   The fields are the names of the fields to sort, defaulting to sorting by ascending.
        ///   You can prefix a field name with '-' to indicate sorting by descending or '+' to sort by ascending
        /// </summary>
        /// <param name = "fields">The fields.</param>
        void OrderBy(params string[] fields);

        /// <summary>
        ///   Adds an ordering for a specific field to the query
        /// </summary>
        /// <param name = "fieldName">Name of the field.</param>
        /// <param name = "descending">if set to <c>true</c> [descending].</param>
        void AddOrder(string fieldName, bool descending);

        /// <summary>
        ///   Returns a <see cref = "System.String" /> that represents this instance.
        /// </summary>
        /// <returns>
        ///   A <see cref = "System.String" /> that represents this instance.
        /// </returns>
        string ToString();

        void Distinct();
    }
}
