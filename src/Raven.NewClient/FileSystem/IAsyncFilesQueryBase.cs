using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Threading.Tasks;

namespace Raven.NewClient.Client.FileSystem
{
    public interface IAsyncFilesOrderedQueryBase<T, out TSelf> : IAsyncFilesQueryBase<T,TSelf> where TSelf : IAsyncFilesQueryBase<T, TSelf>
    {
        /// <summary>
        ///   Order the results by the specified fields
        ///   The fields are the names of the fields to sort.
        /// </summary>
        /// <param name = "fields">The fields.</param>
        IAsyncFilesOrderedQuery<T> ThenBy(params string[] fields);

        /// <summary>
        ///   Order the results by the specified fields
        ///   The fields are the names of the fields to sort.
        /// </summary>
        /// <param name = "propertySelectors">Property selectors for the fields.</param>
        IAsyncFilesOrderedQuery<T> ThenBy<TValue>(params Expression<Func<T, TValue>>[] propertySelectors);

        /// <summary>
        ///   Order the results by the specified fields
        ///   The fields are the names of the fields to sort.
        /// </summary>
        /// <param name = "fields">The fields.</param>
        IAsyncFilesOrderedQuery<T> ThenByDescending(params string[] fields);

        /// <summary>
        ///   Order the results by the specified fields
        ///   The fields are the names of the fields to sort.
        /// </summary>
        /// <param name = "propertySelectors">Property selectors for the fields.</param>
        IAsyncFilesOrderedQuery<T> ThenByDescending<TValue>(params Expression<Func<T, TValue>>[] propertySelectors);
    }

    /// <summary>
    /// A query against a file system.
    /// </summary>
    public interface IAsyncFilesQueryBase<T, out TSelf>
        where TSelf : IAsyncFilesQueryBase<T, TSelf> 
    {
        /// <summary>
        /// Gets the files convention from the query session
        /// </summary>
        FilesConvention Conventions { get; }

        /// <summary>
        ///   Provide statistics about the query, such as total count of matching records
        /// </summary>
        TSelf Statistics(out FilesQueryStatistics stats);

        /// <summary>
        ///   This function exists solely to forbid in memory where clause on IFilesQuery, because
        ///   that is nearly always a mistake.
        /// </summary>
        [Obsolete(@"You cannot issue an in memory filter - such as Where(x=>x.Name == ""Test.file"") - on IFilesQuery. 
This is likely a bug, because this will execute the filter in memory, rather than in RavenFS.", true)]
        IEnumerable<T> Where(Func<T, bool> predicate);

        /// <summary>
        ///   Filter the results from the index using the specified where clause.
        /// </summary>
        /// <param name = "whereClause">The where clause.</param>
        TSelf Where(string whereClause);

        /// <summary>
        ///   Matches exact value
        /// </summary>
        /// <remarks>
        ///   Defaults to NotAnalyzed
        /// </remarks>
        TSelf WhereEquals(string fieldName, object value);

        /// <summary>
        ///   Matches exact value
        /// </summary>
        /// <remarks>
        ///   Defaults to NotAnalyzed
        /// </remarks>
        TSelf WhereEquals<TValue>(Expression<Func<T, TValue>> propertySelector, TValue value);

        /// <summary>
        /// Matches exact value
        /// </summary>
        TSelf WhereEquals(WhereParams whereParams);

        /// <summary>
        /// Check that the field has one of the specified value
        /// </summary>
        TSelf WhereIn(string fieldName, IEnumerable<object> values);

        /// <summary>
        /// Check that the field has one of the specified value
        /// </summary>
        TSelf WhereIn<TValue>(Expression<Func<T, TValue>> propertySelector, IEnumerable<TValue> values);

        /// <summary>
        ///   Matches fields which starts with the specified value.
        /// </summary>
        /// <param name = "fieldName">Name of the field.</param>
        /// <param name = "value">The value.</param>
        TSelf WhereStartsWith(string fieldName, object value);

        /// <summary>
        ///   Matches fields which starts with the specified value.
        /// </summary>
        /// <param name = "propertySelector">Property selector for the field.</param>
        /// <param name = "value">The value.</param>
        TSelf WhereStartsWith<TValue>(Expression<Func<T, TValue>> propertySelector, TValue value);

        /// <summary>
        ///   Matches fields which ends with the specified value.
        /// </summary>
        /// <param name = "fieldName">Name of the field.</param>
        /// <param name = "value">The value.</param>
        TSelf WhereEndsWith(string fieldName, object value);

        /// <summary>
        ///   Matches fields which ends with the specified value.
        /// </summary>
        /// <param name = "propertySelector">Property selector for the field.</param>
        /// <param name = "value">The value.</param>
        TSelf WhereEndsWith<TValue>(Expression<Func<T, TValue>> propertySelector, TValue value);

        /// <summary>
        ///   Matches fields where the value is between the specified start and end, exclusive
        /// </summary>
        /// <param name = "fieldName">Name of the field.</param>
        /// <param name = "start">The start.</param>
        /// <param name = "end">The end.</param>
        TSelf WhereBetween(string fieldName, object start, object end);

        /// <summary>
        ///   Matches fields where the value is between the specified start and end, exclusive
        /// </summary>
        /// <param name = "propertySelector">Property selector for the field.</param>
        /// <param name = "start">The start.</param>
        /// <param name = "end">The end.</param>
        TSelf WhereBetween<TValue>(Expression<Func<T, TValue>> propertySelector, TValue start, TValue end);

        /// <summary>
        ///   Matches fields where the value is between the specified start and end, inclusive
        /// </summary>
        /// <param name = "fieldName">Name of the field.</param>
        /// <param name = "start">The start.</param>
        /// <param name = "end">The end.</param>
        TSelf WhereBetweenOrEqual(string fieldName, object start, object end);

        /// <summary>
        ///   Matches fields where the value is between the specified start and end, inclusive
        /// </summary>
        /// <param name = "propertySelector">Property selector for the field.</param>
        /// <param name = "start">The start.</param>
        /// <param name = "end">The end.</param>
        TSelf WhereBetweenOrEqual<TValue>(Expression<Func<T, TValue>> propertySelector, TValue start, TValue end);

        /// <summary>
        ///   Matches fields where the value is greater than the specified value
        /// </summary>
        /// <param name = "fieldName">Name of the field.</param>
        /// <param name = "value">The value.</param>
        TSelf WhereGreaterThan(string fieldName, object value);

        /// <summary>
        ///   Matches fields where the value is greater than the specified value
        /// </summary>
        /// <param name = "propertySelector">Property selector for the field.</param>
        /// <param name = "value">The value.</param>
        TSelf WhereGreaterThan<TValue>(Expression<Func<T, TValue>> propertySelector, TValue value);

        /// <summary>
        ///   Matches fields where the value is greater than or equal to the specified value
        /// </summary>
        /// <param name = "fieldName">Name of the field.</param>
        /// <param name = "value">The value.</param>
        TSelf WhereGreaterThanOrEqual(string fieldName, object value);

        /// <summary>
        ///   Matches fields where the value is greater than or equal to the specified value
        /// </summary>
        /// <param name = "propertySelector">Property selector for the field.</param>
        /// <param name = "value">The value.</param>
        TSelf WhereGreaterThanOrEqual<TValue>(Expression<Func<T, TValue>> propertySelector, TValue value);

        /// <summary>
        ///   Matches fields where the value is less than the specified value
        /// </summary>
        /// <param name = "fieldName">Name of the field.</param>
        /// <param name = "value">The value.</param>
        TSelf WhereLessThan(string fieldName, object value);

        /// <summary>
        ///   Matches fields where the value is less than the specified value
        /// </summary>
        /// <param name = "propertySelector">Property selector for the field.</param>
        /// <param name = "value">The value.</param>
        TSelf WhereLessThan<TValue>(Expression<Func<T, TValue>> propertySelector, TValue value);

        /// <summary>
        ///   Matches fields where the value is less than or equal to the specified value
        /// </summary>
        /// <param name = "fieldName">Name of the field.</param>
        /// <param name = "value">The value.</param>
        TSelf WhereLessThanOrEqual(string fieldName, object value);

        /// <summary>
        ///   Matches fields where the value is less than or equal to the specified value
        /// </summary>
        /// <param name = "propertySelector">Property selector for the field.</param>
        /// <param name = "value">The value.</param>
        TSelf WhereLessThanOrEqual<TValue>(Expression<Func<T, TValue>> propertySelector, TValue value);


        /// <summary>
        /// Performs a query matching ANY of the provided values against the given field (OR)
        /// </summary>
        TSelf ContainsAny(string fieldName, IEnumerable<object> values);

        /// <summary>
        /// Performs a query matching ANY of the provided values against the given field (OR)
        /// </summary>
        TSelf ContainsAny<TValue>(Expression<Func<T, TValue>> propertySelector, IEnumerable<object> values);

        /// <summary>
        /// Performs a query matching ALL of the provided values against the given field (AND)
        /// </summary>
        TSelf ContainsAll(string fieldName, IEnumerable<object> values);

        /// <summary>
        /// Performs a query matching ALL of the provided values against the given field (AND)
        /// </summary>
        TSelf ContainsAll<TValue>(Expression<Func<T, TValue>> propertySelector, IEnumerable<object> values);

        /// <summary>
        ///   Order the results by the specified fields
        ///   The fields are the names of the fields to sort, defaulting to sorting by ascending.
        /// </summary>
        /// <param name = "fields">The fields.</param>
        IAsyncFilesOrderedQuery<T> OrderBy(params string[] fields);

        /// <summary>
        ///   Order the results by the specified fields
        ///   The fields are the names of the fields to sort, defaulting to sorting by ascending.
        /// </summary>
        /// <param name = "propertySelectors">Property selectors for the fields.</param>
        IAsyncFilesOrderedQuery<T> OrderBy<TValue>(params Expression<Func<T, TValue>>[] propertySelectors);

        /// <summary>
        ///   Order the results by the specified fields
        ///   The fields are the names of the fields to sort, defaulting to sorting by descending.
        /// </summary>
        /// <param name = "fields">The fields.</param>
        IAsyncFilesOrderedQuery<T> OrderByDescending(params string[] fields);

        /// <summary>
        ///   Order the results by the specified fields
        ///   The fields are the names of the fields to sort, defaulting to sorting by descending.
        /// </summary>
        /// <param name = "propertySelectors">Property selectors for the fields.</param>
        IAsyncFilesOrderedQuery<T> OrderByDescending<TValue>(params Expression<Func<T, TValue>>[] propertySelectors);

        /// <summary>
        ///   Add an AND to the query
        /// </summary>
        TSelf AndAlso();

        /// <summary>
        ///   Add an OR to the query
        /// </summary>
        TSelf OrElse();


        /// <summary>
        ///   Returns first element or default value for type if sequence is empty.
        /// </summary>
        /// <returns></returns>
        Task<T> FirstOrDefaultAsync();

        /// <summary>
        ///   Returns first element or throws if sequence is empty.
        /// </summary>
        /// <returns></returns>
        Task<T> FirstAsync();

        /// <summary>
        ///   Returns first element or default value for given type if sequence is empty. Throws if sequence contains more than one element.
        /// </summary>
        /// <returns></returns>
        Task<T> SingleOrDefaultAsync();

        /// <summary>
        ///   Returns first element or throws if sequence is empty or contains more than one element.
        /// </summary>
        /// <returns></returns>
        Task<T> SingleAsync();

        /// <summary>
        ///   Takes the specified count.
        /// </summary>
        /// <param name = "count">The count.</param>
        /// <returns></returns>
        TSelf Take(int count);

        /// <summary>
        ///   Skips the specified count.
        /// </summary>
        /// <param name = "count">The count.</param>
        /// <returns></returns>
        TSelf Skip(int count);
    }
}
