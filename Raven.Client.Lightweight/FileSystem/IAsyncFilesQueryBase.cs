using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading.Tasks;

namespace Raven.Client.FileSystem
{

    /// <summary>
    /// A query against a file system.
    /// </summary>
    public interface IAsyncFilesQueryBase<T, out TSelf> where TSelf : IAsyncFilesQueryBase<T, TSelf>
    {
        /// <summary>
        /// Gets the files convention from the query session
        /// </summary>
        FilesConvention Conventions { get; }


        /// <summary>
        ///   This function exists solely to forbid in memory where clause on IFilesQuery, because
        ///   that is nearly always a mistake.
        /// </summary>
        [Obsolete(@"You cannot issue an in memory filter - such as Where(x=>x.Name == ""Test.file"") - on IFilesQuery. 
This is likely a bug, because this will execute the filter in memory, rather than in RavenDB.
Consider using session.Query<T>() instead of session.Query. The session.Query() method fully supports Linq queries, while session.Query() is intended for lower level API access.
If you really want to do in memory filtering on the data returned from the query, you can use: session.Query<T>().ToList().Where(x=>x.Name == ""Test.file"")
", true)]
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
    }
}
