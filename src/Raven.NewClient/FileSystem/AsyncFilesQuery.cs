using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;

namespace Raven.NewClient.Client.FileSystem
{
    public class AsyncFilesQuery<T> : AbstractFilesQuery<T, AsyncFilesQuery<T>>, IAsyncFilesQuery<T>, IAsyncFilesOrderedQuery<T> where T : class
    {
        public AsyncFilesQuery( InMemoryFilesSessionOperations theSession, IAsyncFilesCommands commands ) : base ( theSession, commands )
        {}

        #region IAsyncFilesQuery operations

        /// <summary>
        ///     Provide statistics about the query, such as total count of matching records
        /// </summary>
        IAsyncFilesQuery<T> IAsyncFilesQueryBase<T, IAsyncFilesQuery<T>>.Statistics(out FilesQueryStatistics stats)
        {
            Statistics(out stats);
            return this;
        }

        /// <summary>
        ///   This function exists solely to forbid in memory where clause on IAsyncFilesQuery, because
        ///   that is nearly always a mistake.
        /// </summary>
        [Obsolete(@"You cannot issue an in memory filter - such as Where(x=>x.Name == ""Test.file"") - on IAsyncFilesQuery. 
This is likely a bug, because this will execute the filter in memory, rather than in RavenDB.
Consider using session.Query() instead of session.AsyncFilesQuery<T>. The session.Query() method supports partial Linq queries, while session.AsyncFilesQuery<T>() is intended for lower level API access.
If you really want to do in memory filtering on the data returned from the query, you can use: session.Query<T>().ToList().Where(x=>x.Name == ""Test.file"")
", true)]
        IEnumerable<T> IAsyncFilesQueryBase<T, IAsyncFilesQuery<T>>.Where(Func<T, bool> predicate)
        {
            throw new NotSupportedException();
        }

        /// <summary>
        /// Filter the results using the specified where clause.
        /// </summary>
        /// <param name="whereClause">The where clause.</param>
        IAsyncFilesQuery<T> IAsyncFilesQueryBase<T, IAsyncFilesQuery<T>>.Where(string whereClause)
        {
            Where(whereClause);
            return this;
        }

        /// <summary>
        /// 	Matches exact value
        /// </summary>
        IAsyncFilesQuery<T> IAsyncFilesQueryBase<T, IAsyncFilesQuery<T>>.WhereEquals(string fieldName, object value)
        {
            WhereEquals(fieldName, value);
            return this;
        }

        /// <summary>
        /// 	Matches exact value
        /// </summary>
        IAsyncFilesQuery<T> IAsyncFilesQueryBase<T, IAsyncFilesQuery<T>>.WhereEquals<TValue>(Expression<Func<T, TValue>> propertySelector, TValue value)
        {
            WhereEquals(GetMemberQueryPath(propertySelector.Body), value);
            return this;
        }

        /// <summary>
        /// 	Matches exact value
        /// </summary>
        IAsyncFilesQuery<T> IAsyncFilesQueryBase<T, IAsyncFilesQuery<T>>.WhereEquals(WhereParams whereParams)
        {
            WhereEquals(whereParams);
            return this;
        }

        /// <summary>
        /// Check that the field has one of the specified value
        /// </summary>
        IAsyncFilesQuery<T> IAsyncFilesQueryBase<T, IAsyncFilesQuery<T>>.WhereIn(string fieldName, IEnumerable<object> values)
        {
            WhereIn(fieldName, values);
            return this;
        }

        /// <summary>
        /// Check that the field has one of the specified value
        /// </summary>
        IAsyncFilesQuery<T> IAsyncFilesQueryBase<T, IAsyncFilesQuery<T>>.WhereIn<TValue>(Expression<Func<T, TValue>> propertySelector, IEnumerable<TValue> values)
        {
            WhereIn(GetMemberQueryPath(propertySelector.Body), values.Cast<object>());
            return this;
        }

        /// <summary>
        /// Matches fields which starts with the specified value.
        /// </summary>
        /// <param name="fieldName">Name of the field.</param>
        /// <param name="value">The value.</param>
         IAsyncFilesQuery<T> IAsyncFilesQueryBase<T, IAsyncFilesQuery<T>>.WhereStartsWith(string fieldName, object value)
        {
            WhereStartsWith(fieldName, value);
            return this;
        }

        /// <summary>
        ///   Matches fields which starts with the specified value.
        /// </summary>
        /// <param name = "propertySelector">Property selector for the field.</param>
        /// <param name = "value">The value.</param>
         IAsyncFilesQuery<T> IAsyncFilesQueryBase<T, IAsyncFilesQuery<T>>.WhereStartsWith<TValue>(Expression<Func<T, TValue>> propertySelector, TValue value)
        {
            WhereStartsWith(GetMemberQueryPath(propertySelector.Body), value);
            return this;
        }

        /// <summary>
        /// Matches fields which ends with the specified value.
        /// </summary>
        /// <param name="fieldName">Name of the field.</param>
        /// <param name="value">The value.</param>
         IAsyncFilesQuery<T> IAsyncFilesQueryBase<T, IAsyncFilesQuery<T>>.WhereEndsWith(string fieldName, object value)
        {
            WhereEndsWith(fieldName, value);
            return this;
        }

        /// <summary>
        ///   Matches fields which ends with the specified value.
        /// </summary>
        /// <param name = "propertySelector">Property selector for the field.</param>
        /// <param name = "value">The value.</param>
         IAsyncFilesQuery<T> IAsyncFilesQueryBase<T, IAsyncFilesQuery<T>>.WhereEndsWith<TValue>(Expression<Func<T, TValue>> propertySelector, TValue value)
        {
            WhereEndsWith(GetMemberQueryPath(propertySelector.Body), value);
            return this;
        }

        /// <summary>
        ///   Matches fields where the value is between the specified start and end, exclusive
        /// </summary>
        /// <param name = "fieldName">Name of the field.</param>
        /// <param name = "start">The start.</param>
        /// <param name = "end">The end.</param>
        IAsyncFilesQuery<T> IAsyncFilesQueryBase<T, IAsyncFilesQuery<T>>.WhereBetween(string fieldName, object start, object end)
        {
            WhereBetween(fieldName, start, end);
            return this;
        }

        /// <summary>
        ///   Matches fields where the value is between the specified start and end, exclusive
        /// </summary>
        /// <param name = "propertySelector">Property selector for the field.</param>
        /// <param name = "start">The start.</param>
        /// <param name = "end">The end.</param>
        IAsyncFilesQuery<T> IAsyncFilesQueryBase<T, IAsyncFilesQuery<T>>.WhereBetween<TValue>(Expression<Func<T, TValue>> propertySelector, TValue start, TValue end)
        {
            WhereBetween(GetMemberQueryPath(propertySelector.Body), start, end);
            return this;
        }

        /// <summary>
        ///   Matches fields where the value is between the specified start and end, inclusive
        /// </summary>
        /// <param name = "fieldName">Name of the field.</param>
        /// <param name = "start">The start.</param>
        /// <param name = "end">The end.</param>
        IAsyncFilesQuery<T> IAsyncFilesQueryBase<T, IAsyncFilesQuery<T>>.WhereBetweenOrEqual(string fieldName, object start, object end)
        {
            WhereBetweenOrEqual(fieldName, start, end);
            return this;
        }

        /// <summary>
        ///   Matches fields where the value is between the specified start and end, inclusive
        /// </summary>
        /// <param name = "propertySelector">Property selector for the field.</param>
        /// <param name = "start">The start.</param>
        /// <param name = "end">The end.</param>
        IAsyncFilesQuery<T> IAsyncFilesQueryBase<T, IAsyncFilesQuery<T>>.WhereBetweenOrEqual<TValue>(Expression<Func<T, TValue>> propertySelector, TValue start, TValue end)
        {
            WhereBetweenOrEqual(GetMemberQueryPath(propertySelector.Body), start, end);
            return this;
        }

        /// <summary>
        /// Matches fields where the value is greater than the specified value
        /// </summary>
        /// <param name="fieldName">Name of the field.</param>
        /// <param name="value">The value.</param>
        IAsyncFilesQuery<T> IAsyncFilesQueryBase<T, IAsyncFilesQuery<T>>.WhereGreaterThan(string fieldName, object value)
        {
            WhereGreaterThan(fieldName, value);
            return this;
        }

        /// <summary>
        ///   Matches fields where the value is greater than the specified value
        /// </summary>
        /// <param name = "propertySelector">Property selector for the field.</param>
        /// <param name = "value">The value.</param>
        IAsyncFilesQuery<T> IAsyncFilesQueryBase<T, IAsyncFilesQuery<T>>.WhereGreaterThan<TValue>(Expression<Func<T, TValue>> propertySelector, TValue value)
        {
            WhereGreaterThan(GetMemberQueryPath(propertySelector.Body), value);
            return this;
        }

        /// <summary>
        /// Matches fields where the value is greater than the specified value
        /// </summary>
        /// <param name="fieldName">Name of the field.</param>
        /// <param name="value">The value.</param>
        IAsyncFilesQuery<T> IAsyncFilesQueryBase<T, IAsyncFilesQuery<T>>.WhereGreaterThanOrEqual(string fieldName, object value)
        {
            WhereGreaterThanOrEqual(fieldName, value);
            return this;
        }

        /// <summary>
        ///   Matches fields where the value is greater than the specified value
        /// </summary>
        /// <param name = "propertySelector">Property selector for the field.</param>
        /// <param name = "value">The value.</param>
        IAsyncFilesQuery<T> IAsyncFilesQueryBase<T, IAsyncFilesQuery<T>>.WhereGreaterThanOrEqual<TValue>(Expression<Func<T, TValue>> propertySelector, TValue value)
        {
            WhereGreaterThanOrEqual(GetMemberQueryPath(propertySelector.Body), value);
            return this;
        }

        /// <summary>
        /// Matches fields where the value is less than the specified value
        /// </summary>
        /// <param name="fieldName">Name of the field.</param>
        /// <param name="value">The value.</param>
        IAsyncFilesQuery<T> IAsyncFilesQueryBase<T, IAsyncFilesQuery<T>>.WhereLessThan(string fieldName, object value)
        {
            WhereLessThan(fieldName, value);
            return this;
        }

        /// <summary>
        ///   Matches fields where the value is less than the specified value
        /// </summary>
        /// <param name = "propertySelector">Property selector for the field.</param>
        /// <param name = "value">The value.</param>
        IAsyncFilesQuery<T> IAsyncFilesQueryBase<T, IAsyncFilesQuery<T>>.WhereLessThan<TValue>(Expression<Func<T, TValue>> propertySelector, TValue value)
        {
            WhereLessThan(GetMemberQueryPath(propertySelector.Body), value);
            return this;
        }

        /// <summary>
        /// Matches fields where the value is less than or equal to the specified value
        /// </summary>
        /// <param name="fieldName">Name of the field.</param>
        /// <param name="value">The value.</param>
        IAsyncFilesQuery<T> IAsyncFilesQueryBase<T, IAsyncFilesQuery<T>>.WhereLessThanOrEqual(string fieldName, object value)
        {
            WhereLessThanOrEqual(fieldName, value);
            return this;
        }

        /// <summary>
        ///   Matches fields where the value is less than or equal to the specified value
        /// </summary>
        /// <param name = "propertySelector">Property selector for the field.</param>
        /// <param name = "value">The value.</param>
        IAsyncFilesQuery<T> IAsyncFilesQueryBase<T, IAsyncFilesQuery<T>>.WhereLessThanOrEqual<TValue>(Expression<Func<T, TValue>> propertySelector, TValue value)
        {
            WhereLessThanOrEqual(GetMemberQueryPath(propertySelector.Body), value);
            return this;
        }

        /// <summary>
        /// Performs a query matching ANY of the provided values against the given field (OR)
        /// </summary>
        IAsyncFilesQuery<T> IAsyncFilesQueryBase<T, IAsyncFilesQuery<T>>.ContainsAny(string fieldName, IEnumerable<object> values)
        {
            ContainsAny(fieldName, values);
            return this;
        }

        /// <summary>
        /// Performs a query matching ANY of the provided values against the given field (OR)
        /// </summary>
        IAsyncFilesQuery<T> IAsyncFilesQueryBase<T, IAsyncFilesQuery<T>>.ContainsAny<TValue>(Expression<Func<T, TValue>> propertySelector, IEnumerable<object> values)
        {
            ContainsAny(GetMemberQueryPath(propertySelector.Body), values);
            return this;
        }

        /// <summary>
        /// Performs a query matching ALL of the provided values against the given field (AND)
        /// </summary>
        IAsyncFilesQuery<T> IAsyncFilesQueryBase<T, IAsyncFilesQuery<T>>.ContainsAll(string fieldName, IEnumerable<object> values)
        {
            ContainsAll(fieldName, values);
            return this;
        }

        /// <summary>
        /// Performs a query matching ALL of the provided values against the given field (AND)
        /// </summary>
        IAsyncFilesQuery<T> IAsyncFilesQueryBase<T, IAsyncFilesQuery<T>>.ContainsAll<TValue>(Expression<Func<T, TValue>> propertySelector, IEnumerable<object> values)
        {
            ContainsAll(GetMemberQueryPath(propertySelector.Body), values);
            return this;
        }

        /// <summary>
        /// Add an AND to the query
        /// </summary>
        IAsyncFilesQuery<T> IAsyncFilesQueryBase<T, IAsyncFilesQuery<T>>.AndAlso()
        {
            AndAlso();
            return this;
        }

        /// <summary>
        /// Add an OR to the query
        /// </summary>
        IAsyncFilesQuery<T> IAsyncFilesQueryBase<T, IAsyncFilesQuery<T>>.OrElse()
        {
            OrElse();
            return this;
        }

        /// <summary>
        /// Order the results by the specified fields
        /// The fields are the names of the fields to sort, defaulting to sorting by ascending.
        /// </summary>
        /// <param name="fields">The fields.</param>
        IAsyncFilesOrderedQuery<T> IAsyncFilesQueryBase<T, IAsyncFilesQuery<T>>.OrderBy(params string[] fields)
        {
            OrderBy(fields);
            return (IAsyncFilesOrderedQuery<T>)this;
        }

        /// <summary>
        ///   Order the results by the specified fields
        ///   The fields are the names of the fields to sort, defaulting to sorting by ascending.
        /// </summary>
        /// <param name = "propertySelectors">Property selectors for the fields.</param>
        IAsyncFilesOrderedQuery<T> IAsyncFilesQueryBase<T, IAsyncFilesQuery<T>>.OrderBy<TValue>(params Expression<Func<T, TValue>>[] propertySelectors)
        {
            var orderByfields = propertySelectors.Select(GetMemberQueryPathForOrderBy).ToArray();
            OrderBy(orderByfields);
            return (IAsyncFilesOrderedQuery<T>)this;
        }

        /// <summary>
        /// Order the results by the specified fields
        /// The fields are the names of the fields to sort, defaulting to sorting by descending.
        /// </summary>
        /// <param name="fields">The fields.</param>
        IAsyncFilesOrderedQuery<T> IAsyncFilesQueryBase<T, IAsyncFilesQuery<T>>.OrderByDescending(params string[] fields)
        {
            OrderByDescending(fields);
            return (IAsyncFilesOrderedQuery<T>)this;
        }

        /// <summary>
        ///   Order the results by the specified fields
        ///   The fields are the names of the fields to sort, defaulting to sorting by descending.
        /// </summary>
        /// <param name = "propertySelectors">Property selectors for the fields.</param>
        IAsyncFilesOrderedQuery<T> IAsyncFilesQueryBase<T, IAsyncFilesQuery<T>>.OrderByDescending<TValue>(params Expression<Func<T, TValue>>[] propertySelectors)
        {
            var orderByfields = propertySelectors.Select(expression => GetMemberQueryPathForOrderBy(expression)).ToArray();
            OrderByDescending(orderByfields);
            return (IAsyncFilesOrderedQuery<T>)this;
        }

        /// <summary>
        /// Order the results by the specified fields
        /// The fields are the names of the fields to sort, defaulting to sorting by ascending.
        /// </summary>
        /// <param name="fields">The fields.</param>
        IAsyncFilesOrderedQuery<T> IAsyncFilesOrderedQueryBase<T, IAsyncFilesQuery<T>>.ThenBy(params string[] fields)
        {
            foreach (var field in fields)
                AddOrder(field, false);

            return (IAsyncFilesOrderedQuery<T>)this;
        }

        /// <summary>
        ///   Order the results by the specified fields
        ///   The fields are the names of the fields to sort, defaulting to sorting by ascending.
        /// </summary>
        /// <param name = "propertySelectors">Property selectors for the fields.</param>
        IAsyncFilesOrderedQuery<T> IAsyncFilesOrderedQueryBase<T, IAsyncFilesQuery<T>>.ThenBy<TValue>(params Expression<Func<T, TValue>>[] propertySelectors)
        {
            var orderByfields = propertySelectors.Select(expression => GetMemberQueryPathForOrderBy(expression)).ToArray();
            foreach (var field in orderByfields)
                AddOrder(field, false);

            return (IAsyncFilesOrderedQuery<T>)this;
        }

        /// <summary>
        /// Order the results by the specified fields
        /// The fields are the names of the fields to sort, defaulting to sorting by descending.
        /// </summary>
        /// <param name="fields">The fields.</param>
        IAsyncFilesOrderedQuery<T> IAsyncFilesOrderedQueryBase<T, IAsyncFilesQuery<T>>.ThenByDescending(params string[] fields)
        {
            foreach (var field in fields)
                AddOrder(field, true);

            return (IAsyncFilesOrderedQuery<T>)this;
        }

        /// <summary>
        ///   Order the results by the specified fields
        ///   The fields are the names of the fields to sort, defaulting to sorting by descending.
        /// </summary>
        /// <param name = "propertySelectors">Property selectors for the fields.</param>
        IAsyncFilesOrderedQuery<T> IAsyncFilesOrderedQueryBase<T, IAsyncFilesQuery<T>>.ThenByDescending<TValue>(params Expression<Func<T, TValue>>[] propertySelectors)
        {
            var orderByfields = propertySelectors.Select(expression => GetMemberQueryPathForOrderBy(expression)).ToArray();
            foreach (var field in orderByfields)
                AddOrder(field, true);

            return (IAsyncFilesOrderedQuery<T>)this;
        }

        FilesConvention IAsyncFilesQueryBase<T, IAsyncFilesQuery<T>>.Conventions
        {
            get { return this.Conventions; }
        }


        bool IAsyncFilesQuery<T>.IsDistinct
        {
            get { return isDistinct; }
        }

        #endregion 

        /// <summary>
        ///   Takes the specified count.
        /// </summary>
        /// <param name = "count">The count.</param>
        IAsyncFilesQuery<T> IAsyncFilesQueryBase<T, IAsyncFilesQuery<T>>.Take(int count)
        {
            Take(count);
            return this;
        }

        /// <summary>
        ///   Skips the specified count.
        /// </summary>
        /// <param name = "count">The count.</param>
        IAsyncFilesQuery<T> IAsyncFilesQueryBase<T, IAsyncFilesQuery<T>>.Skip(int count)
        {
            Skip(count);
            return this;
        }

        public IAsyncFilesQuery<T> OnDirectory(string path = null, bool recursive = false)
        {
            if (string.IsNullOrWhiteSpace(path))
                path = string.Empty;

            string normalizedPath;
            if (!path.StartsWith("/"))
                normalizedPath = "/" + path.TrimEnd('/');
            else
                normalizedPath = path.TrimEnd('/');

            if (recursive)
            {
                this.WhereStartsWith("directory", normalizedPath);
                this.AndAlso();
            }
            else
            {            
                if ( string.IsNullOrWhiteSpace(path) )
                {
                    this.OpenSubclause();
                    {
                        this.WhereStartsWith("directory", "/");
                        this.AndAlso();
                        this.WhereEndsWith("directory", "/");                      
                    }
                    this.CloseSubclause();
                }
                else
                {
                    this.OpenSubclause();
                    {
                        this.WhereEquals("directory", normalizedPath);
                        this.AndAlso();
                        this.NegateNext();
                        this.WhereStartsWith("directory", normalizedPath + "/");
                    }
                    this.CloseSubclause();
                }

                this.AndAlso();
            }

            return this;
        }

        void IAsyncFilesQuery<T>.RegisterResultsForDeletion()
        {
            var query = ToString();
            if (string.IsNullOrWhiteSpace(query))
                throw new ArgumentException("Query is empty! Did you forget OnDirectory before RegisterDeletion?");

            Session.RegisterDeletionQuery(query);
        }

        public FilesQuery GetFilesQuery()
        {
            return new FilesQuery(ToString(), start, pageSizeSet ? pageSize : (int?) null, orderByFields);
        }
    }
}
