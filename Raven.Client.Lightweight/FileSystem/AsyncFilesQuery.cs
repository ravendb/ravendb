using Raven.Abstractions.FileSystem;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading.Tasks;

namespace Raven.Client.FileSystem
{
    public class AsyncFilesQuery<T> : AbstractFilesQuery<T, AsyncFilesQuery<T>>, IAsyncFilesQuery<T> where T : class
    {
        public AsyncFilesQuery( InMemoryFilesSessionOperations theSession, IAsyncFilesCommands commands ) : base ( theSession, commands )
        {}

        #region IFilesQuery operations

        /// <summary>
        ///   This function exists solely to forbid in memory where clause on IFilesQuery, because
        ///   that is nearly always a mistake.
        /// </summary>
        [Obsolete(@"You cannot issue an in memory filter - such as Where(x=>x.Name == ""Test.file"") - on IFilesQuery. 
This is likely a bug, because this will execute the filter in memory, rather than in RavenDB.
Consider using session.Query<T>() instead of session.FilesQuery<T>. The session.Query<T>() method fully supports Linq queries, while session.FilesQuery<T>() is intended for lower level API access.
If you really want to do in memory filtering on the data returned from the query, you can use: session.FilesQuery<T>().ToList().Where(x=>x.Name == ""Test.file"")
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

        FilesConvention IAsyncFilesQueryBase<T, IAsyncFilesQuery<T>>.Conventions
        {
            get { return this.Conventions; }
        }


        bool IAsyncFilesQuery<T>.IsDistinct
        {
            get { return this.isDistinct; }
        }

        #endregion 

        public IAsyncFilesQuery<T> OnDirectory(string path, bool recursive = false)
        {
            var normalizedPath = path;
            if (!path.StartsWith("/"))
                normalizedPath = "/" + path.TrimEnd('/');

            if (recursive)
            {
                this.WhereEquals("directory", normalizedPath);
                this.AndAlso();
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
                this.AndAlso();
            }

            return this;
        }
    }
}
