using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;

namespace Raven.Client.Documents.Linq
{
    public static class RavenQueryableExtensions
    {
        /// <summary>
        /// Filters a sequence of values based on a predicate.
        /// </summary>
        public static IRavenQueryable<T> Where<T>(this IRavenQueryable<T> source, Expression<Func<T, bool>> predicate)
        {
            return (IRavenQueryable<T>)Queryable.Where(source, predicate);
        }

        /// <summary>
        /// Filters a sequence of values based on a predicate.
        /// </summary>
        public static IRavenQueryable<T> Where<T>(this IRavenQueryable<T> source, Expression<Func<T, int, bool>> predicate)
        {
            return (IRavenQueryable<T>)Queryable.Where(source, predicate);
        }

        /// <summary>
        /// Sorts the elements of a sequence in ascending order according to a key.
        /// </summary>
        public static IRavenQueryable<T> OrderBy<T, TK>(this IRavenQueryable<T> source, Expression<Func<T, TK>> keySelector)
        {
            return (IRavenQueryable<T>)Queryable.OrderBy(source, keySelector);
        }

        /// <summary>
        /// Sorts the elements of a sequence in ascending order according to a key.
        /// </summary>
        public static IRavenQueryable<T> OrderBy<T, TK>(this IRavenQueryable<T> source, Expression<Func<T, TK>> keySelector, IComparer<TK> comparer)
        {
            return (IRavenQueryable<T>)Queryable.OrderBy(source, keySelector, comparer);
        }

        /// <summary>
        /// Sorts the elements of a sequence in descending order according to a key.
        /// </summary>
        public static IRavenQueryable<T> OrderByDescending<T, TK>(this IRavenQueryable<T> source, Expression<Func<T, TK>> keySelector)
        {
            return (IRavenQueryable<T>)Queryable.OrderByDescending(source, keySelector);
        }

        /// <summary>
        /// Sorts the elements of a sequence in descending order according to a key.
        /// </summary>
        public static IRavenQueryable<T> OrderByDescending<T, TK>(this IRavenQueryable<T> source, Expression<Func<T, TK>> keySelector, IComparer<TK> comparer)
        {
            return (IRavenQueryable<T>)Queryable.OrderByDescending(source, keySelector, comparer);
        }

        /// <summary>
        /// Sorts(secondary) the elements of a sequence in ascending order according to a key.
        /// </summary>
        public static IRavenQueryable<T> ThenBy<T, TK>(this IRavenQueryable<T> source, Expression<Func<T, TK>> keySelector)
        {
            return (IRavenQueryable<T>)Queryable.ThenBy(source, keySelector);
        }


        /// <summary>
        /// Sorts(secondary) the elements of a sequence in descending order according to a key.
        /// </summary>
        public static IRavenQueryable<T> ThenByDescending<T, TK>(this IRavenQueryable<T> source, Expression<Func<T, TK>> keySelector)
        {
            return (IRavenQueryable<T>)Queryable.ThenByDescending(source, keySelector);
        }


        /// <summary>
        /// Projects each element of a sequence into a new form.
        /// </summary>
        public static IRavenQueryable<TResult> Select<TSource, TResult>(this IRavenQueryable<TSource> source, Expression<Func<TSource, TResult>> selector)
        {
            return (IRavenQueryable<TResult>)Queryable.Select(source, selector);
        }

        /// <summary>
        /// Projects each element of a sequence into a new form.
        /// </summary>
        public static IRavenQueryable<TResult> Select<TSource, TResult>(this IRavenQueryable<TSource> source, Expression<Func<TSource, int, TResult>> selector)
        {
            return (IRavenQueryable<TResult>)Queryable.Select(source, selector);
        }

        /// <summary>
        /// Implementation of In operator
        /// </summary>
        public static bool In<T>(this T field, IEnumerable<T> values)
        {
            return values.Any(value => field.Equals(value));
        }

        /// <summary>
        /// Implementation of In operator
        /// </summary>
        public static bool In<T>(this T field, params T[] values)
        {
            return values.Any(value => field.Equals(value));
        }

        /// <summary>
        /// Bypasses a specified number of elements in a sequence and then returns the remaining elements.
        /// </summary>
        /// Summary:
        public static IRavenQueryable<TSource> Skip<TSource>(this IRavenQueryable<TSource> source, int count)
        {
            return (IRavenQueryable<TSource>)Queryable.Skip(source, count);
        }
        public static IRavenQueryable<TSource> Take<TSource>(this IRavenQueryable<TSource> source, int count)
        {
            return (IRavenQueryable<TSource>)Queryable.Take(source, count);
        }

        /// <summary>
        /// Implementation of the Contains ANY operator
        /// </summary>
        public static bool ContainsAny<T>(this IEnumerable<T> list, IEnumerable<T> items)
        {
            throw new InvalidOperationException(
                "This method isn't meant to be called directly, it just exists as a place holder, for the LINQ provider");
        }

        /// <summary>
        /// Implementation of the Contains ALL operator
        /// </summary>
        public static bool ContainsAll<T>(this IEnumerable<T> list, IEnumerable<T> items)
        {
            throw new InvalidOperationException(
                "This method isn't meant to be called directly, it just exists as a place holder for the LINQ provider");
        }
    }
}
