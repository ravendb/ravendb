using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;

namespace Raven.Server.Documents.Indexes.Static.Linq
{
    public static partial class DynamicEnumerable
    {
        public static IOrderedEnumerable<dynamic> OrderBy<TSource, TKey>(IEnumerable<TSource> source, Func<TSource, TKey> keySelector, IComparer<TKey> comparer)
        {
            return new DynamicArray(Enumerable.OrderBy(source, keySelector, comparer));
        }

        public static IOrderedEnumerable<dynamic> OrderBy(IEnumerable source, Func<dynamic, dynamic> keySelector)
        {
            return new DynamicArray(Enumerable.OrderBy(source.Cast<dynamic>(), keySelector));
        }

        public static IOrderedEnumerable<dynamic> OrderBy(IEnumerable source, Func<dynamic, dynamic> keySelector, object comparer)
        {
            return (IOrderedEnumerable<dynamic>)AsDynamicArray(source).OrderBy(keySelector, (IComparer)comparer);
        }

        public static IOrderedEnumerable<TSource> OrderByWithDefaultComparer<TSource, TKey>(IEnumerable<TSource> source, Func<TSource, TKey> keySelector, IComparer<TKey> comparer = null)
        {
            return Enumerable.OrderBy(source, keySelector, GetOrderingComparer(comparer));
        }

        public static IOrderedEnumerable<dynamic> OrderByDescending<TSource, TKey>(IEnumerable<TSource> source, Func<TSource, TKey> keySelector, IComparer<TKey> comparer)
        {
            return new DynamicArray(Enumerable.OrderByDescending(source, keySelector, comparer));
        }

        public static IOrderedEnumerable<dynamic> OrderByDescending(IEnumerable source, Func<dynamic, dynamic> keySelector)
        {
            return new DynamicArray(Enumerable.OrderByDescending(source.Cast<dynamic>(), keySelector));
        }

        public static IOrderedEnumerable<dynamic> OrderByDescending(IEnumerable source, Func<dynamic, dynamic> keySelector, object comparer)
        {
            return (IOrderedEnumerable<dynamic>)AsDynamicArray(source).OrderByDescending(keySelector, (IComparer)comparer);
        }

        public static IOrderedEnumerable<TSource> OrderByDescendingWithDefaultComparer<TSource, TKey>(IEnumerable<TSource> source, Func<TSource, TKey> keySelector, IComparer<TKey> comparer = null)
        {
            return Enumerable.OrderByDescending(source, keySelector, GetOrderingComparer(comparer));
        }

        public static IOrderedEnumerable<TSource> ThenByWithDefaultComparer<TSource, TKey>(IOrderedEnumerable<TSource> source, Func<TSource, TKey> keySelector, IComparer<TKey> comparer = null)
        {
            return Enumerable.ThenBy(source, keySelector, GetOrderingComparer(comparer));
        }

        public static IOrderedEnumerable<TSource> ThenByDescendingWithDefaultComparer<TSource, TKey>(IOrderedEnumerable<TSource> source, Func<TSource, TKey> keySelector, IComparer<TKey> comparer = null)
        {
            return Enumerable.ThenByDescending(source, keySelector, GetOrderingComparer(comparer));
        }

        private static IComparer<TKey> GetOrderingComparer<TKey>(IComparer<TKey> comparer)
        {
            // Match released object-key string ordering without allocating a culture comparer per sort.
            // Keep other key types and explicit comparers unchanged.
            if (comparer == null && typeof(TKey) == typeof(string))
                return (IComparer<TKey>)(object)Comparer<object>.Default;

            return comparer;
        }

        private static DynamicArray AsDynamicArray(object source)
        {
            if (source is DynamicArray dynamicArray)
                return dynamicArray;

            return new DynamicArray(AsEnumerable(source));
        }
    }
}
