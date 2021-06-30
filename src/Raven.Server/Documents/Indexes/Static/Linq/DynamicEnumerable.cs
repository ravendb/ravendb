using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;

namespace Raven.Server.Documents.Indexes.Static.Linq
{
    public static class DynamicEnumerable
    {
        public static IEnumerable<dynamic> Union(object source, object other)
        {
            return new DynamicArray(((IEnumerable<object>)source).Union((IEnumerable<object>)other));
        }

        public static dynamic First(IEnumerable source)
        {
            return FirstOrDefault(source);
        }

        public static dynamic First(IEnumerable source, Func<dynamic, bool> predicate)
        {
            return FirstOrDefault(source, predicate);
        }

        public static dynamic FirstOrDefault(IEnumerable source)
        {
            if (source == null) return DynamicNullObject.Null;

            var result = source.Cast<object>().FirstOrDefault();
            if (ReferenceEquals(result, null))
                return DynamicNullObject.Null;
            return result;
        }

        public static dynamic FirstOrDefault(IEnumerable source, Func<dynamic, bool> predicate)
        {
            if (source == null) return DynamicNullObject.Null;
            if (predicate == null) return DynamicNullObject.Null;

            var result = source.Cast<dynamic>().FirstOrDefault(predicate);
            if (ReferenceEquals(result, null))
                return DynamicNullObject.Null;
            return result;
        }

        public static dynamic Last(IEnumerable source)
        {
            return LastOrDefault(source);

        }

        public static dynamic Last(IEnumerable source, Func<dynamic, bool> predicate)
        {
            return LastOrDefault(source, predicate);
        }

        public static dynamic LastOrDefault(IEnumerable source)
        {
            if (source == null) return DynamicNullObject.Null;

            var result = source.Cast<object>().LastOrDefault();
            if (ReferenceEquals(result, null))
                return DynamicNullObject.Null;
            return result;
        }

        public static dynamic LastOrDefault(IEnumerable source, Func<dynamic, bool> predicate)
        {
            if (source == null) return DynamicNullObject.Null;
            if (predicate == null) return DynamicNullObject.Null;

            var result = source.Cast<dynamic>().LastOrDefault(predicate);
            if (ReferenceEquals(result, null))
                return DynamicNullObject.Null;
            return result;
        }

        public static dynamic Single(IEnumerable source)
        {
            return SingleOrDefault(source);
        }

        public static dynamic Single(IEnumerable source, Func<dynamic, bool> predicate)
        {
            return SingleOrDefault(source, predicate);
        }

        public static dynamic SingleOrDefault(IEnumerable source)
        {
            if (source == null) return DynamicNullObject.Null;

            var result = source.Cast<object>().SingleOrDefault();
            if (ReferenceEquals(result, null))
                return DynamicNullObject.Null;
            return result;
        }

        public static dynamic SingleOrDefault(IEnumerable source, Func<dynamic, bool> predicate)
        {
            if (source == null) return DynamicNullObject.Null;
            if (predicate == null) return DynamicNullObject.Null;

            var result = source.Cast<dynamic>().SingleOrDefault(predicate);
            if (ReferenceEquals(result, null))
                return DynamicNullObject.Null;
            return result;
        }

        public static dynamic ElementAt(IEnumerable source, int index)
        {
            return ElementAtOrDefault(source, index);

        }

        public static dynamic ElementAtOrDefault(IEnumerable source, int index)
        {
            if (source == null) return DynamicNullObject.Null;
            if (index < 0) return DynamicNullObject.Null;

            var result = source.Cast<dynamic>().ElementAtOrDefault(index);
            if (ReferenceEquals(result, null))
                return DynamicNullObject.Null;
            return result;
        }


        private static IEnumerable<dynamic> Yield(IEnumerator enumerator)
        {
            do
            {
                yield return enumerator.Current;
            } while (enumerator.MoveNext());
        }

        public static dynamic Min(IEnumerable source)
        {
            if (source == null) return DynamicNullObject.Null;

            var enumerator = source.GetEnumerator();
            if (enumerator.MoveNext() == false)
                return DynamicNullObject.Null;

            var result = Yield(enumerator).Min();
            if (ReferenceEquals(result, null))
                return DynamicNullObject.Null;
            return result;
        }

        public static dynamic Min(IEnumerable source, Func<dynamic, dynamic> selector)
        {
            return Min(Enumerable.Select(source.Cast<dynamic>(), selector));
        }

        public static dynamic Max(IEnumerable source)
        {
            if (source == null) return DynamicNullObject.Null;

            var enumerator = source.GetEnumerator();
            if (enumerator.MoveNext() == false)
                return DynamicNullObject.Null;

            var result = Yield(enumerator).Max();
            if (ReferenceEquals(result, null))
                return DynamicNullObject.Null;
            return result;
        }

        public static dynamic Max(IEnumerable source, Func<dynamic, dynamic> selector)
        {
            return Max(Enumerable.Select(source.Cast<dynamic>(), selector));
        }

        public static IEnumerable<dynamic> Concat(object source, object other)
        {
            return new DynamicArray(((IEnumerable<object>)source).Concat((IEnumerable<object>)other));
        }

        public static IEnumerable<dynamic> Intersect(object source, object other)
        {
            return new DynamicArray(((IEnumerable<object>)source).Intersect((IEnumerable<object>)other));
        }


        public static IOrderedEnumerable<dynamic> OrderBy(IEnumerable source, Func<dynamic, dynamic> keySelector)
        {
            return new DynamicArray(Enumerable.OrderBy(source.Cast<dynamic>(), keySelector));
        }

        public static IOrderedEnumerable<dynamic> OrderBy<TSource, TKey>(IEnumerable<TSource> source, Func<TSource, TKey> keySelector, IComparer<TKey> comparer)
        {
            return new DynamicArray(Enumerable.OrderBy(source, keySelector, comparer));
        }
        
        public static IOrderedEnumerable<dynamic> OrderByDescending(IEnumerable source, Func<dynamic, dynamic> keySelector)
        {
            return new DynamicArray(Enumerable.OrderByDescending(source.Cast<dynamic>(), keySelector));
        }

        public static IOrderedEnumerable<dynamic> OrderByDescending<TSource, TKey>(IEnumerable<TSource> source, Func<TSource, TKey> keySelector, IComparer<TKey> comparer)
        {
            return new DynamicArray(Enumerable.OrderByDescending(source, keySelector, comparer));
        }
    }
}
