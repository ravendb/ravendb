//-----------------------------------------------------------------------
// <copyright file="LinqOnDynamic.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;

namespace Raven.Server.Documents.Indexes.Static.Extensions
{
    public static class LinqOnDynamic
    {
        [Obsolete("This method should never be used directly.")]
        public static IEnumerable<IGrouping<dynamic, dynamic>> GroupBy(this IEnumerable<dynamic> source, Func<dynamic, dynamic> keySelector)
        {
            return Enumerable.Select(Enumerable.GroupBy(source, keySelector), inner => new DynamicArray.DynamicGrouping(inner));
        }

        [Obsolete("This method should never be used directly.")]
        public static IEnumerable<IGrouping<dynamic, dynamic>> GroupBy(this IEnumerable<dynamic> source, Func<dynamic, dynamic> keySelector, Func<dynamic, dynamic> resultSelector)
        {
            return Enumerable.Select(Enumerable.GroupBy(source, keySelector, resultSelector), inner => new DynamicArray.DynamicGrouping(inner));
        }

        [Obsolete("This method should never be used directly.")]
        public static IEnumerable<dynamic> Select(this IEnumerable<dynamic> source, Func<object, object> func)
        {
            return new DynamicArray(source.Select<object, object>(func));
        }

        [Obsolete("This method should never be used directly.")]
        public static IEnumerable<dynamic> Select(this IEnumerable<dynamic> source, Func<IGrouping<object, object>, object> func)
        {
            return new DynamicArray(Enumerable.Select(source, o => func((IGrouping<object, object>)o)));
        }

        [Obsolete("This method should never be used directly.")]
        public static IEnumerable<dynamic> Select(this IEnumerable<dynamic> source, Func<object, int, object> func)
        {
            return new DynamicArray(source.Select<object, object>(func));
        }

        [Obsolete("This method should never be used directly.")]
        public static IEnumerable<dynamic> SelectMany(this object source,
                                                Func<dynamic, int, IEnumerable<dynamic>> collectionSelector,
                                                Func<dynamic, dynamic, dynamic> resultSelector)
        {
            return new DynamicArray(Enumerable.SelectMany(Select(source), collectionSelector, resultSelector));
        }

        [Obsolete("This method should never be used directly.")]
        public static IEnumerable<dynamic> SelectMany(this object source,
                                                      Func<dynamic, IEnumerable<dynamic>> collectionSelector,
                                                      Func<dynamic, dynamic, dynamic> resultSelector)
        {
            return new DynamicArray(Enumerable.SelectMany(Select(source), collectionSelector, resultSelector));
        }

        [Obsolete("This method should never be used directly.")]
        public static IEnumerable<dynamic> SelectMany(this object source,
                                                      Func<dynamic, IEnumerable<dynamic>> selector)
        {
            return new DynamicArray(Select(source).SelectMany<object, object>(selector));
        }

        [Obsolete("This method should never be used directly.")]
        public static IEnumerable<dynamic> SelectMany(this IGrouping<dynamic, dynamic> source,
                                                      Func<dynamic, int, IEnumerable<dynamic>> collectionSelector,
                                                      Func<dynamic, dynamic, dynamic> resultSelector)
        {
            return new DynamicArray(Enumerable.SelectMany(Select(source), collectionSelector, resultSelector));
        }

        [Obsolete("This method should never be used directly.")]
        public static IEnumerable<dynamic> SelectMany(this IGrouping<dynamic, dynamic> source,
                                                      Func<dynamic, IEnumerable<dynamic>> collectionSelector,
                                                      Func<dynamic, dynamic, dynamic> resultSelector)
        {
            return new DynamicArray(Enumerable.SelectMany(Select(source), collectionSelector, resultSelector));
        }

        [Obsolete("This method should never be used directly.")]
        public static IEnumerable<dynamic> SelectMany(this IGrouping<dynamic, dynamic> source,
                                                      Func<dynamic, IEnumerable<dynamic>> selector)
        {
            return new DynamicArray(Select(source).SelectMany<object, object>(selector));
        }

        [Obsolete("This method should never be used directly.")]
        public static IEnumerable<dynamic> DefaultIfEmpty(this IEnumerable<dynamic> self)
        {
            return self.DefaultIfEmpty<dynamic>(DynamicNullObject.Null);
        }

        [Obsolete("This method should never be used directly.")]
        public static IDictionary<dynamic, dynamic> ToDictionary<T>(this IEnumerable<T> source, Func<T, dynamic> keySelector, Func<T, dynamic> elementSelector)
        {
            var type = typeof(T);
            if (type == typeof(object))
            {
                return new DynamicDictionary(Enumerable.ToDictionary(source, keySelector, elementSelector));
            }
            if (type == typeof(IGrouping<dynamic, dynamic>))
            {
                return new DynamicDictionary(Enumerable.ToDictionary(source, o => keySelector(o), u => elementSelector(u)));
            }

            throw new NotSupportedException($"ToDictionary method is not supported on IEnumerable type {type}.");
        }

        [Obsolete("This method should never be used directly.")]
        public static IDictionary<dynamic, dynamic> ToDictionary<T>(this IEnumerable<T> source, Func<T, dynamic> keySelector, Func<T, dynamic> elementSelector, IEqualityComparer<dynamic> comparer)
        {
            var type = typeof(T);
            if (type == typeof(object))
            {
                return new DynamicDictionary(Enumerable.ToDictionary(source, keySelector, elementSelector, comparer));
            }
            if (type == typeof(IGrouping<dynamic, dynamic>))
            {
                return new DynamicDictionary(Enumerable.ToDictionary(source, o => keySelector(o), u => elementSelector(u), comparer));
            }

            throw new NotSupportedException($"ToDictionary method is not supported on IEnumerable type {type}.");
        }

        private static IEnumerable<dynamic> Select(this object self)
        {
            if (self == null || self is DynamicNullObject)
                yield break;
            if (self is IEnumerable == false || self is string)
                throw new InvalidOperationException("Attempted to enumerate over " + self.GetType().Name);

            foreach (var item in (IEnumerable)self)
            {
                yield return item;
            }
        }
    }
}
