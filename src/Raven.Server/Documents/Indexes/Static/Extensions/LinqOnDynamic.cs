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
        public static IEnumerable<IGrouping<dynamic, dynamic>> GroupBy(this IEnumerable<dynamic> source, Func<dynamic, dynamic> keySelector)
        {
            return Enumerable.GroupBy(source, keySelector).Select(inner => new DynamicArray.DynamicGrouping(inner));
        }

        public static IEnumerable<IGrouping<dynamic, dynamic>> GroupBy(this IEnumerable<dynamic> source, Func<dynamic, dynamic> keySelector, Func<dynamic, dynamic> resultSelector)
        {
            return Enumerable.GroupBy(source, keySelector, resultSelector).Select(inner => new DynamicArray.DynamicGrouping(inner));
        }

        public static IEnumerable<dynamic> SelectMany(this object source,
                                                Func<dynamic, int, IEnumerable<dynamic>> collectionSelector,
                                                Func<dynamic, dynamic, dynamic> resultSelector)
        {
            return new DynamicArray(Enumerable.SelectMany(Select(source), collectionSelector, resultSelector));
        }

        public static IEnumerable<dynamic> SelectMany(this object source,
                                                      Func<dynamic, IEnumerable<dynamic>> collectionSelector,
                                                      Func<dynamic, dynamic, dynamic> resultSelector)
        {
            return new DynamicArray(Enumerable.SelectMany(Select(source), collectionSelector, resultSelector));
        }

        public static IEnumerable<dynamic> SelectMany(this object source,
                                                      Func<dynamic, IEnumerable<dynamic>> selector)
        {
            return new DynamicArray(Select(source).SelectMany<object, object>(selector));
        }

        public static IEnumerable<dynamic> SelectMany(this IGrouping<dynamic, dynamic> source,
                                                      Func<dynamic, int, IEnumerable<dynamic>> collectionSelector,
                                                      Func<dynamic, dynamic, dynamic> resultSelector)
        {
            return new DynamicArray(Enumerable.SelectMany(Select(source), collectionSelector, resultSelector));
        }

        public static IEnumerable<dynamic> SelectMany(this IGrouping<dynamic, dynamic> source,
                                                      Func<dynamic, IEnumerable<dynamic>> collectionSelector,
                                                      Func<dynamic, dynamic, dynamic> resultSelector)
        {
            return new DynamicArray(Enumerable.SelectMany(Select(source), collectionSelector, resultSelector));
        }

        public static IEnumerable<dynamic> SelectMany(this IGrouping<dynamic, dynamic> source,
                                                      Func<dynamic, IEnumerable<dynamic>> selector)
        {
            return new DynamicArray(Select(source).SelectMany<object, object>(selector));
        }

        public static IEnumerable<dynamic> DefaultIfEmpty(this IEnumerable<dynamic> self)
        {
            return self.DefaultIfEmpty<dynamic>(DynamicNullObject.Null);
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
