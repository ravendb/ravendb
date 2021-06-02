using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;

namespace Raven.Client.Extensions
{
    internal static class EnumerableExtension
    {
        public static bool ApplyIfNotNull<T>(this IEnumerable<T> self, Action<T> action)
        {
            if (self == null)
                return false;
            var hasItems = false;
            foreach (var item in self)
            {
                hasItems = true;
                action(item);
            }
            return hasItems;
        }

        public static bool ContentEquals<TValue>(IEnumerable<TValue> x, IEnumerable<TValue> y)
        {
            if (x == null || y == null)
                return x == null && y == null;

            return x.SequenceEqual(y);
        }

        public static IEnumerable<T> EmptyIfNull<T>(this IEnumerable<T> self)
        {
            if (self == null)
                return new T[0];
            return self;
        }

        public static int GetEnumerableHashCode<TKey>(this IEnumerable<TKey> self)
        {
            int result = 0;

            foreach (var item in self)
            {
                result = (result * 397) ^ (Equals(item, default(TKey)) == false ? item.GetHashCode() : 0);
            }

            return result;
        }

        /// <summary>
        /// This is used to prevent race condition errors when using linq extension methods on ConcurrentDictionary
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <typeparam name="S"></typeparam>
        /// <param name="collection"></param>
        /// <returns></returns>
        public static IEnumerable<KeyValuePair<T, S>> ForceEnumerateInThreadSafeManner<T, S>(this ConcurrentDictionary<T, S> collection)
        {
            // thanks to: https://stackoverflow.com/questions/47630824/is-c-sharp-linq-orderby-threadsafe-when-used-with-concurrentdictionarytkey-tva#
            foreach (var item in collection)
                yield return item;
        }
    }
}
