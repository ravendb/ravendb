using System;
using System.Collections.Generic;

namespace Raven.Abstractions.Data
{
    internal static class EnumerableExtension
    {
        public static void ApplyIfNotNull<T>(this IEnumerable<T> self, Action<T> action)
        {
            if (self == null)
                return;
            foreach (var item in self)
            {
                action(item);
            }
        }
    }
}