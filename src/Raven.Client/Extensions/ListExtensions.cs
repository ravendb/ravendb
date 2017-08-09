using System.Collections.Generic;

namespace Raven.Client.Extensions
{
    internal static class ListExtensions
    {
        public static HashSet<T> ToHashSet<T>(this IEnumerable<T> items)
        {
            return new HashSet<T>(items);
        }
    }
}
