using System.Collections.Generic;

namespace Raven.Client.Extensions
{
    internal static class ListExtensions
    {
        public static bool IsNullOrEmpty<T>(this List<T> list)
        {
            return list == null || list.Count == 0;
        }
    }
}
