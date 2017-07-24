using System.Collections.Generic;

namespace Raven.Client.Documents.Queries
{
    internal static class QueryHashHelper
    {
        internal static int HashCode<TValue>(IEnumerable<TValue> x)
            where TValue : class
        {
            if (x == null)
                return 0;

            var result = 0;
            foreach (var value in x)
                result = (result * 397) ^ (value?.GetHashCode() ?? 0);

            return result;
        }

        internal static int HashCode<TKey, TValue>(Dictionary<TKey, TValue> x)
        {
            if (x == null)
                return 0;

            int result = 0;
            foreach (var kvp in x)
            {
                result = (result * 397) ^ kvp.Key.GetHashCode();
                result = (result * 397) ^ (kvp.Value?.GetHashCode() ?? 0);
            }
            return result;
        }
    }
}