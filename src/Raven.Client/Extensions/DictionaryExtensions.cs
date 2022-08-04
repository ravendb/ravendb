using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography.X509Certificates;

namespace Raven.Client.Extensions
{
    internal static class DictionaryExtensions
    {
        public static TVal GetOrDefault<TKey, TVal>(this IDictionary<TKey, TVal> self, TKey key)
        {
            TVal value;
            self.TryGetValue(key, out value);
            return value;
        }

        public static bool ContentEquals<TKey, TValue>(IDictionary<TKey, TValue> x, IDictionary<TKey, TValue> y, bool compareNullValues = true)
        {
            if (x == null || y == null)
                return x == null && y == null;

            var xNullsCount = 0;
            var yNullsCount = 0;
            KeyValuePair<TKey, TValue>[] yNulls = y.Where(kvp => kvp.Value == null).ToArray();
            if (compareNullValues == false)
            {
                xNullsCount = x.Where(kvp => kvp.Value == null).ToArray().Length;
                yNullsCount = yNulls.Length;
            }

            if (x.Count - xNullsCount != y.Count - yNullsCount)
                return false;

            foreach (var v in x)
            {
                if ( compareNullValues==false && v.Value == null )
                    continue;
                TValue value;
                if (y.TryGetValue(v.Key, out value) == false)
                    return false;

                if (Equals(value, v.Value) == false)
                    return false;
            }

            if (compareNullValues)
            {
                foreach (var v in yNulls)
                {
                    TValue value;
                    if (x.TryGetValue(v.Key, out value) == false)
                        return false;

                    if (Equals(value, null) == false)
                        return false;
                }
            }

            return true;
        }

        public static int GetDictionaryHashCode<TKey, TValue>(this IDictionary<TKey, TValue> self)
        {
            int result = 0;
            foreach (var kvp in self)
            {
                result = (result * 397) ^ kvp.Key.GetHashCode();
                result = (result * 397) ^ (Equals(kvp.Value, default(TValue)) == false ? kvp.Value.GetHashCode() : 0);
            }
            return result;
        }
    }
}
