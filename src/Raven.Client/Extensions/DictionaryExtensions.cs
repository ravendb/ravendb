using System.Collections.Generic;

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

        public static bool ContentEquals<TKey, TValue>(IDictionary<TKey, TValue> x, IDictionary<TKey, TValue> y)
        {
            if (x == null || y == null)
                return x == null && y == null;

            if (x.Count != y.Count)
                return false;

            foreach (var v in x)
            {
                TValue value;
                if (y.TryGetValue(v.Key, out value) == false)
                    return false;

                if (Equals(value, v.Value) == false)
                    return false;
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
