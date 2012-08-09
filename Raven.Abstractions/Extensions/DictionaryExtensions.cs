using System.Collections.Generic;

namespace Raven.Abstractions.Extensions
{
	public static class DictionaryExtensions
	{
		public static TVal GetOrAdd<TKey, TVal>(this IDictionary<TKey, TVal> self, TKey key) where TVal : new()
		{
			TVal value;
			if (self.TryGetValue(key, out value))
				return value;

			value = new TVal();
			self.Add(key, value);
			return value;
		}

		/// <summary>
		/// Returns dictionary[key] or default<V>.
		/// </summary>
		public static V TryGetValue<K, V>(this IDictionary<K, V> dictionary, K key) {
			V result;
			dictionary.TryGetValue(key, out result);
			return result;
		}
	}
}
