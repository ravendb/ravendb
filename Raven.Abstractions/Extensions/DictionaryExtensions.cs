using System.Collections.Generic;

namespace Raven.Abstractions.Extensions
{
	public static class DictionaryExtensions
	{
		public static TVal GetOrAdd<TKey, TVal>(this Dictionary<TKey, TVal> self, TKey key) where TVal : new()
		{
			TVal value;
			if (self.TryGetValue(key, out value))
				return value;

			value = new TVal();
			self.Add(key, value);
			return value;
		}
	}
}