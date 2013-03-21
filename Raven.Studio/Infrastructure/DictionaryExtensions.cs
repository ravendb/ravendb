using Raven.Json.Linq;

namespace Raven.Studio.Framework
{
	using System.Collections.Generic;

	public static class DictionaryExtensions
	{
		public static T IfPresent<T>(this RavenJObject dictionary, string key)
		{
			if(dictionary == null) return default(T);
			return dictionary.ContainsKey(key) ? dictionary[key].Value<T>() : default(T);
		}

		public static T IfPresent<T>(this IDictionary<string, RavenJToken> dictionary, string key)
		{
			if (dictionary == null) return default(T);
			return dictionary.ContainsKey(key) ? dictionary[key].Value<T>() : default(T);
		}
	}
}