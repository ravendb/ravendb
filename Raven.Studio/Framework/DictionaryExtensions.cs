namespace Raven.Studio.Framework
{
	using System.Collections.Generic;
	using Newtonsoft.Json.Linq;

	public static class DictionaryExtensions
	{
		public static T IfPresent<T>(this IDictionary<string, JToken> dictionary, string key)
		{
			if(dictionary == null) return default(T);
			return dictionary.ContainsKey(key) ? dictionary[key].Value<T>() : default(T);
		}
	}
}