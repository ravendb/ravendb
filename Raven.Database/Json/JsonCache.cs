using System;
using System.Collections.Concurrent;
using System.Threading;
using Newtonsoft.Json.Linq;
using System.Linq;

namespace Raven.Database.Json
{
	public class JsonCache
	{
		static readonly ConcurrentDictionary<string, ValueAndTimestamp> cache = new ConcurrentDictionary<string, ValueAndTimestamp>();
		private const int NumberOfEntries = 1024*64;
		private static int currentTimestamp = 0;

		class ValueAndTimestamp
		{
			public string Key;
			public JObject Value;
			public int Timestamp;
		}



		public static JObject Parse(string key, byte[] data)
		{
			var current = Interlocked.Increment(ref currentTimestamp);
			var shouldCheckCleanup = false;
			var valueAndTimestamp = cache.GetOrAdd(key, guid =>
			{
				shouldCheckCleanup = true;
				return new ValueAndTimestamp
				{
					Key = key,
					Value = data.ToJObject(),
					Timestamp = current
				};
			});
			valueAndTimestamp.Timestamp = current;
			if(shouldCheckCleanup && cache.Count > NumberOfEntries)
			{
				var valueToRemove = cache
					.Values
					.Take(NumberOfEntries / 10)
					.OrderBy(x=>x.Timestamp)
					.ToArray();

				foreach (var toRemove in valueToRemove)
				{
					ValueAndTimestamp ignored;
					cache.TryRemove(toRemove.Key, out ignored);
				}
			}
			return valueAndTimestamp.Value;
		}

		public static JObject ParseDocument(Guid etag, byte[] data)
		{
			return new JObject(Parse("docs/" + etag, data)); // force deep cloning
		}

		public static JObject ParseMetadata(Guid etag, byte[] data)
		{
			return new JObject(Parse("metadata/" + etag, data)); // force deep cloning
		}
	}
}