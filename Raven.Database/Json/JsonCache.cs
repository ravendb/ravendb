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
		private static int currentTimestamp;

		class ValueAndTimestamp
		{
			public string Key;
			public WeakReference Value;
			public int Timestamp;
		}

		public static void RememberDocument(Guid etag, JObject value)
		{
			Remember("docs/" + etag, value);
		}

		public static void RememberMetadata(Guid etag, JObject value)
		{
			Remember("metadata/" + etag, value);
		}

		private static void Remember(string key, JObject value)
		{
			var current = Thread.VolatileRead(ref currentTimestamp);
			cache.AddOrUpdate(key, new ValueAndTimestamp
			{
				Key = key,
				Timestamp = current,
				Value = new WeakReference(value)
			}, (s, timestamp) =>
			{
				timestamp.Timestamp = current;
				timestamp.Value = new WeakReference(value);
				return timestamp;
			});
		}

		public static JObject Parse(string key, byte[] data)
		{
			var current = Interlocked.Increment(ref currentTimestamp);
			var shouldCheckCleanup = false;
			JObject createdObj = null;
			var valueAndTimestamp = cache.GetOrAdd(key, guid =>
			{
				shouldCheckCleanup = true;
				createdObj = data.ToJObject();
				return new ValueAndTimestamp
				{
					Key = key,
					Value = new WeakReference(createdObj),
					Timestamp = current
				};
			});
			createdObj = GetCreatedObj(createdObj, data, valueAndTimestamp);
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
			return createdObj;
		}

		private static JObject GetCreatedObj(JObject createdObj, byte[] data, ValueAndTimestamp valueAndTimestamp)
		{
			if(createdObj != null)
				return createdObj;
			var target = valueAndTimestamp.Value.Target;
			if(target == null)
			{
				createdObj = data.ToJObject();
				valueAndTimestamp.Value= new WeakReference(createdObj);
			}
			else
			{
				createdObj = (JObject) target;
			}
			return createdObj;
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