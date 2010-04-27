using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Threading;

namespace Raven.Database.Cache
{
	public class CacheBuildingBlock<T>
		where T : class
	{
		protected static readonly ConcurrentDictionary<string, ValueAndTimestamp> Cache = new ConcurrentDictionary<string, ValueAndTimestamp>();
		protected const int NumberOfEntries = 1024 * 64;
		protected static int currentTimestamp;

		protected class ValueAndTimestamp
		{
			public string Key;
			public WeakReference Value;
			public int Timestamp;
		}

		protected static void Remember(string key, T value)
		{
			var current = Thread.VolatileRead(ref currentTimestamp);
			Cache.AddOrUpdate(key, new ValueAndTimestamp
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

		protected static T Parse(string key, Func<T> createObject)
		{
			var current = Interlocked.Increment(ref currentTimestamp);
			var shouldCheckCleanup = false;
			T createdObj = null;
			var valueAndTimestamp = Cache.GetOrAdd(key, guid =>
			{
				shouldCheckCleanup = true;
				createdObj = createObject();
				return new ValueAndTimestamp
				{
					Key = key,
					Value = new WeakReference(createdObj),
					Timestamp = current
				};
			});
			createdObj = GetCreatedObj(createdObj, createObject, valueAndTimestamp);
			valueAndTimestamp.Timestamp = current;
			if (shouldCheckCleanup && Cache.Count > NumberOfEntries)
			{
				var valueToRemove = Cache
					.Values
					.Take(NumberOfEntries / 10)
					.OrderBy(x => x.Timestamp)
					.ToArray();

				foreach (var toRemove in valueToRemove)
				{
					ValueAndTimestamp ignored;
					Cache.TryRemove(toRemove.Key, out ignored);
				}
			}
			return createdObj;
		}

		private static T GetCreatedObj(T createdObj, Func<T> createObject, ValueAndTimestamp valueAndTimestamp)
		{
			if (createdObj != null)
				return createdObj;
			var target = valueAndTimestamp.Value.Target;
			if (target == null)
			{
				createdObj = createObject();
				valueAndTimestamp.Value = new WeakReference(createdObj);
			}
			else
			{
				createdObj = (T)target;
			}
			return createdObj;
		}
	}
}