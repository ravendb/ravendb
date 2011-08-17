using System;
#if !NET_3_5
using System.Collections.Concurrent;
#endif
using Raven.Client.Connection.Profiling;

namespace Raven.Client.Util
{
	public class SimpleCache : IDisposable
	{
		readonly ConcurrentLruLSet<string> lruKeys;
		readonly ConcurrentDictionary<string, object> actualCache;

		public SimpleCache(int maxNumberOfCacheEntries)
		{
			actualCache = new ConcurrentDictionary<string, object>();
			lruKeys = new ConcurrentLruLSet<string>(maxNumberOfCacheEntries, key =>
			{
				object _;
				actualCache.TryRemove(key, out _);
			});
		}

		public void Set(string key, object val)
		{
			actualCache.AddOrUpdate(key, val, (s, o) => val);
			lruKeys.Push(key);
		}

		public object Get(string key)
		{
			object value;
			if(actualCache.TryGetValue(key, out value))
				lruKeys.Push(key);
			return value;
		}

		public void Dispose()
		{
			lruKeys.Clear();
			actualCache.Clear();
		}
	}
}