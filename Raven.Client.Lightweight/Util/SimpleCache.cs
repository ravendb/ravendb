using System;
#if !NET35
using System.Collections.Concurrent;
#endif
using Raven.Client.Connection.Profiling;

namespace Raven.Client.Util
{
	public class SimpleCache<T> : IDisposable
	{
		readonly ConcurrentLruLSet<string> lruKeys;
		readonly ConcurrentDictionary<string, T> actualCache;

		public SimpleCache(int maxNumberOfCacheEntries)
		{
			actualCache = new ConcurrentDictionary<string, T>();
			lruKeys = new ConcurrentLruLSet<string>(maxNumberOfCacheEntries, key =>
			{
				T _;
				actualCache.TryRemove(key, out _);
			});
		}

		public void Set(string key, T val)
		{
			actualCache.AddOrUpdate(key, val, (s, o) => val);
			lruKeys.Push(key);
		}

		public T Get(string key)
		{
			T value;
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