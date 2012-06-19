#if !NET35
using System;
using System.Collections.Concurrent;
using System.Threading;

namespace Raven.Abstractions.Extensions
{
	public static class ConcurrentDictionaryExtensions
	{
		public static TVal GetOrDefault<TKey,TVal>(this ConcurrentDictionary<TKey, TVal> self, TKey key, TVal value = default(TVal))
		{
			TVal fromDic;
			if (self.TryGetValue(key, out fromDic))
				return fromDic;
			return value;
		}


		public static TVal GetOrAddAtomically<TKey,TVal>(this ConcurrentDictionary<TKey, TVal> self, TKey key, Func<TKey, TVal> valueFactory)
		{
			TVal val;
			if (self.TryGetValue(key, out val))
				return val;

			lock(self)
			{
				if (self.TryGetValue(key, out val))
					return val;

				var value = valueFactory(key);
				while (self.TryAdd(key, value) == false)
				{
					Thread.SpinWait(100);
				}
				return value;
			}
		}
	}
}
#endif