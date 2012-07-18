using System;
using System.Collections.Concurrent;
using System.Collections.Generic;

namespace Raven.Abstractions.Util
{
	public class AtomicDictionary<TKey, TVal>
	{
		private readonly ConcurrentDictionary<TKey, object> locks;
		private readonly ConcurrentDictionary<TKey, TVal> items;

		public AtomicDictionary()
		{
			items = new ConcurrentDictionary<TKey, TVal>();
			locks = new ConcurrentDictionary<TKey, object>();
		}

		public AtomicDictionary(IEqualityComparer<TKey> comparer)
		{
			items = new ConcurrentDictionary<TKey, TVal>(comparer);
			locks = new ConcurrentDictionary<TKey, object>(comparer);
	
		}

		public TVal GetOrAdd(TKey key, Func<TKey, TVal> valueGenerator)
		{
			TVal val;
			if (items.TryGetValue(key, out val))
				return val;
			lock (locks.GetOrAdd(key, new object()))
			{
				return items.GetOrAdd(key, valueGenerator);
			}
		}

		public void Remove(TKey key)
		{
			object value;
			if(locks.TryGetValue(key, out value) == false)
			{
				TVal val;
				items.TryRemove(key, out val); // just to be on the safe side
				return;
			}
			lock(value)
			{
				object o;
				locks.TryRemove(key, out o);
				TVal val;
				items.TryRemove(key, out val);
			}
		}
	}
}