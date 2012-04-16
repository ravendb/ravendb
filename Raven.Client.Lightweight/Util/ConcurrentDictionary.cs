#if NET35
using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;

namespace Raven.Client.Util
{
	public class ConcurrentDictionary<K,V> : IEnumerable<KeyValuePair<K,V>>
	{
		readonly Dictionary<K,V> items;

		public ConcurrentDictionary()
		{
			items = new Dictionary<K, V>();
		}

		public V GetOrAddAtomically(K key, Func<K, V> valueFactory)
		{
			lock(items)
			{
				V value;
				if(items.TryGetValue(key, out value))
					return value;
				value = valueFactory(key);
				items[key] = value;
				return value;
			}
		}

		public ConcurrentDictionary(IEqualityComparer<K> comparer)
		{
			items = new Dictionary<K, V>(comparer);
		}

		public void AddOrUpdate(K key, V actualValue, Func<K,V,V> updateFactory)
		{
			lock(items)
			{
				V existing;
				if (items.TryGetValue(key, out existing))
				{
					items[key] = updateFactory(key, existing);
				}
				else
				{
					items[key] = actualValue;
				}
			}
		}

		public void Clear()
		{
			lock(items)
			{
				items.Clear();
			}
		}

		public bool TryGetValue(K key, out V value)
		{
			lock (items)
			{
				return items.TryGetValue(key, out value);
			}
		}

		public void TryRemove(K key, out V val)
		{
			lock (items)
			{
				if (items.TryGetValue(key, out val))
					items.Remove(key);
			}
		}

		public IEnumerator<KeyValuePair<K, V>> GetEnumerator()
		{
			lock(items)
			{
				return items.ToList().GetEnumerator();
			}
		}

		IEnumerator IEnumerable.GetEnumerator()
		{
			return GetEnumerator();
		}
	}
}
#endif