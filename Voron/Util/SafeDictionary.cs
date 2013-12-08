using System;
using System.Collections;
using System.Collections.Generic;
using Voron.Trees;

namespace Voron.Util
{
	public class SafeDictionary<TKey,TValue> : IEnumerable<KeyValuePair<TKey,TValue>>
	{
		private Dictionary<TKey, TValue> _inner = new Dictionary<TKey, TValue>();
		private IEqualityComparer<TKey> _comparer = EqualityComparer<TKey>.Default;

		private SafeDictionary()
		{
			
		}

		public static readonly SafeDictionary<TKey, TValue> Empty = new SafeDictionary<TKey, TValue>();

		public static SafeDictionary<TKey, TValue> From(Dictionary<TKey, TValue> inner)
		{
			return new SafeDictionary<TKey, TValue>
			{
				_inner = inner
			};
		}

		public bool TryGetValue(TKey key, out TValue value)
		{
			return _inner.TryGetValue(key, out value);
		}

		public SafeDictionary<TKey, TValue> SetItems(Dictionary<TKey, TValue> items)
		{
			var newValues = new Dictionary<TKey, TValue>(_inner, _comparer);

			foreach (var value in items)
			{
				newValues[value.Key] = value.Value;
			}

			return new SafeDictionary<TKey, TValue>
			{
				_inner = newValues,
				_comparer = _comparer
			};
		}

		public SafeDictionary<TKey, TValue> Add(TKey key, TValue value)
		{
			return new SafeDictionary<TKey, TValue>
			{
				_inner = new Dictionary<TKey, TValue>(_inner, _comparer)
				{
					{key,value}
				},
				_comparer = _comparer
			};
		}

		public IEnumerator<KeyValuePair<TKey, TValue>> GetEnumerator()
		{
			return _inner.GetEnumerator();
		}

		IEnumerator IEnumerable.GetEnumerator()
		{
			return GetEnumerator();
		}

		public SafeDictionary<TKey, TValue> RemoveRange(IEnumerable<TKey> range)
		{
			var items = new Dictionary<TKey, TValue>(_inner, _comparer);
			foreach (var key in range)
			{
				items.Remove(key);
			}
			return new SafeDictionary<TKey, TValue>
			{
				_inner = items,
				_comparer = _comparer
			};
		}

		public int Count {get { return _inner.Count; }}

		public IEnumerable<TValue> Values { get { return _inner.Values; }}

		public TValue this[TKey key]
		{
			get { return _inner[key]; }
		}

		public SafeDictionary<TKey, TValue> WithComparers(IEqualityComparer<TKey> equalityComparer)
		{
			return new SafeDictionary<TKey, TValue>
			{
				_comparer = equalityComparer,
				_inner = _inner
			};
		}

		public SafeDictionary<TKey, TValue> Remove(TKey key)
		{
			var items = new Dictionary<TKey, TValue>(_inner, _comparer);
			items.Remove(key);
			return new SafeDictionary<TKey, TValue>
			{
				_comparer = _comparer,
				_inner = _inner
			};
		}
	}
}