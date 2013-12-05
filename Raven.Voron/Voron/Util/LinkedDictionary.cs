using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Serialization;

namespace Voron.Util
{
	public class LinkedDictionary<TKey, TValue> : IEnumerable<KeyValuePair<TKey, TValue>>
		where TValue : class
	{
		private static readonly TValue _deleteMarker = (TValue) FormatterServices.GetUninitializedObject(typeof (TValue));

		private class Node
		{
			public Dictionary<TKey, TValue> Dictionary;
			public KeyValuePair<TKey, TValue>? Value;
			public Node Prev;
			public int Depth;
		}

		private IEqualityComparer<TKey> _comparer = EqualityComparer<TKey>.Default;

		private Node _header;

		private LinkedDictionary()
		{

		}

		public static readonly LinkedDictionary<TKey, TValue> Empty = new LinkedDictionary<TKey, TValue>();

		public static LinkedDictionary<TKey, TValue> From(Dictionary<TKey, TValue> inner)
		{
			return new LinkedDictionary<TKey, TValue>
			{
				_header = new Node
				{
					Dictionary = inner,
					Prev = null,
					Depth = 1,
				}
			};
		}

		public bool TryGetValue(TKey key, out TValue value)
		{
			var current = _header;
			while (current != null)
			{
				if (current.Value != null)
				{
					var kvp = current.Value.Value;
					if (_comparer.Equals(kvp.Key, key))
					{
						if (kvp.Value == _deleteMarker)
						{
							value = null;
							return false;
						}
						value = kvp.Value;
						return true;
					}
				}
				else
				{
					if (current.Dictionary.TryGetValue(key, out value))
					{
						if (value == _deleteMarker)
						{
							value = null;
							return false;
						}
						return true;
					}

				}
				current = current.Prev;
			}
			value = null;
			return false;
		}

		private LinkedDictionary<TKey, TValue> NewNode(Node header)
		{
			header.Prev = _header;
			if (_header == null)
			{
				header.Depth = 1;
			}
			else
			{
				header.Depth = _header.Depth + 1;
			}
			if (header.Depth > 32)
			{
				return MergeItems(header);
			}
			return new LinkedDictionary<TKey, TValue>
			{
				_comparer = _comparer,
				_header = header
			};
		}

		private LinkedDictionary<TKey, TValue> MergeItems(Node header)
		{
			// too deep, let us merge it all and generate a single dictionary;
			var dic = new Dictionary<TKey, TValue>(_comparer);
			var existing = new HashSet<TKey>(_comparer);
			var current = header;
			while (current != null)
			{
				if (current.Value != null)
				{
					var kvp = current.Value.Value;
					if (existing.Add(kvp.Key) && kvp.Value != _deleteMarker)
					{
						dic.Add(kvp.Key, kvp.Value);
					}
				}
				else
				{
					foreach (var kvp in current.Dictionary)
					{
						if (existing.Add(kvp.Key) && kvp.Value != _deleteMarker)
						{
							dic.Add(kvp.Key, kvp.Value);
						}
					}
				}

				current = current.Prev;
			}
			return new LinkedDictionary<TKey, TValue>
			{
				_comparer = _comparer,
				_header = new Node
				{
					Dictionary = dic,
					Depth = 1,
				}
			};
		}

		public LinkedDictionary<TKey, TValue> SetItems(Dictionary<TKey, TValue> items)
		{
			return NewNode(new Node { Dictionary = items });
		}

		public LinkedDictionary<TKey, TValue> Add(TKey key, TValue value)
		{
			return NewNode(new Node
			{
				Value = new KeyValuePair<TKey, TValue>(key, value),
			});
		}

		public IEnumerator<KeyValuePair<TKey, TValue>> GetEnumerator()
		{
			var current = _header;
			var existing = new HashSet<TKey>(_comparer);
			while (current != null)
			{
				if (current.Value != null)
				{
					var kvp = current.Value.Value;
					if (existing.Add(kvp.Key) && kvp.Value != _deleteMarker)
					{
						yield return kvp;
					}
				}
				else
				{
					foreach (var kvp in current.Dictionary)
					{
						if (existing.Add(kvp.Key) && kvp.Value != _deleteMarker)
						{
							yield return kvp;
						}
					}
				}

				current = current.Prev;
			}
		}

		IEnumerator IEnumerable.GetEnumerator()
		{
			return GetEnumerator();
		}

		public LinkedDictionary<TKey, TValue> RemoveRange(IEnumerable<TKey> range)
		{
			var items = new Dictionary<TKey, TValue>();
			foreach (var key in range)
			{
				items[key] = _deleteMarker;
			}
			return NewNode(new Node
			{
				Dictionary = items
			});
		}

		public bool IsEmpty
		{
			get
			{
				var current = _header;
				while (current != null)
				{
					if (current.Value == null)
					{
						if (current.Dictionary.Values.Any(x => x != _deleteMarker))
							return false;
					}
					else if (current.Value.Value.Value != _deleteMarker)
					{
						return false;
					}
					current = current.Prev;
				}
				return true;
			}
		}

		public LinkedDictionary<TKey, TValue> WithComparers(IEqualityComparer<TKey> equalityComparer)
		{
			return new LinkedDictionary<TKey, TValue>
			{
				_comparer = equalityComparer,
				_header = _header
			};
		}

		public LinkedDictionary<TKey, TValue> Remove(TKey key)
		{
			return new LinkedDictionary<TKey, TValue>
			{
				_comparer = _comparer,
				_header = new Node
				{
					Prev = _header,
					Value = new KeyValuePair<TKey, TValue>(key, _deleteMarker)
				}
			};
		}
	}
}