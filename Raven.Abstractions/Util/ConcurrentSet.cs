using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Linq;

namespace Raven.Database.Util
{

	[DebuggerTypeProxy(typeof(ConcurrentSet<>.DebugProxy))]
	public class ConcurrentSet<T> : IEnumerable<T>
	{
		public class DebugProxy
		{
			private ConcurrentSet<T> parent;

			public DebugProxy(ConcurrentSet<T> parent)
			{
				this.parent = parent;
			}

			[DebuggerBrowsable(DebuggerBrowsableState.RootHidden)]
			public object[] Items
			{
				get { return parent.Cast<object>().ToArray(); }
			}
		}

		private readonly ConcurrentDictionary<T, object> inner;

		public ConcurrentSet()
		{
			inner = new ConcurrentDictionary<T, object>();
		}

		public ConcurrentSet(IEqualityComparer<T> comparer)
		{
			inner = new ConcurrentDictionary<T, object>(comparer);
		}

		public int Count
		{
			get { return inner.Count; }
		}

		public void Add(T item)
		{
			TryAdd(item);
		}

		public bool TryAdd(T item)
		{
			return inner.TryAdd(item, null);
		}

		public bool Contains(T item)
		{
			return inner.ContainsKey(item);
		}

		public bool TryRemove(T item)
		{
			object _;
			return inner.TryRemove(item, out _);
		}

		public IEnumerator<T> GetEnumerator()
		{
			return inner.Keys.GetEnumerator();
		}

		IEnumerator IEnumerable.GetEnumerator()
		{
			return GetEnumerator();
		}

		public void RemoveWhere(Func<T, bool> predicate)
		{
			foreach (var item in inner.Where(item => predicate(item.Key)))
			{
				object value;
				inner.TryRemove(item.Key, out value);
			}
		}

		public void Clear()
		{
			inner.Clear();
		}

		public override string ToString()
		{
			return Count.ToString("#,#", CultureInfo.InvariantCulture);
		}
	}
}