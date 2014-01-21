using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using Voron.Impl.Journal;

namespace Voron.Util
{
	public class SafeList<T> : IEnumerable<T>
	{
		List<T> _inner = new List<T>();

		public static readonly SafeList<T> Empty = new SafeList<T>();

		private SafeList()
		{

		}

		public SafeList<T> AddRange(IEnumerable<T> items)
		{
			var inner = new List<T>(_inner);
			inner.AddRange(items);
			return new SafeList<T>
			{
				_inner = inner
			};
		}

		public SafeList<T> Add(T item)
		{
			return new SafeList<T>
			{
				_inner = new List<T>(_inner) {item}
			};
		}

		public int Count
		{
			get { return _inner.Count; }
		}

		public T this[int i]
		{
			get { return _inner[i]; }
		}

		public IEnumerator<T> GetEnumerator()
		{
			return _inner.GetEnumerator();
		}

		IEnumerator IEnumerable.GetEnumerator()
		{
			return _inner.GetEnumerator(); 
		}

		public void ForEach(Action<T> func)
		{
			_inner.ForEach(func);
		}

		public SafeList<T> RemoveAll(Func<T, bool> filter)
		{
			return new SafeList<T>
			{
				_inner = new List<T>(_inner.Where(x => filter(x) == false))
			};
		}

		public T Find(Predicate<T> predicate)
		{
			return _inner.Find(predicate);
		}

		public SafeList<T> RemoveAllAndGetDiscards(Predicate<T> filter, out List<T> discards)
		{
			var list = new List<T>();

			discards = list;

			return new SafeList<T>
			{
				_inner = new List<T>(_inner.Where(x =>
				{
					if (filter(x) == false)
					{
						list.Add(x);
						return false;
					}
					return true;
				}))
			};

		}
	}
}