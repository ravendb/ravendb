// -----------------------------------------------------------------------
//  <copyright file="SortedKeyList.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
namespace Raven.Database.Util
{
	using System.Collections;
	using System.Collections.Generic;

	public class SortedKeyList<T> : IEnumerable<T>
	{
		private SortedList<T, object> inner;

		public SortedKeyList()
		{
			inner = new SortedList<T, object>();
		}

		public void Add(T item)
		{
			inner.Add(item, null, true);
		}

		public int Count
		{
			get { return inner.Count; }
		}

		public bool Contains(T item)
		{
			return inner.ContainsKey(item);
		}

		public void RemoveSmallerOrEqual(T item)
		{
			inner.RemoveSmallerOrEqual(item);
		}

		public IEnumerator<T> GetEnumerator()
		{
			return inner.Keys.GetEnumerator();
		}

		IEnumerator IEnumerable.GetEnumerator()
		{
			return GetEnumerator();
		}
	}
}