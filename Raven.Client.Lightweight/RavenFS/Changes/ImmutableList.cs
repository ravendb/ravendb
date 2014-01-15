using System;
using System.Collections;
using System.Collections.Generic;

namespace Raven.Client.RavenFS.Changes
{
	public class ImmutableList<T> : IEnumerable<T>
	{
		private T[] items;

		public ImmutableList()
		{
		}

		protected ImmutableList(T[] items)
		{
			this.items = items;
		}

		public int Count
		{
			get { return items == null ? 0 : items.Length; }
		}

		public ImmutableList<T> Add(T item)
		{
			if (items == null)
				return new ImmutableList<T>(new[] { item });

			var newItems = new T[items.Length + 1];
			Array.Copy(items, newItems, items.Length);
			newItems[newItems.Length - 1] = item;

			return new ImmutableList<T>(newItems);
		}

		public ImmutableList<T> Remove(T item)
		{
			var index = IndexOf(item);
			if (index < 0)
				return this;

			var newItems = new T[items.Length - 1];
			Array.Copy(items, newItems, index);
			Array.Copy(items, index + 1, newItems, index, items.Length - index - 1);

			return new ImmutableList<T>(newItems);
		}

		public int IndexOf(T item)
		{
			if (items == null)
				return -1;

			return Array.IndexOf(items, item);
		}

		public IEnumerator<T> GetEnumerator()
		{
			if (items == null)
			{
				yield break;
			}
			else
			{
				for (var i = 0; i < items.Length; i++)
				{
					yield return items[i];
				}
			}
		}

		IEnumerator IEnumerable.GetEnumerator()
		{
			return GetEnumerator();
		}
	}
}