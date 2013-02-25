using System;
using System.Collections.Generic;
using System.Threading;
using System.Linq;

namespace Raven.Client.Connection.Profiling
{
	internal class ConcurrentLruLSet<T>
	{
		private readonly int maxCapacity;
		private readonly Action<T> onDrop;
		private LinkedList<T> items = new LinkedList<T>();

		public ConcurrentLruLSet(int maxCapacity, Action<T> onDrop = null)
		{
			this.maxCapacity = maxCapacity;
			this.onDrop = onDrop;
		}

		public T FirstOrDefault(Func<T, bool> predicate)
		{
			var current = items;
			return current.FirstOrDefault(predicate);
		}

		public void Push(T item)
		{
			do
			{
				var current = items;
				var newList = new LinkedList<T>(current);

				// this ensures the item is at the head of the list
				newList.Remove(item);
				newList.AddLast(item);

				LinkedListNode<T> linkedListNode = null;
				if (newList.Count > maxCapacity)
				{
					linkedListNode = newList.First;
					newList.RemoveFirst();
				}

				if (Interlocked.CompareExchange(ref items, newList, current) != current)
					continue;

				if (onDrop != null && linkedListNode != null)
					onDrop(linkedListNode.Value);

				return;
			} while (true);

		}

		public void Clear()
		{
			items = new LinkedList<T>();
		}

		public void ClearHalf()
		{
			do
			{
				var current = items;
				var newList = new LinkedList<T>(current.Skip(current.Count / 2));

				if (Interlocked.CompareExchange(ref items, newList, current) != current)
					continue;

				if (onDrop != null)
				{
					foreach (var item in current.Take(current.Count/2))
					{
						onDrop(item);
					}
				}
				return;
			} while (true);
		}

		public void Remove(T val)
		{
			items.Remove(val);
		}
	}
}