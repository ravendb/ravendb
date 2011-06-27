using System;
using System.Collections.Generic;
using System.Threading;
using System.Linq;

namespace Raven.Client.Connection.Profiling
{
	internal class ConcurrentLruLSet<T>
	{
		private readonly int maxCapacity;
		private LinkedList<T> items = new LinkedList<T>();

		public ConcurrentLruLSet(int maxCapacity)
		{
			this.maxCapacity = maxCapacity;
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

				if(newList.Count > maxCapacity)
				{
					newList.RemoveFirst();
				}

				if (Interlocked.CompareExchange(ref items, newList, current) == current)
					return;

			} while (true);

		}
	}
}