using System;
using System.Collections.Generic;
using System.Threading;
using System.Linq;

namespace Raven.Client.Connection.Profiling
{
	internal class ConcurrentLruLSet<T>
	{
		private readonly int maxCapacity;
		private List<T> items = new List<T>();

		public ConcurrentLruLSet(int maxCapacity)
		{
			this.maxCapacity = maxCapacity;
		}

		public T FirstOrDefault(Func<T, bool> predicate)
		{
			var current = items;
			return current.FirstOrDefault(predicate);
		}

		public T Push(T item)
		{
			T result = default(T);
			do
			{
				var current = items;
				var newList = new List<T>(current);

				// this ensures the item is at the head of the list
				newList.Remove(item);
				newList.Add(item);

				if(newList.Count > maxCapacity)
				{
					result = newList[newList.Count - 1];
					newList.RemoveAt(newList.Count - 1);
				}

				if (Interlocked.CompareExchange(ref items, newList, current) == current)
					break;

			} while (true);

			return result;
		}
	}
}