using System.Collections;
using System.Collections.Generic;

namespace Raven.Studio.Models
{
	public class QueueModel<T> : IEnumerable<T>
	{
		private List<T> List { get; set; }
		private int size;
		public QueueModel(int size)
		{
			this.size = size;
			List = new List<T>(size);
		}

		public void Add(T item)
		{
			if (List.Contains(item))
				List.Remove(item);
			if(List.Count == size)
				List.RemoveAt(size-1);

			List.Insert(0, item);
		}

		public void Remove(T item)
		{
			List.Remove(item);
		}

		public void RemoveAt(int index)
		{
			List.RemoveAt(index);
		}


		public IEnumerator<T> GetEnumerator()
		{
			return List.GetEnumerator();
		}

		IEnumerator IEnumerable.GetEnumerator()
		{
			return GetEnumerator();
		}
	}
}
