#if !SILVERLIGHT
using System.Collections.Concurrent;
using System.Collections.Generic;

namespace Raven.Database.Util
{
	public class SizeLimitedConcurrentSet<T>
	{
		private readonly ConcurrentDictionary<T, object> dic;
		private readonly ConcurrentQueue<T> queue = new ConcurrentQueue<T>();

		private readonly int size;

		public SizeLimitedConcurrentSet(int size = 100)
			: this(size, EqualityComparer<T>.Default)
		{

		}

		public SizeLimitedConcurrentSet(int size, IEqualityComparer<T> equalityComparer)
		{
			this.size = size;
			dic = new ConcurrentDictionary<T, object>(equalityComparer);
		}

		public bool Add(T item)
		{
			if (dic.TryAdd(item, null) == false)
				return false;
			queue.Enqueue(item);

			while (queue.Count > size)
			{
				T result;
				if (queue.TryDequeue(out result) == false)
					break;
				object value;
				dic.TryRemove(result, out value);
			}

			return true;
		}

		public bool TryRemove(T item)
		{
			object value;
			return dic.TryRemove(item, out value);
		}

		public bool Contains(T item)
		{
			return dic.ContainsKey(item);
		}

		public T[] ToArray()
		{
			return queue.ToArray();
		}
	}
}
#endif