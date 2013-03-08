using System.Collections;
using System.Collections.Generic;

namespace Raven.Database.Util
{
	public class ActiveEnumerable<T> : IEnumerable<T>, IEnumerator<T>
	{
		private T first;
		private readonly bool isNotEmpty;
		private bool isOnFirst;
		readonly IEnumerator<T> enumerator;

		public ActiveEnumerable(IEnumerable<T> enumerable)
		{
			enumerator = enumerable.GetEnumerator();
			isNotEmpty = enumerator.MoveNext();
			isOnFirst = true;
			if (isNotEmpty)
				first = enumerator.Current;
		}

		public IEnumerator<T> GetEnumerator()
		{
			return this;
		}

		IEnumerator IEnumerable.GetEnumerator()
		{
			return GetEnumerator();
		}

		public void Dispose()
		{
			enumerator.Dispose();
		}

		public bool MoveNext()
		{
			if(isOnFirst)
			{
				isOnFirst = false;
				Current = first;
				first = default(T);
				return isNotEmpty;
			}
			var moveNext = enumerator.MoveNext();
			Current = moveNext ? enumerator.Current : default (T);
			return moveNext;
		}

		public void Reset()
		{
			throw new System.NotSupportedException();
		}

		public T Current { get; private set; }
		object IEnumerator.Current
		{
			get { return Current; }
		}
	}
}