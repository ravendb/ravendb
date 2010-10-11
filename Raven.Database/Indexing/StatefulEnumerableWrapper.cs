using System.Collections;
using System.Collections.Generic;

namespace Raven.Database.Indexing
{
	public class StatefulEnumerableWrapper<T> : IEnumerable<T>
	{
		private readonly IEnumerator<T> inner;

		public StatefulEnumerableWrapper(IEnumerator<T> inner)
		{
			this.inner = inner;
		}

		public T Current
		{
			get { return inner.Current; }
		}

		#region IEnumerable<T> Members

		public IEnumerator<T> GetEnumerator()
		{
			return new StatefulbEnumeratorWrapper(inner);
		}

		IEnumerator IEnumerable.GetEnumerator()
		{
			return GetEnumerator();
		}

		#endregion

		#region Nested type: StatefulbEnumeratorWrapper

		public class StatefulbEnumeratorWrapper : IEnumerator<T>
		{
			private readonly IEnumerator<T> inner;

			public StatefulbEnumeratorWrapper(IEnumerator<T> inner)
			{
				this.inner = inner;
			}

			#region IEnumerator<T> Members

			public void Dispose()
			{
				inner.Dispose();
			}

			public bool MoveNext()
			{
				return inner.MoveNext();
			}

			public void Reset()
			{
				inner.Reset();
			}

			public T Current
			{
				get { return inner.Current; }
			}

			object IEnumerator.Current
			{
				get { return Current; }
			}

			#endregion
		}

		#endregion
	}
}
