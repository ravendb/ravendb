//-----------------------------------------------------------------------
// <copyright file="StatefulEnumerableWrapper.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System.Collections;
using System.Collections.Generic;

namespace Raven.Database.Indexing
{
	public class StatefulEnumerableWrapper<T> : IEnumerable<T>
	{
		private readonly IEnumerator<T> inner;
		private bool calledMoveNext;
		public StatefulEnumerableWrapper(IEnumerator<T> inner)
		{
			this.inner = inner;
		}

		public T Current
		{
			get
			{
				if (calledMoveNext == false)
					return default(T);
				return inner.Current;
			}
		}

		#region IEnumerable<T> Members

		public IEnumerator<T> GetEnumerator()
		{
			return new StatefulbEnumeratorWrapper(inner, this);
		}

		IEnumerator IEnumerable.GetEnumerator()
		{
			return GetEnumerator();
		}

		#endregion

		#region Nested type: StatefulbEnumeratorWrapper

		private class StatefulbEnumeratorWrapper : IEnumerator<T>
		{
			private readonly IEnumerator<T> inner;
			private readonly StatefulEnumerableWrapper<T> statefulEnumerableWrapper;

			public StatefulbEnumeratorWrapper(IEnumerator<T> inner, StatefulEnumerableWrapper<T> statefulEnumerableWrapper)
			{
				this.inner = inner;
				this.statefulEnumerableWrapper = statefulEnumerableWrapper;
			}

			#region IEnumerator<T> Members

			public void Dispose()
			{
				inner.Dispose();
			}

			public bool MoveNext()
			{
				statefulEnumerableWrapper.calledMoveNext = true;
				return inner.MoveNext();
			}

			public void Reset()
			{
				statefulEnumerableWrapper.calledMoveNext = false; 
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
