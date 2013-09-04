//-----------------------------------------------------------------------
// <copyright file="StatefulEnumerableWrapper.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections;
using System.Collections.Generic;

namespace Raven.Database.Indexing
{
	public class StatefulEnumerableWrapper<T> : IEnumerable<T>, IDisposable
	{
		private readonly IEnumerator<T> inner;
		private bool calledMoveNext;
		private bool enumerationCompleted;
		public StatefulEnumerableWrapper(IEnumerator<T> inner)
		{
			this.inner = inner;
		}

		public T Current
		{
			get
			{
				if (calledMoveNext == false || enumerationCompleted)
					return default(T);
				return inner.Current;
			}
		}

		#region IEnumerable<T> Members

		public IEnumerator<T> GetEnumerator()
		{
			return new StatefulEnumeratorWrapper(inner, this);
		}

		IEnumerator IEnumerable.GetEnumerator()
		{
			return GetEnumerator();
		}

		#endregion

		#region Nested type: StatefulEnumeratorWrapper

		private class StatefulEnumeratorWrapper : IEnumerator<T>
		{
			private readonly IEnumerator<T> inner;
			private readonly StatefulEnumerableWrapper<T> statefulEnumerableWrapper;

			public StatefulEnumeratorWrapper(IEnumerator<T> inner, StatefulEnumerableWrapper<T> statefulEnumerableWrapper)
			{
				this.inner = inner;
				this.statefulEnumerableWrapper = statefulEnumerableWrapper;
			}

			#region IEnumerator<T> Members

			public void Dispose()
			{
				// This has been EXPLICITLY removed because of http://issues.hibernatingrhinos.com/issue/RavenDB-987
				// when we have a SelectMany, dispose is called after the source is exhuasted, but BEFORE we are done
				// enumerating the rest of the collection
				// We will rely on the caller code to release the CurrentIndexingScope.Current itself for us.
				//if (CurrentIndexingScope.Current != null)
				//	CurrentIndexingScope.Current.Source = null;
				inner.Dispose();
			}

			public bool MoveNext()
			{
				statefulEnumerableWrapper.calledMoveNext = true;
				var moveNext = inner.MoveNext();
				if (moveNext == false)
					statefulEnumerableWrapper.enumerationCompleted = true;

				if (CurrentIndexingScope.Current != null && moveNext)
					CurrentIndexingScope.Current.Source = inner.Current;
				return moveNext;
			}

			public void Reset()
			{
				statefulEnumerableWrapper.enumerationCompleted = false;
				statefulEnumerableWrapper.calledMoveNext = false;
				if (CurrentIndexingScope.Current != null)
					CurrentIndexingScope.Current.Source = null;
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

		public void Dispose()
		{
			inner.Dispose();
		}
	}
}
