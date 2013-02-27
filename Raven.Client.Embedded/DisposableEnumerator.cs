using System;
using System.Collections;
using System.Collections.Generic;

namespace Raven.Client.Embedded
{
	internal class DisposableEnumerator<T> : IEnumerator<T>
	{
		private readonly IEnumerator<T> inner;
		private readonly Action disposeAction;

		public DisposableEnumerator(IEnumerator<T> inner, Action disposeAction)
		{
			this.inner = inner;
			this.disposeAction = disposeAction;
		}

		public void Dispose()
		{
			disposeAction();
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

		object IEnumerator.Current
		{
			get { return Current; }
		}

		public T Current
		{
			get { return inner.Current; }
		}
	}
}