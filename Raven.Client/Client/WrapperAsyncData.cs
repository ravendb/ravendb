using System;
using System.Threading;

namespace Raven.Client.Client
{
	public class WrapperAsyncData<T> : IAsyncResult
	{
		private readonly IAsyncResult inner;
		private readonly T wrapped;

		public WrapperAsyncData(IAsyncResult inner, T wrapped)
		{
			this.inner = inner;
			this.wrapped = wrapped;
		}

		public bool IsCompleted
		{
			get { return inner.IsCompleted; }
		}

		public WaitHandle AsyncWaitHandle
		{
			get
			{
				return inner.AsyncWaitHandle;
			}
		}

		public object AsyncState
		{
			get { return inner.AsyncState; }
		}

		public bool CompletedSynchronously
		{
			get { return inner.CompletedSynchronously; }
		}

		public T Wrapped
		{
			get { return wrapped; }
		}

		public IAsyncResult Inner
		{
			get {
				return inner;
			}
		}
	}
}