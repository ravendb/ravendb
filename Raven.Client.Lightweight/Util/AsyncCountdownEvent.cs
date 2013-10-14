using System;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.Util;

namespace Raven.Client.Util
{
	public class AsyncCountdownEvent
	{
		private readonly AsyncManualResetEvent resetEvent = new AsyncManualResetEvent();
		private volatile int count;
		private volatile int errors = 0;

		public AsyncCountdownEvent(int initialCount)
		{
			if (initialCount <= 0) 
				throw new ArgumentOutOfRangeException("initialCount");
			count = initialCount;
		}

		public bool Active
		{
			get { return count > 0; }
		}

		public int Count
		{
			get { return count; }
		}

		public Task WaitAsync() { return resetEvent.WaitAsync(); }

		public bool Signal()
		{
			if (count <= 0)
				return false;

#pragma warning disable 420
			int newCount = Interlocked.Decrement(ref count);
#pragma warning restore 420
			if (newCount - errors == 0)
				resetEvent.Set();
			if (newCount < 0)
				return false;

			return true;
		}

		public void Error()
		{
#pragma warning disable 420
			var newError = Interlocked.Increment(ref errors);
#pragma warning restore 420
			if (count - newError == 0)
				resetEvent.Set();
		}
	}
}
