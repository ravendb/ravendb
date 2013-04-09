// -----------------------------------------------------------------------
//  <copyright file="AsyncManualResetEvent.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Threading;
using System.Threading.Tasks;

namespace Raven.Client.Util
{
	public class AsyncManualResetEvent
	{
		private volatile TaskCompletionSource<bool> completionSource = new TaskCompletionSource<bool>();

		public Task WaitAsync() { return completionSource.Task; }

		public void Set() { completionSource.TrySetResult(true); }

		public void Reset()
		{
			while (true)
			{
				var tcs = completionSource;
				if (tcs.Task.IsCompleted == false)
					return;

#pragma warning disable 420
				if (Interlocked.CompareExchange(ref completionSource, new TaskCompletionSource<bool>(), tcs) == tcs)
					return;
#pragma warning restore 420
			}
		}
	}
}