using System.Threading;
using System.Threading.Tasks;

namespace Voron.Util
{
	public class AsyncManualResetEvent
	{
		private volatile TaskCompletionSource<bool> _tcs = new TaskCompletionSource<bool>();

		public Task WaitAsync() { return _tcs.Task; }

		public async Task<bool> WaitAsync(int timeout)
		{
			var waitAsync = _tcs.Task;
			var result = await Task.WhenAny(waitAsync, Task.Delay(timeout));
			return result == waitAsync;
		}

		public void Set() { _tcs.TrySetResult(true); }

		public void Reset()
		{
			while (true)
			{
				var tcs = _tcs;
				if (!tcs.Task.IsCompleted ||
#pragma warning disable 420
					Interlocked.CompareExchange(ref _tcs, new TaskCompletionSource<bool>(), tcs) == tcs)
#pragma warning restore 420
					return;
			}
		}
	}
}