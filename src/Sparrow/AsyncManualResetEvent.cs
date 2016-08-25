using System;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

namespace Sparrow
{
    public class AsyncManualResetEvent
    {
        private volatile TaskCompletionSource<bool> _tcs = new TaskCompletionSource<bool>();
        private readonly CancellationToken _token;

        public AsyncManualResetEvent()
        {
            _token = CancellationToken.None;		    
        }

        public AsyncManualResetEvent(CancellationToken token)
        {
            _token = token;
            // ReSharper disable once ImpureMethodCallOnReadonlyValueField
            _token.Register(() => _tcs.TrySetResult(false));
        }

        public Task<bool> WaitAsync()
        {
            return _tcs.Task;
        }

        public async Task<bool> WaitAsync(TimeSpan timeout)
        {			
            var waitAsync = _tcs.Task;			
            var result = await Task.WhenAny(waitAsync, Task.Delay(timeout, _token));
            if (_token != CancellationToken.None)
                return result == waitAsync && !_token.IsCancellationRequested;

            return result == waitAsync;
        }

        public void Set() { _tcs.TrySetResult(true); }

        public void SetByAsyncCompletion()
        {
            SetInAsyncManner(_tcs);
        }

        public void Reset(bool force = false)
        {
            while (true)
            {
                var tcs = _tcs;
                if ((tcs.Task.IsCompleted == false && force == false) ||
#pragma warning disable 420
                    Interlocked.CompareExchange(ref _tcs, new TaskCompletionSource<bool>(), tcs) == tcs)
#pragma warning restore 420
                    return;
            }
        }

        public void SetAndResetAtomically()
        {
            // we intentionally reset it first to have this operation to behave as atomic

            var previousTcs = _tcs;

            Reset(force: true);
            SetInAsyncManner(previousTcs);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static void SetInAsyncManner(TaskCompletionSource<bool> tcs)
        {
            // run the completion asynchronously to ensure that continuations (await WaitAsync()) won't happen as part of a call to TrySetResult
            // http://blogs.msdn.com/b/pfxteam/archive/2012/02/11/10266920.aspx

            var currentTcs = tcs;

            Task.Factory.StartNew(s => ((TaskCompletionSource<bool>)s).TrySetResult(true),
                currentTcs, CancellationToken.None, TaskCreationOptions.PreferFairness | TaskCreationOptions.RunContinuationsAsynchronously, TaskScheduler.Default);

            currentTcs.Task.Wait();
        }
    }
}
