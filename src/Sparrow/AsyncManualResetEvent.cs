using System;
using System.Diagnostics.Contracts;
using System.Threading;
using System.Threading.Tasks;
using Sparrow.Utils;

namespace Sparrow
{
    public class AsyncManualResetEvent
    {
        private volatile TaskCompletionSource<bool> _tcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        private CancellationToken _token;

        public AsyncManualResetEvent()
        {
            _token = CancellationToken.None;
        }

        public AsyncManualResetEvent(CancellationToken token)
        {
            token.Register(() => _tcs.TrySetCanceled());
            _token = token;
        }
        public Task<bool> WaitAsync()
        {
            _token.ThrowIfCancellationRequested();
            return _tcs.Task;
        }

        public bool IsSet => _tcs.Task.IsCompleted;

        public Task<bool> WaitAsync(TimeSpan timeout)
        {
            return new FrozenAwaiter(_tcs, this).WaitAsync(timeout);
        }

        public FrozenAwaiter GetFrozenAwaiter()
        {
            return new FrozenAwaiter(_tcs, this);
        }

        public struct FrozenAwaiter
        {
            private readonly TaskCompletionSource<bool> _tcs;
            private readonly AsyncManualResetEvent _parent;

            public FrozenAwaiter(TaskCompletionSource<bool> tcs, AsyncManualResetEvent parent)
            {
                _tcs = tcs;
                _parent = parent;
            }

            [Pure]
            public Task<bool> WaitAsync()
            {
                return _tcs.Task;
            }


            [Pure]
            public async Task<bool> WaitAsync(TimeSpan timeout)
            {
                var waitAsync = _tcs.Task;
                _parent._token.ThrowIfCancellationRequested();
                var result = await Task.WhenAny(waitAsync, TimeoutManager.WaitFor(timeout, _parent._token)).ConfigureAwait(false);
                if (_parent._token != CancellationToken.None)
                    return result == waitAsync && !_parent._token.IsCancellationRequested;

                return result == waitAsync;
            }
        }

        public void Set()
        {
            _token.ThrowIfCancellationRequested();
            _tcs.TrySetResult(true);
        }

        public void Reset(bool force = false)
        {
            while (true)
            {
                var tcs = _tcs;
                _token.ThrowIfCancellationRequested();
                if ((tcs.Task.IsCompleted == false && force == false) ||
#pragma warning disable 420
                    Interlocked.CompareExchange(ref _tcs, new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously), tcs) == tcs)
#pragma warning restore 420
                    return;
            }
        }

        public void SetAndResetAtomically()
        {
            while (true)
            {
                var tcs = _tcs;
                _token.ThrowIfCancellationRequested();
                var taskCompletionSource = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
#pragma warning disable 420
                if (Interlocked.CompareExchange(ref _tcs, taskCompletionSource, tcs) == tcs)
#pragma warning restore 420
                {
                    tcs.TrySetResult(true);
                    break;
                }
            }
        }
    }
}
