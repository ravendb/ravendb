using System;
using System.Diagnostics;
using System.Threading;

namespace Raven.Server.ServerWide
{
    public class OperationCancelToken : IDisposable
    {
        public static readonly OperationCancelToken None = new(CancellationToken.None);

        private readonly CancellationTokenSource _cts;
        private bool _disposed;
        private Stopwatch _sw;
        private readonly TimeSpan _cancelAfter;

        public readonly CancellationToken Token;
        private readonly long _minimumDelayTimeInMs;

        public OperationCancelToken(TimeSpan cancelAfter, CancellationToken token)
        {
            ValidateCancelAfter(cancelAfter);

            _cts = CancellationTokenSource.CreateLinkedTokenSource(token);
            _cancelAfter = cancelAfter;
            Token = _cts.Token;

            _minimumDelayTimeInMs = GetMinimumDelayTime(cancelAfter);
            _cts.CancelAfter(cancelAfter);
        }

        public OperationCancelToken(TimeSpan cancelAfter, CancellationToken token1, CancellationToken token2)
        {
            ValidateCancelAfter(cancelAfter);

            _cts = CancellationTokenSource.CreateLinkedTokenSource(token1, token2);
            _cancelAfter = cancelAfter;
            Token = _cts.Token;

            _minimumDelayTimeInMs = GetMinimumDelayTime(cancelAfter);
            _cts.CancelAfter(cancelAfter);
        }

        public OperationCancelToken(CancellationToken token)
            : this(Timeout.InfiniteTimeSpan, token)
        {
        }

        public OperationCancelToken(CancellationToken token1, CancellationToken token2)
            : this(Timeout.InfiniteTimeSpan, token1, token2)
        {
        }

        public void ThrowIfCancellationRequested() => Token.ThrowIfCancellationRequested();

        public void Delay()
        {
            if (_disposed)
                return;

            if (_cancelAfter == Timeout.InfiniteTimeSpan)
                throw new InvalidOperationException("Cannot delay cancellation without timeout set.");

            if (_sw?.ElapsedMilliseconds < _minimumDelayTimeInMs)
                return;

            lock (this)
            {
                if (_disposed)
                    return;
                if (_sw?.ElapsedMilliseconds < _minimumDelayTimeInMs)
                    return;

                if (_sw == null)
                    _sw = Stopwatch.StartNew();
                else
                    _sw.Restart();

                _cts.CancelAfter(_cancelAfter);
            }
        }

        public void Cancel()
        {
            _cts.Cancel();
        }

        public void Dispose()
        {
            GC.SuppressFinalize(this);
            DisposeInternal();
        }

        ~OperationCancelToken()
        {
            _cts?.Dispose();
        }

        private void DisposeInternal()
        {
            if (_disposed)
                return;

            lock (this)
            {
                if (_disposed)
                    return;

                _disposed = true;
            }

            _cts?.Dispose();
        }

        private static long GetMinimumDelayTime(TimeSpan timeSpan)
        {
            return Math.Max(250, (long)(timeSpan / 3).TotalMilliseconds);
        }

        private static void ValidateCancelAfter(TimeSpan cancelAfter)
        {
            if (cancelAfter != Timeout.InfiniteTimeSpan && cancelAfter < TimeSpan.Zero)
                throw new ArgumentOutOfRangeException(nameof(cancelAfter));
        }
    }
}
