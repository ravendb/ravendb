using System;
using System.Diagnostics;
using System.Threading;

namespace Raven.Server.ServerWide
{
    public class OperationCancelToken : IDisposable
    {
        public static OperationCancelToken None = new OperationCancelToken(CancellationToken.None);

        private readonly CancellationTokenSource _cts;
        private bool _disposed;
        private Stopwatch _sw;
        private readonly TimeSpan _cancelAfter;

        public readonly CancellationToken Token;

        public OperationCancelToken(TimeSpan cancelAfter, CancellationToken shutdown)
        {
            if (cancelAfter != Timeout.InfiniteTimeSpan && cancelAfter < TimeSpan.Zero)
                throw new ArgumentOutOfRangeException(nameof(cancelAfter));

            _cts = CancellationTokenSource.CreateLinkedTokenSource(shutdown);
            _cancelAfter = cancelAfter;
            Token = _cts.Token;

            _cts.CancelAfter(cancelAfter);
        }

        public OperationCancelToken(CancellationToken shutdown)
            : this(Timeout.InfiniteTimeSpan, shutdown)
        {
        }       

        public void Delay()
        {
            if (_disposed)
                return;

            if (_cancelAfter == Timeout.InfiniteTimeSpan)
                throw new InvalidOperationException("Cannot delay cancellation without timeout set.");

            const int minimumDelayTime = 25;

            if (_sw?.ElapsedMilliseconds < minimumDelayTime)
                return;

            lock (this)
            {
                if (_disposed)
                    return;
                if (_sw?.ElapsedMilliseconds < minimumDelayTime)
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
    }
}
