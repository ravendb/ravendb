using System;
using System.Diagnostics;
using System.Threading;

namespace Raven.Server.Extensions
{
    public static class CancellationTokenSourceExtensions
    {
        public static CancellationTimeout TimeoutAfter(this CancellationTokenSource cts, TimeSpan dueTime)
        {
            return new CancellationTimeout(cts, dueTime);
        }
    }

    public class CancellationTimeout : IDisposable
    {
        public CancellationTokenSource CancellationTokenSource { get; private set; }
        private readonly Timer _timer;
        private readonly int _dueTime;
        private readonly object _locker = new object();
        private volatile bool _isTimerDisposed;
        private readonly Stopwatch _sp = Stopwatch.StartNew();

        public void ThrowIfCancellationRequested()
        {
            CancellationTokenSource.Token.ThrowIfCancellationRequested();
        }

        public CancellationTimeout(CancellationTokenSource cancellationTokenSource, TimeSpan dueTime)
        {
            if (cancellationTokenSource == null)
                throw new ArgumentNullException(nameof(cancellationTokenSource));
            if (dueTime < TimeSpan.Zero)
                throw new ArgumentOutOfRangeException(nameof(dueTime));

            _isTimerDisposed = false;
            CancellationTokenSource = cancellationTokenSource;
            _dueTime = (int) dueTime.TotalMilliseconds;
            _timer = new Timer(self =>
            {
                var source = self as CancellationTokenSource;
                if (source == null)
                    return;

                try
                {
                    source.Cancel();
                }
                catch (ObjectDisposedException)
                {
                }
            }, cancellationTokenSource, _dueTime, -1);
        }

        ~CancellationTimeout()
        {
            DisposeInternal();
        }

        public void Delay()
        {
            if (_isTimerDisposed)
                return;

            if (_sp.ElapsedMilliseconds < 500)
                return;

            lock (_locker)
            {
                if (_isTimerDisposed)
                    return;
                if (_sp.ElapsedMilliseconds < 500)
                    return;

                _sp.Restart();
                _timer.Change(_dueTime, -1);
            }
        }

        public void Pause()
        {
            if (_isTimerDisposed)
                return;

            lock (_locker)
            {
                if (_isTimerDisposed)
                    return;

                _timer.Change(Timeout.Infinite, Timeout.Infinite);
            }
        }

        public void Resume()
        {
            Delay();
        }

        public void Dispose()
        {
            GC.SuppressFinalize(this);
            DisposeInternal();
        }

        private void DisposeInternal()
        {
            if (_isTimerDisposed)
                return;

            lock (_locker)
            {
                if (_isTimerDisposed)
                    return;

                _isTimerDisposed = true;
            }

            _timer?.Dispose();
        }
    }
}