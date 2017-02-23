using System;
using System.Runtime.ExceptionServices;
using System.Threading;

namespace Raven.Server.Rachis
{
    public class TimeoutEvent : IDisposable
    {
        public static bool Disable;

        private readonly int _timeoutPeriod;
        private readonly ManualResetEventSlim _timeoutEventSlim = new ManualResetEventSlim();
        private ExceptionDispatchInfo _edi;
        private readonly Timer _timer;
        private long _lastDeferredTicks;
        private Action _timeoutHappened;

        public TimeoutEvent(int timeoutPeriod)
        {
            _timeoutPeriod = timeoutPeriod;
            _lastDeferredTicks = DateTime.UtcNow.Ticks;
            _timer = new Timer(Callback, null, Timeout.Infinite, Timeout.Infinite);
        }

        public int TimeoutPeriod => _timeoutPeriod;

        public void Start(Action onTimeout)
        {
            _edi?.Throw();
            _timeoutHappened = onTimeout;
            _timer.Change(_timeoutPeriod, _timeoutPeriod);
        }

        private void Callback(object state)
        {
            try
            {
                if (_timeoutEventSlim.IsSet == false)
                {
                    lock (this)
                    {
                        if (_timeoutHappened == null)
                            return;
                        if (Disable)
                            return;
                         _timeoutHappened?.Invoke();
                        _timeoutHappened = null;
                        _timer.Change(Timeout.Infinite, Timeout.Infinite);
                    }
                    return;
                }
                _timeoutEventSlim.Reset();
            }
            catch (Exception e)
            {
                _edi = ExceptionDispatchInfo.Capture(e);
                _timer.Dispose();
            }
        }

        public void Defer()
        {
            _edi?.Throw();
            Interlocked.Exchange(ref _lastDeferredTicks, DateTime.UtcNow.Ticks);
            _timeoutEventSlim.Set();
        }

        public bool ExpiredLastDeferral(int maxInInMs)
        {
            var ticks = Interlocked.Read(ref _lastDeferredTicks);
            var elapsed = (DateTime.UtcNow - new DateTime(ticks));
            if (elapsed < TimeSpan.Zero)
                return true; // if times goes backward (clock shift, etc), assume expired

            return elapsed.TotalMilliseconds > maxInInMs;
        }

        public void Dispose()
        {
            _timeoutHappened = null;
            _timer.Dispose();
        }
    }
}