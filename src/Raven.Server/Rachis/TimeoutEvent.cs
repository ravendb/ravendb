using System;
using System.Runtime.ExceptionServices;
using System.Threading;

namespace Raven.Server.Rachis
{
    public class TimeoutEvent : IDisposable
    {
        private readonly int _timeoutPeriod;
        private readonly ManualResetEventSlim _timeoutEventSlim = new ManualResetEventSlim();
        private ExceptionDispatchInfo _edi;
        private readonly Timer _timer;

        private Action _timeoutHappened;

        public TimeoutEvent(int timeoutPeriod)
        {
            _timeoutPeriod = timeoutPeriod;
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
                    _timeoutHappened?.Invoke();
                    _timeoutHappened = null;
                    _timer.Change(Timeout.Infinite, Timeout.Infinite);
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
            _timeoutEventSlim.Set();
        }

        public void Dispose()
        {
            _timeoutHappened = null;
            _timer.Dispose();
        }
    }
}