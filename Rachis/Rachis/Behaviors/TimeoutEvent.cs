using System;
using System.Threading;
using System.Threading.Tasks;

namespace Rachis.Behaviors
{
    public class TimeoutEvent : IDisposable
    {
        private readonly ManualResetEventSlim _timeoutEventSlim = new ManualResetEventSlim();

        private readonly Timer _timer;

        public event Action TimeoutHappened;

        public TimeoutEvent(int timeoutPeriod)
        {
            _timer = new Timer(Callback, null, timeoutPeriod, timeoutPeriod);
        }

        private void Callback(object state)
        {
            try
            {
                if (_timeoutEventSlim.IsSet == false)
                {
                    TimeoutHappened?.Invoke();
                    return;
                }
                _timeoutEventSlim.Reset();
            }
            catch (Exception e)
            {
                // TODO: log this
            }
        }

        public void Defer()
        {
            _timeoutEventSlim.Set();
        }

        public void Dispose()
        {
            TimeoutHappened = null;
            _timer.Dispose();
        }
    }
}