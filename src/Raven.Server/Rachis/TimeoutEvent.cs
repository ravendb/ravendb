using System;
using System.Threading;
using Raven.Client.Exceptions;
using Sparrow.Logging;
using Sparrow.LowMemory;
using Sparrow.Platform;

namespace Raven.Server.Rachis
{
    public class TimeoutEvent : IDisposable, ILowMemoryHandler
    {
        public bool Disable;

        private readonly ManualResetEventSlim _timeoutEventSlim = new ManualResetEventSlim();
        private readonly Timer _timer;
        private long _lastDeferredTicks;
        private Action _timeoutHappened;
        private string _currentLeader;

        public TimeoutEvent(int timeoutPeriod, string name)
        {
            TimeoutPeriod = timeoutPeriod;
            _lastDeferredTicks = DateTime.UtcNow.Ticks;
            _timer = new Timer(Callback, null, Timeout.Infinite, Timeout.Infinite);
            _logger = LoggingSource.Instance.GetLogger<TimeoutEvent>(name);
            LowMemoryNotification.Instance?.RegisterLowMemoryHandler(this);
        }

        public int TimeoutPeriod;
        private readonly Logger _logger;

        public void Start(Action onTimeout)
        {
            lock (this)
            {
                if (onTimeout == _timeoutHappened)
                {
                    Defer(_currentLeader);
                    return;
                }
                _timeoutHappened = onTimeout;

                try
                {
                    _timer.Change(TimeoutPeriod, TimeoutPeriod);
                }
                catch (ObjectDisposedException)
                {
                    // done here
                }
            }
        }

        private void Callback(object state)
        {
            if (_timeoutEventSlim.IsSet == false)
            {
                try
                {
                    ExecuteTimeoutBehavior();
                }
                catch (Exception e)
                {
                    if (_logger.IsInfoEnabled)
                    {
                        _logger .Info($"Failed to execute timeout callback, will retry again" ,e);
                    }

                    lock (this)
                    {
                        // schedule again once, because the error may be transient
                        if (_timeoutHappened != null)
                            _timer.Change(TimeoutPeriod, TimeoutPeriod);
                    }
                }
                return;
            }
            _timeoutEventSlim.Reset();
       
        }

        private void DisableTimeoutInternal()
        {
            try
            {
                _timer.Change(Timeout.Infinite, Timeout.Infinite);
            }
            catch (ObjectDisposedException)
            {
            }
            _timeoutHappened = null;
            _currentLeader = null;
        }

        public void DisableTimeout()
        {
            lock (this)
            {
                DisableTimeoutInternal();
            }
        }

        public void ExecuteTimeoutBehavior()
        {
            lock (this)
            {
                if (Disable)
                    return;
                
                _timer.Change(Timeout.Infinite, Timeout.Infinite);

                try
                {
                    if (_timeoutHappened == null)
                        return;

                    _timeoutHappened?.Invoke();
                }
                catch (ConcurrencyException)
                {
                    // expected, ignoring
                }
            }
        }

        public void Defer(string leader)
        {
            Interlocked.Exchange(ref _lastDeferredTicks, DateTime.UtcNow.Ticks);
            _timeoutEventSlim.Set();
            _currentLeader = leader;
        }


        public bool ExpiredLastDeferral(double maxInMs, out string leader)
        {
            var ticks = Interlocked.Read(ref _lastDeferredTicks);
            var elapsed = (DateTime.UtcNow - new DateTime(ticks));
            if (elapsed < TimeSpan.Zero)
            {
                leader = null;
                return true; // if times goes backward (clock shift, etc), assume expired
            }
            leader = _currentLeader;
            return elapsed.TotalMilliseconds > maxInMs;
        }

        public void Dispose()
        {
            DisableTimeout();
            using (var waitHandle = new ManualResetEvent(false))
            {
                if (_timer.Dispose(waitHandle))
                {
                    waitHandle.WaitOne();
                }
            }
        }

        public void LowMemory()
        {
            // will defer the timeout event if we detected low-memory
            _timeoutEventSlim.Set();
        }

        public void LowMemoryOver()
        {
            //
        }
    }
}
