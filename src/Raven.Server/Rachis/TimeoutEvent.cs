using System;
using System.Threading;
using Raven.Client.Exceptions;
using Raven.Server.Logging;
using Sparrow.Logging;
using Sparrow.LowMemory;
using Sparrow.Server.Logging;
using Sparrow.Threading;

namespace Raven.Server.Rachis
{
    public sealed class TimeoutEvent : IDisposable, ILowMemoryHandler
    {
        public bool Disable;

        private readonly ManualResetEventSlim _timeoutEventSlim = new ManualResetEventSlim();
        private readonly Timer _timer;
        private long _lastDeferredTicks;
        private Action _timeoutHappened;
        private string _currentLeader;

        public TimeoutEvent(int timeoutPeriod, string name, bool singleShot = true)
        {
            TimeoutPeriod = timeoutPeriod;
            _singleShot = singleShot;
            _lastDeferredTicks = DateTime.UtcNow.Ticks;
            _timer = new Timer(Callback, null, Timeout.Infinite, Timeout.Infinite);
            _logger = RavenLogManager.Instance.GetLoggerForCluster<TimeoutEvent>(LoggingComponent.Name(name));
            LowMemoryNotification.Instance?.RegisterLowMemoryHandler(this);
        }

        public TimeoutEvent(TimeSpan timeoutPeriod, string name, bool singleShot = true) :
            this((int)timeoutPeriod.TotalMilliseconds, name, singleShot)
        {
        }

        public int TimeoutPeriod;
        private readonly bool _singleShot;
        private readonly RavenLogger _logger;

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

        private readonly MultipleUseFlag _flag = new MultipleUseFlag(false);
        private void Callback(object state)
        {
            if (_timeoutEventSlim.IsSet == false)
            {
                try
                {
                    if (_flag.Raise() == false)
                        return; // prevent double entry to the callback

                    ExecuteTimeoutBehavior();
                }
                catch (Exception e)
                {
                    if (_logger.IsInfoEnabled)
                    {
                        _logger.Info("Failed to execute timeout callback, will retry again", e);
                    }

                    lock (this)
                    {
                        // schedule again once, because the error may be transient
                        _flag.Lower();

                        if (_timeoutHappened != null)
                            _timer.Change(TimeoutPeriod, TimeoutPeriod);
                    }
                }
                finally
                {
                    _flag.Lower();
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

        private int _inProgress;

        public void ExecuteTimeoutBehavior()
        {
            if (Interlocked.CompareExchange(ref _inProgress, 1, 0) == 1)
            {
                Defer(_currentLeader);
                return;
            }

            try
            {
                Action action;

                lock (this)
                {
                    if (Disable)
                        return;

                    if (_singleShot)
                        _timer.Change(Timeout.Infinite, Timeout.Infinite);

                    action = _timeoutHappened;
                }

                if (action == null)
                    return;

                action.Invoke();
            }
            catch (ConcurrencyException)
            {
                // expected, ignoring
            }
            finally
            {
                Interlocked.Exchange(ref _inProgress, 0);
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

        public void LowMemory(LowMemorySeverity lowMemorySeverity)
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
