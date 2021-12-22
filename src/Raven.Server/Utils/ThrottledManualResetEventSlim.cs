using System;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Sparrow.Threading;
using Sparrow.Utils;

namespace Raven.Server.Utils
{
    public class ThrottledManualResetEventSlim : IDisposable
    {
        public enum TimerManagement
        {
            Automatic,
            Manual
        }

        private readonly ManualResetEventSlim _mre;
        private readonly MultipleUseFlag _setCalled = new MultipleUseFlag();
        private readonly TimerManagement _timerManagement;
        private readonly CancellationToken _token;
        private bool _throttlingStarted;
        private TimeSpan? _throttlingInterval;
        internal Task _timerTask;
        private CancellationTokenSource _timerCts;

        public ThrottledManualResetEventSlim(TimeSpan? throttlingInterval, bool initialState = false, TimerManagement timerManagement = TimerManagement.Automatic, CancellationToken token = default)
        {
            _throttlingInterval = throttlingInterval;
            _mre = new ManualResetEventSlim(initialState);
            _timerManagement = timerManagement;
            _token = token;

            switch (_timerManagement)
            {
                case TimerManagement.Automatic:
                    StartThrottling();
                    break;
                case TimerManagement.Manual:
                    break;
                default:
                    throw new ArgumentException($"Unsupported throttling management: {_timerManagement}");
            }
        }

        public TimeSpan? ThrottlingInterval => _throttlingInterval;

        public void Set(bool ignoreThrottling = false)
        {
            if (_throttlingInterval == null || ignoreThrottling)
                _mre.Set();
            else
                _setCalled.Raise();
        }

        public void Reset()
        {
            _mre.Reset();
        }

        public bool Wait(int timeout, CancellationToken token)
        {
            return _mre.Wait(timeout, token);
        }

        public WaitHandle WaitHandle => _mre.WaitHandle;

        public bool IsSet => _mre.IsSet;

        public bool IsSetScheduled => _setCalled.IsRaised();

        [MethodImpl(MethodImplOptions.Synchronized)]
        public void EnableThrottlingTimer()
        {
            switch (_timerManagement)
            {
                case TimerManagement.Automatic:
                    throw new InvalidOperationException($"Cannot enable throttling timer manually when the behavior is: {nameof(TimerManagement.Automatic)}");
                case TimerManagement.Manual:
                    StartThrottling();
                    break;
                default:
                    throw new ArgumentException($"Unsupported throttling behavior: {_timerManagement}");
            }
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public void DisableThrottlingTimer()
        {
            switch (_timerManagement)
            {
                case TimerManagement.Automatic:
                    throw new InvalidOperationException($"Cannot disable throttling timer manually when the behavior is: {nameof(TimerManagement.Automatic)}");
                case TimerManagement.Manual:
                    StopThrottling();
                    break;
                default:
                    throw new ArgumentException($"Unsupported throttling behavior: {_timerManagement}");
            }
        }

        [MethodImpl(MethodImplOptions.Synchronized)]
        public void Update(TimeSpan? throttlingInterval)
        {
            var oldThrottlingInterval = _throttlingInterval;

            _throttlingInterval = throttlingInterval;

            if (oldThrottlingInterval == null)
            {
                if (_throttlingInterval == null)
                    return;

                if (_throttlingStarted || _timerManagement == TimerManagement.Automatic)
                {
                    StopThrottling();
                    StartThrottling();
                }
            }
            else
            {
                if (throttlingInterval == null)
                {
                    if (_throttlingStarted)
                        StopThrottling();
                }
                else if (throttlingInterval != oldThrottlingInterval)
                {
                    if (_throttlingStarted)
                    {
                        StopThrottling();
                        StartThrottling();
                    }
                }
            }
        }

        private void StartThrottling()
        {
            if (_throttlingStarted)
                return;

            if (_throttlingInterval == null)
                return;

            if (_timerTask != null)
                throw new InvalidOperationException("Cannot start throttling timer task because it's already started");

            _timerCts = new CancellationTokenSource();

            _timerTask = Task.Run(async () =>
            {
                using (var cts = CancellationTokenSource.CreateLinkedTokenSource(_token, _timerCts.Token))
                {
                    try
                    {
                        while (cts.IsCancellationRequested == false)
                        {
                            await TimeoutManager.WaitFor(_throttlingInterval.Value, cts.Token);

                            if (_setCalled.Lower()) 
                                _mre.Set();
                        }
                    }
                    catch (OperationCanceledException)
                    {
                        // ignore
                    }
                }
            });

            _throttlingStarted = true;
        }

        private void StopThrottling()
        {
            if (_throttlingStarted == false)
                return;

            _timerCts.Cancel();

            try
            {
                _timerTask.Wait();
            }
            catch
            {
                // ignored
            }
            _timerTask = null;
            _timerCts = null;

            if (_setCalled.Lower())
            {
                _mre.Set();
            }

            _throttlingStarted = false;
        }

        public void Dispose()
        {
            _timerCts?.Cancel();

            try
            {
                _timerTask?.Wait();
            }
            catch
            {
                // ignored
            }

            _timerTask = null;

            _timerCts?.Dispose();
            _timerCts = null;

            _mre.Dispose();
        }
    }
}
