using System;
using System.Threading;

namespace Raven.Client.Util;

internal class WeakReferencingTimer : IDisposable
{
    internal delegate void WeakReferencingTimerCallback(object state);

    private readonly Timer _timer;

    public WeakReferencingTimer(WeakReferencingTimerCallback callback, object state, TimeSpan dueTime, TimeSpan period)
    {
        if (callback == null)
            throw new ArgumentNullException(nameof(callback));

        if (callback.Method.IsStatic == false)
            throw new ArgumentException("Callback is supposed to be a static method", nameof(callback));

        var internalTimerState = new TimerState
        {
            State = state is not null ? new WeakReference<object>(state) : null,
            Callback = callback
        };

        const uint infinite = unchecked((uint)-1); // register the timer but do not activate it yet

        _timer = new Timer(StaticCallback, internalTimerState, infinite, infinite);

        internalTimerState.Timer = _timer; // assign timer instance to the state

        _timer.Change(dueTime, period); // now let's activate the timer
    }

    private static void StaticCallback(object state)
    {
        var timerState = (TimerState)state;
        object stateObj = null;

        if (timerState.State is not null && timerState.State.TryGetTarget(out stateObj) == false)
        {
            try
            {
                timerState.Timer.Dispose();
            }
#pragma warning disable CS0168
            catch (Exception e)
#pragma warning restore CS0168
            {
#if DEBUG
                Console.WriteLine($"Disposal of timer instance got an exception:{Environment.NewLine}{e}");
#endif
                // ignored
            }
            return;
        }

        timerState.Callback(stateObj);
    }

    private class TimerState
    {
        public WeakReference<object> State;
        public WeakReferencingTimerCallback Callback;
        public Timer Timer;
    }

    public void Change(TimeSpan dueTime, TimeSpan period)
    {
        _timer?.Change(dueTime, period);
    }

    public void Dispose()
    {
        _timer?.Dispose();
    }
}
