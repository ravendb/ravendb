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

        var internalTimerState = new TimerState
        {
            State = state is not null ? new WeakReference<object>(state) : null,
            Callback = new WeakReference<WeakReferencingTimerCallback>(callback)
        };

        _timer = new Timer(StaticCallback, internalTimerState, dueTime, period);

        internalTimerState.Timer = _timer;
    }

    private static void StaticCallback(object state)
    {
        var timerState = (TimerState)state;
        object stateObj = null;

        if (timerState.State is not null && timerState.State.TryGetTarget(out stateObj) == false)
        {
            timerState.Timer.Dispose();
            return;
        }

        if (timerState.Callback.TryGetTarget(out var callback) == false)
        {
            timerState.Timer.Dispose();
            return;
        }

        callback(stateObj);
    }

    private class TimerState
    {
        public WeakReference<object> State;
        public WeakReference<WeakReferencingTimerCallback> Callback;
        public Timer Timer;
    }

    public void Dispose()
    {
        _timer?.Dispose();
    }

    public void Change(TimeSpan dueTime, TimeSpan period)
    {
        _timer?.Change(dueTime, period);
    }
}
