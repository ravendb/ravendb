using System;
using System.Collections.Concurrent;
using System.Collections.Frozen;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;

namespace Sparrow.Utils
{
    internal static class TimeoutManager
    {
        private static FrozenDictionary<uint, TimerTaskHolder> ValuesForRead = new Dictionary<uint, TimerTaskHolder>().ToFrozenDictionary();
        private static readonly ConcurrentDictionary<uint, TimerTaskHolder> Values = new ConcurrentDictionary<uint, TimerTaskHolder>();
        private static readonly Task InfiniteTask = new TaskCompletionSource<object>(TaskCreationOptions.RunContinuationsAsynchronously).Task;

        private static readonly bool UseTaskDelay;

        static TimeoutManager()
        {
            if (bool.TryParse(Environment.GetEnvironmentVariable("RAVEN_TIMEOUTMANAGER_USE_TASK_DELAY"), out var useTaskDelay))
                UseTaskDelay = useTaskDelay;
        }

        private sealed class TimerTaskHolder : IDisposable
        {
            private TaskCompletionSource<object> _nextTimeout;
            private readonly Timer _timer;

            public void TimerCallback(object state)
            {
                var old = Interlocked.Exchange(ref _nextTimeout, null);
                old?.TrySetResult(null);
            }

            public Task NextTask
            {
                get
                {
                    while (true)
                    {
                        var tcs = _nextTimeout;
                        if (tcs != null)
                            return tcs.Task;

                        tcs = new TaskCompletionSource<object>(TaskCreationOptions.RunContinuationsAsynchronously);
                        if (Interlocked.CompareExchange(ref _nextTimeout, tcs, null) == null)
                            return tcs.Task;
                    }
                }
            }

            public TimerTaskHolder(uint timeout)
            {
                if (timeout > uint.MaxValue - 1) // Timer cannot have an interval bigger than this value
                    timeout = uint.MaxValue - 1;
                var period = TimeSpan.FromMilliseconds(timeout);
                _timer = new Timer(TimerCallback, null, period, period);
            }

            public void Dispose()
            {
                _timer?.Dispose();
            }
        }

        private sealed class CancellationSignal : TaskCompletionSource<object>
        {
            public static readonly Action<object> Callback = static state =>
            {
                var signal = (CancellationSignal)state;
                signal.TrySetCanceled(signal._token);
            };

            private readonly CancellationToken _token;

            public CancellationSignal(CancellationToken token)
                : base(TaskCreationOptions.RunContinuationsAsynchronously)
            {
                _token = token;
            }
        }

        private static async Task WaitForInternal(TimeSpan time, CancellationToken token)
        {
            if (time.TotalMilliseconds < 0)
                ThrowOutOfRange();

            var duration = (uint)Math.Min(time.TotalMilliseconds, uint.MaxValue - 45);
            if (duration == 0)
                return;

            var mod = duration % 50;
            if (mod != 0)
            {
                duration += 50 - mod;
            }

            var step = duration / 8;
            var deadline = duration - step;

            var sp = Stopwatch.StartNew();

            var next = GetHolderForDuration(duration).NextTask;
            while (true)
            {
                await next.ConfigureAwait(false);
                token.ThrowIfCancellationRequested();

                if (sp.ElapsedMilliseconds >= deadline)
                    return;

                next = GetHolderForDuration(step).NextTask;
            }
        }

        private static void ThrowOutOfRange()
        {
            throw new ArgumentOutOfRangeException("time");
        }

        private static TimerTaskHolder GetHolderForDuration(uint duration)
        {
            if (ValuesForRead.TryGetValue(duration, out var value) == false)
            {
                value = Values.GetOrAdd(duration, d => new TimerTaskHolder(d));
                ValuesForRead = Values.ToFrozenDictionary();
            }
            return value;
        }

        public static async Task<Task> WaitFor(this Task outer, TimeSpan duration, CancellationToken token = default)
        {
            if (duration == TimeSpan.Zero)
                return Task.CompletedTask;

            if (token.IsCancellationRequested)
            {
                return Task.CompletedTask;
            }

            if (duration == TimeSpan.MaxValue)
                duration = Timeout.InfiniteTimeSpan;

            if (UseTaskDelay)
            {
                return await Task.WhenAny(outer, Task.Delay(duration, token)).ConfigureAwait(false);
            }

            Task task;
            // ReSharper disable once ConvertIfStatementToConditionalTernaryExpression
            if (duration != Timeout.InfiniteTimeSpan)
                task = WaitForInternal(duration, token);
            else
                task = InfiniteTask;

            if (token.CanBeCanceled == false)
            {
                return await Task.WhenAny(outer, task).ConfigureAwait(false);
            }

            var onCancel = new CancellationSignal(token);
            using (token.Register(CancellationSignal.Callback, onCancel))
            {
                return await Task.WhenAny(outer, task, onCancel.Task).ConfigureAwait(false);
            }
        }

        public static async Task WaitFor(TimeSpan duration, CancellationToken token = default)
        {
            if (duration == TimeSpan.Zero)
                return;

            if (token.IsCancellationRequested)
            {
                return;
            }

            if (duration == TimeSpan.MaxValue)
                duration = Timeout.InfiniteTimeSpan;

            if (UseTaskDelay)
            {
                await Task.Delay(duration, token).ConfigureAwait(false);
                return;
            }

            Task task;
            // ReSharper disable once ConvertIfStatementToConditionalTernaryExpression
            if (duration != Timeout.InfiniteTimeSpan)
                task = WaitForInternal(duration, token);
            else
                task = InfiniteTask;

            if (token.CanBeCanceled == false)
            {
                await task.ConfigureAwait(false);
                return;
            }

            var onCancel = new CancellationSignal(token);
            using (token.Register(CancellationSignal.Callback, onCancel))
            {
                await Task.WhenAny(task, onCancel.Task).ConfigureAwait(false);
            }
        }
    }
}