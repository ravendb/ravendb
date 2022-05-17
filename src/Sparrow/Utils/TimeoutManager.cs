//-----------------------------------------------------------------------
// <copyright company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;

namespace Sparrow.Utils
{
    public static class TimeoutManager
    {
        private static readonly ConcurrentDictionary<uint, TimerTaskHolder> Values = new ConcurrentDictionary<uint, TimerTaskHolder>();
        private static readonly Task InfiniteTask = new TaskCompletionSource<object>(TaskCreationOptions.RunContinuationsAsynchronously).Task;

        private class TimerTaskHolder  : IDisposable
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

            var value = GetHolderForDuration(duration);

            using (token.Register(value.TimerCallback, null))
            {
                var sp = Stopwatch.StartNew();
                await value.NextTask.ConfigureAwait(false);
                token.ThrowIfCancellationRequested();

                var step = duration / 8;

                if (sp.ElapsedMilliseconds >= (duration - step))
                    return;

                value = GetHolderForDuration(step);

                do
                {
                    token.ThrowIfCancellationRequested();
                    await value.NextTask.ConfigureAwait(false);
                } while (sp.ElapsedMilliseconds < (duration - step));
            }
        }

        private static void ThrowOutOfRange()
        {
            throw new ArgumentOutOfRangeException("time");
        }

        private static TimerTaskHolder GetHolderForDuration(uint duration)
        {
            if (Values.TryGetValue(duration, out var value) == false)
            {
                value = Values.GetOrAdd(duration, d => new TimerTaskHolder(d));
            }
            return value;
        }

        public static async Task WaitFor(TimeSpan duration, CancellationToken token = default)
        {
            if (duration == TimeSpan.Zero)
                return;

            if (token.IsCancellationRequested)
            {
                return;
            }

            Task task;
            // ReSharper disable once ConvertIfStatementToConditionalTernaryExpression
            if (duration != Timeout.InfiniteTimeSpan)
                task = WaitForInternal(duration, token);
            else
                task = InfiniteTask;
            
            if (token == CancellationToken.None || token.CanBeCanceled == false)
            {
                await task.ConfigureAwait(false);
                return;
            }

            var onCancel = new TaskCompletionSource<object>(TaskCreationOptions.RunContinuationsAsynchronously);
            using (token.Register(tcs => onCancel.TrySetCanceled(), onCancel))
            {
                await Task.WhenAny(task, onCancel.Task).ConfigureAwait(false);
            }
        }
    }
}
