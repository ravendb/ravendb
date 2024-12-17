//-----------------------------------------------------------------------
// <copyright company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

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

        private const bool ForceTaskDelay = true;

        private static readonly bool UseTaskDelay;

        static TimeoutManager()
        {
            UseTaskDelay = Environment.ProcessorCount <= 2 || ForceTaskDelay;
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

        private static async Task WaitForInternal(TimeSpan time, bool canBeCanceled, CancellationToken token)
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

            using (canBeCanceled ? (IDisposable)token.Register(value.TimerCallback, null) : null)
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

            if (UseTaskDelay)
            {
                return await Task.WhenAny(outer, Task.Delay(duration, token)).ConfigureAwait(false);
            }

            var canBeCanceled = token != CancellationToken.None && token.CanBeCanceled;

            Task task;
            // ReSharper disable once ConvertIfStatementToConditionalTernaryExpression
            if (duration != Timeout.InfiniteTimeSpan)
                task = WaitForInternal(duration, canBeCanceled, token);
            else
                task = InfiniteTask;

            if (canBeCanceled == false)
            {
                return await Task.WhenAny(outer, task).ConfigureAwait(false);
            }

            var onCancel = new TaskCompletionSource<object>(TaskCreationOptions.RunContinuationsAsynchronously);
            using (token.Register(tcs => onCancel.TrySetCanceled(), onCancel))
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

            if (UseTaskDelay)
            {
                await Task.Delay(duration, token).ConfigureAwait(false);
                return;
            }

            var canBeCanceled = token != CancellationToken.None && token.CanBeCanceled;

            Task task;
            // ReSharper disable once ConvertIfStatementToConditionalTernaryExpression
            if (duration != Timeout.InfiniteTimeSpan)
                task = WaitForInternal(duration, canBeCanceled, token);
            else
                task = InfiniteTask;

            if (canBeCanceled == false)
            {
                await task.ConfigureAwait(false);
                return;
            }

            var onCancel = new TaskCompletionSource<object>(TaskCreationOptions.RunContinuationsAsynchronously);
            if (task == InfiniteTask)
            {
                await onCancel.Task.ConfigureAwait(false);
                return;
            }

            using (token.Register(tcs => onCancel.TrySetCanceled(), onCancel))
            {
                await Task.WhenAny(task, onCancel.Task).ConfigureAwait(false);
            }
        }
    }
}
