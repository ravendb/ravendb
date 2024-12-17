//-----------------------------------------------------------------------
// <copyright company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Sparrow.Utils.HWT;
using Timeout = System.Threading.Timeout;

namespace Sparrow.Utils
{
    internal static class TimeoutManager
    {
        private static readonly HashedWheelTimer Timer = new(tickDuration: TimeSpan.FromMilliseconds(50), ticksPerWheel: 512, maxPendingTimeouts: 0);

        private static readonly Task InfiniteTask = new TaskCompletionSource<object>(TaskCreationOptions.RunContinuationsAsynchronously).Task;

        private static async Task WaitForInternal(TimeSpan time, bool canBeCanceled, CancellationToken token)
        {
            if (time.TotalMilliseconds < 0)
                ThrowOutOfRange();

            var duration = (int)Math.Min(time.TotalMilliseconds, uint.MaxValue - 45);
            if (duration == 0)
                return;

            var mod = duration % 50;
            if (mod != 0)
            {
                duration += 50 - mod;
            }

            if (canBeCanceled == false)
            {
#pragma warning disable RDB0002
                await Timer.Delay(duration);
#pragma warning restore RDB0002
                return;
            }

            var sp = Stopwatch.StartNew();

            var step = duration / 8;

            do
            {
                token.ThrowIfCancellationRequested();

#pragma warning disable RDB0002
                await Timer.Delay(step);
#pragma warning restore RDB0002
            } while (sp.ElapsedMilliseconds < duration);

            token.ThrowIfCancellationRequested();
        }

        private static void ThrowOutOfRange()
        {
            throw new ArgumentOutOfRangeException("time");
        }

        public static async Task<Task> WaitFor(this Task outer, TimeSpan duration, CancellationToken token = default)
        {
            if (duration == TimeSpan.Zero)
                return Task.CompletedTask;

            if (token.IsCancellationRequested)
            {
                return Task.CompletedTask;
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
