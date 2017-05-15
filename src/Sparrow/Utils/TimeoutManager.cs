//-----------------------------------------------------------------------
// <copyright company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Sparrow.Collections.LockFree;

namespace Sparrow.Utils
{
    public static class TimeoutManager
    {
        private static readonly ConcurrentDictionary<uint, TimerTaskHolder> Values 
            = new ConcurrentDictionary<uint, TimerTaskHolder>(NumericEqualityComparer.Instance);

        private class TimerTaskHolder  : IDisposable
        {
            private TaskCompletionSource<object> _nextTimeout;
            private readonly Timer _timer;

            private void TimerCallback(object state)
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
                var period = TimeSpan.FromMilliseconds(timeout);
                // TODO: Use the ctor which take uint once it is available.
                // https://github.com/dotnet/coreclr/blob/master/src/mscorlib/src/System/Threading/Timer.cs#L710
                // https://github.com/dotnet/coreclr/blob/c55f023f542e63e93a300752432de7bcc4104b3b/src/mscorlib/src/System/Threading/Timer.cs#L710
                _timer = new Timer(TimerCallback, null, period, period);
            }

            public void Dispose()
            {
                _timer?.Dispose();
            }
        }

        public static async Task WaitFor(uint duration)
        {
            var mod = duration % 50;
            if (mod != 0)
                duration += 50 - mod;

            var value = GetHolderForDuration(duration);

            var sp = Stopwatch.StartNew();
            await value.NextTask;

            var step = duration / 8;

            if (sp.ElapsedMilliseconds >= (duration - step))
                return;

            value = GetHolderForDuration(step);

            do
            {
                await value.NextTask;
            } while (sp.ElapsedMilliseconds < (duration - step));


        }

        private static TimerTaskHolder GetHolderForDuration(uint duration)
        {
            if (Values.TryGetValue(duration, out var value) == false)
            {
                value = Values.GetOrAdd(duration, d => new TimerTaskHolder(d));
            }
            return value;
        }

        public static async Task WaitFor(uint duration, CancellationToken token)
        {
            token.ThrowIfCancellationRequested();
            // ReSharper disable once MethodSupportsCancellation
            var task = WaitFor(duration);
            if (token == CancellationToken.None || token.CanBeCanceled == false)
            {
                await task;
                return;
            }

            var onCancel = new TaskCompletionSource<object>(TaskCreationOptions.RunContinuationsAsynchronously);
            using (token.Register(tcs => onCancel.TrySetCanceled(), onCancel))
            {
                await Task.WhenAny(task, onCancel.Task);
            }
        }
    }
}
