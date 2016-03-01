using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;

namespace Metrics.Utils
{
    /// <summary>
    /// Utility class to schedule an Action to be executed repeatedly according to the interval.
    /// </summary>
    /// <remarks>
    /// The scheduling code is inspired form Daniel Crenna's metrics port
    /// https://github.com/danielcrenna/metrics-net/blob/master/src/metrics/Reporting/ReporterBase.cs
    /// </remarks>
    public sealed class ActionScheduler : Scheduler
    {
        private CancellationTokenSource token = new CancellationTokenSource();

        private Task _schedulerTask;
        ConcurrentBag<ScheduledAction> _scheduledActions = new ConcurrentBag<ScheduledAction>();

        public class ScheduledAction
        {
            public long IntervalInNanoSeconds;
            public Action Action;
            public long LastCalledInNanoSeconds;
        }

        private int runs = 0;
        public ActionScheduler(int tickIntervalInNanoseconds)
        {
            _schedulerTask = Task.Run(() =>
            {
                var sp = Stopwatch.StartNew();
                while (token.IsCancellationRequested == false)
                {
                    runs++;
                    sp.Restart();
                    foreach (var scheduledAction in _scheduledActions)
                    {
                        var sinceLastCalled = Clock.Nanoseconds - scheduledAction.LastCalledInNanoSeconds ;
                        var deltaFromInterval = sinceLastCalled - scheduledAction.IntervalInNanoSeconds;
                        if (deltaFromInterval >= 0 || -deltaFromInterval< scheduledAction.IntervalInNanoSeconds*0.05)
                        {
                            try
                            {
                                scheduledAction.Action();
                            }
                            catch (Exception e)
                            {
                                Console.WriteLine(e);// todo: remove this
                            }
                            scheduledAction.LastCalledInNanoSeconds = Clock.Nanoseconds;
                        }
                    }
                    var ellapsed = sp.ElapsedTicks*Clock.FrequencyFactor;
                    if (ellapsed < tickIntervalInNanoseconds)
                    {
                        var millisecondsDelay = (int)(tickIntervalInNanoseconds - ellapsed) / Clock.NANOSECONDS_IN_MILISECOND;
                        //await Task.Delay(millisecondsDelay);
                        Thread.Sleep(millisecondsDelay);
                    }
                }
            });
        }
        public void Start(TimeSpan interval, Action action)
        {
            _scheduledActions.Add(new ScheduledAction
            {
                Action = action,
                IntervalInNanoSeconds = (long)interval.TotalMilliseconds*Clock.NANOSECONDS_IN_MILISECOND,
                LastCalledInNanoSeconds = Clock.Nanoseconds
            });
        }

        public void Stop()
        {
            if (this.token != null)
            {
                token.Cancel();
            }
        }

        public void Dispose()
        {
            if (this.token != null)
            {
                this.token.Cancel();
                this.token.Dispose();
            }
        }
    }
}
