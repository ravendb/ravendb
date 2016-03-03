using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Threading;
using Raven.Server.Utils.Metrics.Core;

namespace Metrics.Utils
{
    /// <summary>
    /// Utility class to schedule an Action to be executed repeatedly according to the interval.
    /// </summary>
    /// <remarks>
    /// The scheduling code is inspired form Daniel Crenna's metrics port
    /// https://github.com/danielcrenna/metrics-net/blob/master/src/metrics/Reporting/ReporterBase.cs
    /// </remarks>
    public sealed class ActionScheduler
    {
        private CancellationTokenSource token = new CancellationTokenSource();

        private Thread _schedulerThread;
        ConcurrentDictionary<ITickable, ScheduledAction> _scheduledActions = new ConcurrentDictionary<ITickable, ScheduledAction>();

        public class ScheduledAction
        {
            public long IntervalInNanoSeconds;
            public ITickable Tickable;
            public long LastCalledInNanoSeconds;
        }
        
        public ActionScheduler(int tickIntervalInNanoseconds)
        {
            _tickIntervalInNanoseconds = tickIntervalInNanoseconds;
            _schedulerThread = new Thread(SchedulerTicking);
            _schedulerThread.IsBackground = true;
            _schedulerThread.Name = "MetricsScheduler";
            _schedulerThread.Start();
        }

        public ConcurrentQueue<Exception> Exceptions = new ConcurrentQueue<Exception>();

        private readonly int _tickIntervalInNanoseconds;
        private void SchedulerTicking()
        {
            var sp = Stopwatch.StartNew();
            while (token.IsCancellationRequested == false)
            {
                sp.Restart();
                foreach (var scheduledAction in _scheduledActions.Values)
                {
                    var sinceLastCalled = Clock.Nanoseconds - scheduledAction.LastCalledInNanoSeconds;
                    var deltaFromInterval = sinceLastCalled - scheduledAction.IntervalInNanoSeconds;
                    if (deltaFromInterval >= 0 || -deltaFromInterval < scheduledAction.IntervalInNanoSeconds * 0.05)
                    {
                        try
                        {
                            scheduledAction.Tickable.Tick();
                        }
                        catch (Exception e)
                        {
                            Exception outEx;
                            if (Exceptions.Count > 50)
                                Exceptions.TryDequeue(out outEx);
                            Exceptions.Enqueue(e);
                        }
                        scheduledAction.LastCalledInNanoSeconds = Clock.Nanoseconds;
                    }
                }

                if (token.IsCancellationRequested)
                    break;

                var elapsed = sp.ElapsedTicks * Clock.FrequencyFactor;
                if (elapsed < _tickIntervalInNanoseconds)
                {
                    var millisecondsDelay = (int)(_tickIntervalInNanoseconds - elapsed) / Clock.NANOSECONDS_IN_MILISECOND;
                    Thread.Sleep(millisecondsDelay);
                }
            }
        }

        public void StartTickingMetric(TimeSpan interval, ITickable tickable)
        {
            _scheduledActions.TryAdd(tickable,new ScheduledAction
            {
                Tickable= tickable,
                IntervalInNanoSeconds = (long)interval.TotalMilliseconds*Clock.NANOSECONDS_IN_MILISECOND,
                LastCalledInNanoSeconds = Clock.Nanoseconds
            });
        }

        public void StopTickingMetric(ITickable tickable)
        {
            ScheduledAction action;
            _scheduledActions.
                TryRemove(tickable, out action);
        }

        public void Dispose()
        {
            if (this.token != null)
            {
                this.token.Cancel();
                this.token.Dispose(); 
            }
            _schedulerThread.Join();
        }
    }
}
