using System;
using System.Diagnostics;
using System.Threading;
using Raven.Server.Logging;
using Sparrow.Collections;
using Sparrow.Logging;
using Sparrow.Server.Logging;

namespace Raven.Server.Utils.Metrics
{
    /// <summary>
    /// Utility class to schedule an Action to be executed repeatedly according to the interval.
    /// </summary>
    /// <remarks>
    /// The scheduling code is inspired form Daniel Crenna's metrics port
    /// https://github.com/danielcrenna/metrics-net/blob/master/src/metrics/Reporting/ReporterBase.cs
    /// </remarks>
    public sealed class MetricsScheduler : IDisposable
    {
        private readonly Thread _schedulerThread;
        private readonly ManualResetEvent _done = new ManualResetEvent(false);

        private readonly ConcurrentSet<WeakReference<MeterMetric>> _scheduledMetricActions = new ConcurrentSet<WeakReference<MeterMetric>>();

        private readonly ConcurrentSet<WeakReference<Ewma>> _scheduledEwmaActions = new ConcurrentSet<WeakReference<Ewma>>();

        private static readonly RavenLogger _logger = RavenLogManager.Instance.GetLoggerForServer<MetricsScheduler>();

        public static readonly MetricsScheduler Instance = new MetricsScheduler();

        private MetricsScheduler()
        {
            _tickIntervalInNanoseconds = Clock.NanosecondsInSecond;
            _schedulerThread = new Thread(SchedulerTicking)
            {
                IsBackground = true,
                Name = "Metrics Scheduler"
            };
            _schedulerThread.Start();
        }

        private readonly int _tickIntervalInNanoseconds;


        private void SchedulerTicking()
        {
            int millisecondsDelay;
            var sp = Stopwatch.StartNew();
            do
            {
                sp.Restart();
                foreach (var scheduledAction in _scheduledMetricActions)
                {
                    try
                    {
                        if (scheduledAction.TryGetTarget(out var target))
                        {
                            target.Tick();
                        }
                        else
                        {
                            _scheduledMetricActions.TryRemove(scheduledAction);
                        }
                    }
                    catch (Exception e)
                    {
                        if (_logger.IsInfoEnabled)
                            _logger.Info("Error occurred during MetricsScheduler ticking of a single Metric action", e);
                    }
                }

                foreach (var scheduledAction in _scheduledEwmaActions)
                {
                    try
                    {
                        if (scheduledAction.TryGetTarget(out var target))
                        {
                            target.Tick();
                        }
                        else
                        {
                            _scheduledEwmaActions.TryRemove(scheduledAction);
                        }
                    }
                    catch (Exception e)
                    {
                        if (_logger.IsInfoEnabled)
                            _logger.Info("Error occurred during MetricsScheduler ticking of a single EWMA action", e);
                    }
                }

                var elapsedNanoseconds = sp.ElapsedTicks * Clock.FrequencyFactor;
                millisecondsDelay = (int)(_tickIntervalInNanoseconds - elapsedNanoseconds) / Clock.NanosecondsInMillisecond;
                if (millisecondsDelay < 0)
                    millisecondsDelay = 0;
            } while (_done.WaitOne(Math.Max(millisecondsDelay, 0)) == false);
        }

        public void StartTickingMetric(MeterMetric tickable)
        {
            _scheduledMetricActions.TryAdd(new WeakReference<MeterMetric>(tickable));
        }

        public void StartTickingEwma(Ewma tickable)
        {
            _scheduledEwmaActions.TryAdd(new WeakReference<Ewma>(tickable));
        }

        public void Dispose()
        {
            _done.Set();
            _schedulerThread.Join();
            _done.Dispose();
        }
    }
}
