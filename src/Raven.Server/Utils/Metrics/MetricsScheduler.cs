using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Threading;
using Raven.Abstractions.Logging;
using Raven.Client.Document.Batches;
using Sparrow.Collections;
using Sparrow.Logging;

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

        private readonly ConcurrentSet<MeterMetric> _scheduledActions =
            new ConcurrentSet<MeterMetric>();

        private readonly Logger _logger;

        public static MetricsScheduler Instance = new MetricsScheduler();

        private MetricsScheduler()
        {
            _logger = LoggerSetup.Instance.GetLogger<MetricsScheduler>("MetricsScheduler");
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
                foreach (var scheduledAction in _scheduledActions)
                {
                    try
                    {
                        scheduledAction.Tick();
                    }
                    catch (Exception e)
                    {
                        if (_logger.IsInfoEnabled)
                            _logger.Info("Error occured during MetricsScheduler ticking of a single action", e);
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
            _scheduledActions.TryAdd(tickable);
        }

        public void StopTickingMetric(MeterMetric tickable)
        {
            _scheduledActions.TryRemove(tickable);
        }

        public void Dispose()
        {
            _done.Set();
            _schedulerThread.Join();
            _done.Dispose();
        }
    }
}
