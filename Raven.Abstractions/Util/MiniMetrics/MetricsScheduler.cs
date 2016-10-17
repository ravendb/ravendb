using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Sparrow.Collections;

namespace Raven.Abstractions.Util.MiniMetrics
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
        public static readonly MetricsScheduler Instance = new MetricsScheduler();

        static MetricsScheduler() { }

        private readonly ConcurrentSet<MeterMetric> _scheduledActions =
            new ConcurrentSet<MeterMetric>();

        private MetricsScheduler()
        {
            perSecondCounterMetricsTask = CreateTask(1000, _scheduledActions);
        }

        private Task perSecondCounterMetricsTask;
        private Task meterMetricsTask;
      
        private readonly CancellationTokenSource cts = new CancellationTokenSource();

        public void StartTickingMetric(MeterMetric meterMetric)
        {
            _scheduledActions.Add(meterMetric);
        }

        public void StopTickingMetric(MeterMetric meterMetric)
        {
            _scheduledActions.TryRemove(meterMetric);
        }

        private Task CreateTask(int baseInterval, ConcurrentSet<MeterMetric> concurrentSet)
        {
            return Task.Run(async () =>
            {
                long elapsed = 0;

                while (true)
                {
                    try
                    {
                        if (cts.IsCancellationRequested)
                            return;

                        var milliseconds = baseInterval - elapsed;
                        if (milliseconds > 0)
                        {
                            await Task.Delay(TimeSpan.FromMilliseconds(milliseconds), cts.Token).ConfigureAwait(false);
                        }

                        var sp = Stopwatch.StartNew();
                        foreach (var ticker in concurrentSet)
                        {
                            if (cts.IsCancellationRequested)
                                return;

                            ticker.Tick();
                        }
                        elapsed = sp.ElapsedMilliseconds;
                    }
                    catch (TaskCanceledException)
                    {
                        return;
                    }
                }
            }, cts.Token);
        }

        public void Dispose()
        {
            cts.Cancel();
        }
    }
}
