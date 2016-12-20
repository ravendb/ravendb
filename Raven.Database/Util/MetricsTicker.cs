using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Raven.Imports.metrics.Core;
using Sparrow.Collections;

namespace Raven.Database.Util
{
    public sealed class MetricsTicker : IDisposable
    {
        public static readonly MetricsTicker Instance = new MetricsTicker();

        static MetricsTicker() { }

        private MetricsTicker()
        {
            perSecondCounterMetricsTask = CreateTask(1000, oneSecondIntervalMetrics);
            fiveSecondsTickCounterMetricsTask = CreateTask(5000, fiveSecondsTickIntervalMetrics);
            fifteenSecondsTickCounterMetricsTask = CreateTask(15000, fifteenSecondsIntervalMeterMetrics);
        }

        private Task perSecondCounterMetricsTask;
        private Task fiveSecondsTickCounterMetricsTask;
        private Task fifteenSecondsTickCounterMetricsTask;

        private readonly ConcurrentSet<ICounterMetric> oneSecondIntervalMetrics = new ConcurrentSet<ICounterMetric>();
        private readonly ConcurrentSet<ICounterMetric> fiveSecondsTickIntervalMetrics = new ConcurrentSet<ICounterMetric>();
        private readonly ConcurrentSet<ICounterMetric> fifteenSecondsIntervalMeterMetrics = new ConcurrentSet<ICounterMetric>();
        
        private readonly CancellationTokenSource cts = new CancellationTokenSource();

        public void AddPerSecondCounterMetric(ICounterMetric newPerSecondCounterMetric)
        {
            oneSecondIntervalMetrics.Add(newPerSecondCounterMetric);
        }

        public void RemovePerSecondCounterMetric(ICounterMetric perSecondCounterMetricToRemove)
        {
            oneSecondIntervalMetrics.TryRemove(perSecondCounterMetricToRemove);
        }

        public void AddFiveSecondsIntervalMeterMetric(ICounterMetric newMeterMetric)
        {
            fiveSecondsTickIntervalMetrics.Add(newMeterMetric);
        }

        public void RemoveFiveSecondsIntervalMeterMetric(ICounterMetric meterMetricToRemove)
        {
            fiveSecondsTickIntervalMetrics.TryRemove(meterMetricToRemove);
        }

        public void AddFifteenSecondsIntervalMeterMetric(ICounterMetric newMeterMetric)
        {
            fifteenSecondsIntervalMeterMetrics.Add(newMeterMetric);
        }

        public void RemoveFifteenSecondsIntervalMeterMetric(ICounterMetric meterMetricToRemove)
        {
            fifteenSecondsIntervalMeterMetrics.TryRemove(meterMetricToRemove);
        }

        private Task CreateTask(int baseInterval, ConcurrentSet<ICounterMetric> concurrentSet)
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