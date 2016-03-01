/*
// -----------------------------------------------------------------------
//  <copyright file="MetricsCountersManager.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using Raven.Abstractions.Data;
using Raven.Abstractions.Replication;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Metrics;
using Metrics.Core;
using Metrics.MetricData;
using Metrics.Sampling;
using Metrics.Utils;
using Raven.Server.Config.Settings;
using Sparrow.Collections;
using TimeUnit = Metrics.TimeUnit;


namespace Raven.Database.Util
{
    public class CustomScheduler : Scheduler
    {

        public class ScheduledMetricsTask
        {
            private readonly CancellationToken _token;
            private readonly int _cycleLengthInMiliseconds;
            private Task _task;
            private ConcurrentSet<Action> _actions = new ConcurrentSet<Action>();

            public int MilisecondsLeftInCycle;

            public ScheduledMetricsTask(CancellationToken token, int cycleLengthInMiliseconds = 1000)
            {
                _token = token;
                _cycleLengthInMiliseconds = cycleLengthInMiliseconds;
            }

            public void AddOperation(Action operation)
            {
                _actions.Add(operation);
            }

            public void Start()
            {
                _task = new Task(TaskWork,_token);
                _task.Start();
            }

            public void TaskWork()
            {
                var curTickCount = Environment.TickCount;

                while (_token.IsCancellationRequested)
                {
                    foreach (var action in _actions)
                    {
                        action();
                    }
                    curTickCount = Environment.TickCount;
                    MilisecondsLeftInCycle = _cycleLengthInMiliseconds - curTickCount;
                    if (MilisecondsLeftInCycle > 0)
                        Task.Delay(MilisecondsLeftInCycle,_token);
                }
            }
        }

        private ConcurrentSet<ScheduledMetricsTask> _metricsTasks = new ConcurrentSet<ScheduledMetricsTask>(); 
        private CancellationTokenSource _cts = new CancellationTokenSource();


        public void Dispose()
        {
            _cts.Cancel();
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="interval">In thing implementation, we use 1000 miliseconds as the time interval</param>
        /// <param name="action">The tick action</param>
        public void Start(TimeSpan interval, Action action)
        {            
            if (_metricsTasks.Count == 0)
            {
                _metricsTasks.Add(new ScheduledMetricsTask(_cts.Token));
            }
            _metricsTasks.Last().AddOperation(action);
        }

        public void Start(TimeSpan interval, Action<CancellationToken> action)
        {
            throw new NotImplementedException();
        }

        public void Start(TimeSpan interval, Func<Task> action)
        {
            throw new NotImplementedException();
        }

        public void Start(TimeSpan interval, Func<CancellationToken, Task> action)
        {
            throw new NotImplementedException();
        }

        public void Stop()
        {
            throw new NotImplementedException();
        }
    }
    public class CustomMetricsBuilder : MetricsBuilder
    {
        public MetricValueProvider<double> BuildGauge(string name, Unit unit, Func<double> valueProvider)
        {
            return new FunctionGauge(valueProvider);
        }

        public CounterImplementation BuildCounter(string name, Unit unit)
        {
            return new CounterMetric();
        }

        public MeterImplementation BuildMeter(string name, Unit unit, TimeUnit rateUnit)
        {
            return new MeterMetric(
                Clock.Default, );
        }

        public HistogramImplementation BuildHistogram(string name, Unit unit, SamplingType samplingType)
        {
            throw new NotImplementedException();
        }

        public HistogramImplementation BuildHistogram(string name, Unit unit, Reservoir reservoir)
        {
            throw new NotImplementedException();
        }

        public TimerImplementation BuildTimer(string name, Unit unit, TimeUnit rateUnit, TimeUnit durationUnit,
            SamplingType samplingType)
        {
            return new TimerMetric(samplingType);
        }

        public TimerImplementation BuildTimer(string name, Unit unit, TimeUnit rateUnit, TimeUnit durationUnit,
            HistogramImplementation histogram)
        {
            return new TimerMetric(histogram);
        }

        public TimerImplementation BuildTimer(string name, Unit unit, TimeUnit rateUnit, TimeUnit durationUnit, Reservoir reservoir)
        {
            return new TimerMetric(reservoir);
        }
    }

    public class CustomMetricsContext : BaseMetricsContext
    {

        

        /*
        public DefaultMetricsContext(string context)
                    : base(context, new DefaultMetricsRegistry(), new DefaultMetricsBuilder(), () => Clock.Default.UTCDateTime)
                { }

                protected override MetricsContext CreateChildContextInstance(string contextName)
                {
                    return new DefaultMetricsContext(contextName);
                }
        #1#
        public CustomMetricsContext(string context, new DefaultMetricsRegistry(), MetricsBuilder metricsBuilder, Func<DateTime> timestampProvider) : base(context, registry, metricsBuilder, timestampProvider)
        {
        }

        protected override MetricsContext CreateChildContextInstance(string contextName)
        {
            return new CustomMetricsContext(contextName,base.);
        }
    }
    
    public class MetricsCountersManager : IDisposable
    {
        private static int _counter = 0;
        
        public Histogram StaleIndexMaps { get; private set; }

        public Histogram StaleIndexReduces { get; private set; }

        public Histogram RequestDuationMetric { get; private set; }
        
        public Meter DocsPerSecond { get; set; }

        public Meter FilesPerSecond { get; set; }

        public Meter IndexedPerSecond { get; private set; }

        public Meter ReducedPerSecond { get; private set; }

        public Meter ConcurrentRequests { get; private set; }

        public Meter RequestsMeter { get; private set; }
        public Meter RequestsPerSecondCounter { get; private set; }

        public long ConcurrentRequestsCount;
        private MetricsContext _metricsContext;


        public MetricsCountersManager()
        {
            var contextName = $"Context Number {Interlocked.Increment(ref _counter)}";
            _metricsContext = Metric.Context(contextName);
            
            StaleIndexMaps = _metricsContext.Histogram("stale index maps",Unit.Calls,SamplingType.SlidingWindow);

            StaleIndexReduces = _metricsContext.Histogram("stale index reduces", Unit.Calls,SamplingType.SlidingWindow);

            ConcurrentRequests = _metricsContext.Meter("Concurrent Requests Meter",Unit.Calls,TimeUnit.Seconds);

            RequestDuationMetric = _metricsContext.Histogram("req duration", Unit.Calls, SamplingType.SlidingWindow);

            DocsPerSecond = _metricsContext.Meter("Docs Per Second Counter", Unit.Calls, TimeUnit.Seconds);

            FilesPerSecond = _metricsContext.Meter("Files Per Second Counter", Unit.Calls, TimeUnit.Seconds);

            RequestsPerSecondCounter = _metricsContext.Meter("Files Per Second Counter", Unit.Calls, TimeUnit.Seconds);

            ReducedPerSecond = 


            ReducedPerSecond = dbMetrics.TimedCounter("metrics", "reduces/sec", "Reduced Per Second Counter");
            IndexedPerSecond = dbMetrics.TimedCounter("metrics", "indexed/sec", "Index Per Second Counter");

        }

        public void AddGauge<T>(Type type, string name, Func<T> function)
        {
            dbMetrics.Gauge(type, name, function);
        }

        public Metrics DbMetrics
        {
            get { return dbMetrics; }
        }

        public Dictionary<string, Dictionary<string, string>> Gauges
        {
            get
            {
                return dbMetrics
                    .All
                    .Where(x => x.Value is GaugeMetric)
                    .GroupBy(x => x.Key.Context)
                    .ToDictionary(x => x.Key, x => x.ToDictionary(y => y.Key.Name, y => ((GaugeMetric)y.Value).ValueAsString));
            }
        }

        public void Dispose()
        {
            dbMetrics.Dispose();
        }
    }
}
*/
