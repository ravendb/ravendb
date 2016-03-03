// -----------------------------------------------------------------------
//  <copyright file="MetricsCountersManager.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using Raven.Abstractions.Data;
using System.Linq;
using Metrics.MetricData;
using Metrics.Utils;
using Raven.Server.Json.Parsing;
using Raven.Server.Utils.Metrics.Core;


namespace Raven.Database.Util
{
    public class MetricsStats
    {
        public double DocsPerSecond { get; set; }
        public double IndexedPerSecond { get; set; }
        public HistogramData RequestDuationMetric { get; set; }
        public MeterData RequestsMeter { get; set; }
        public double RequestsPerSecondCounter { get; set; }
        public Dictionary<string, double> Gauges { get; set; }
    }

    public class MetricsCountersManager : IDisposable
    {
        public HistogramMetric RequestDuationMetric { get; private set; }
        public MeterMetric RequestsMeter { get; private set; }

        public PerSecondMetric RequestsPerSecondCounter { get; private set; }
        public PerSecondMetric DocPutsPerSecond { get; set; }
        public PerSecondMetric IndexedPerSecond { get; private set; }

        public long ConcurrentRequestsCount;
        private readonly ActionScheduler _actionScheduler;

        public readonly ConcurrentDictionary<string, FunctionGauge> Gauges = new ConcurrentDictionary<string, FunctionGauge>();
        public MetricsCountersManager()
        {
            _actionScheduler = new ActionScheduler(Clock.NANOSECONDS_IN_SECOND);
            RequestDuationMetric = new HistogramMetric("req duration", _actionScheduler);
            RequestsMeter = new MeterMetric("Requests",_actionScheduler);

            RequestsPerSecondCounter = new PerSecondMetric("Files Per Second Counter", _actionScheduler);

            DocPutsPerSecond = new PerSecondMetric("Docs Per Second Counter", _actionScheduler);
            
            IndexedPerSecond = new PerSecondMetric("Indexed Per Second Counter", _actionScheduler);

        }

        public void AddGauge(string name, Func<double> function)
        {
            Gauges.TryAdd(name, new FunctionGauge(function));
            
        }

        public Dictionary<string, double> GaugesValues
        {
            get
            {
                return Gauges.ToDictionary(x => x.Key, x => x.Value.Value);
            }
        }

        public void Dispose()
        {
            _actionScheduler.Dispose();
            RequestDuationMetric.Dispose();
            
            RequestsMeter.Dispose();
            RequestsPerSecondCounter.Dispose();
            DocPutsPerSecond.Dispose();
            IndexedPerSecond.Dispose();
            Gauges.Clear();
        }
    }

    public static class MetricsExtentions
    {
        public static DynamicJsonValue CreateMetricsStatsJsonValue(this MetricsCountersManager self)
        {
            var metricsStatsJsonValue = new DynamicJsonValue
            {
                ["DocsPerSecond"] = self.DocPutsPerSecond.Value,
                ["IndexedPerSecond"] = self.DocPutsPerSecond.Value,
                ["RequestDuationMetric"] = self.DocPutsPerSecond.Value,
                ["RequestsMeter "] = self.RequestsMeter.CreateMeterDataJsonValue(),
                ["RequestsPerSecondCounter"] = self.RequestsPerSecondCounter.Value,
                
                ["ConcurrentRequestsCount"] = self.ConcurrentRequestsCount,

            };

            var gaugesDynamicJsonArray = new DynamicJsonArray { };
            foreach (var x in self.Gauges)
            {
                gaugesDynamicJsonArray.Add(
                    new DynamicJsonArray
                    {
                        x.Key,
                        x.Value.Value
                    });
            }
            
            metricsStatsJsonValue["Gauges"] = gaugesDynamicJsonArray;
            return metricsStatsJsonValue;
        }

        public static DynamicJsonValue CreateHistogramDataJsonValue(this HistogramMetric self)
        {
            var histogramValue = self.Value;
            return new DynamicJsonValue
            {
                ["Counter"] = histogramValue.Count,
                ["Max"] = histogramValue.Max,
                ["Mean"] = histogramValue.Mean,
                ["Min"] = histogramValue.Min,
                ["Stdev"] = histogramValue.StdDev,
                ["Percentiles"] = new DynamicJsonArray()
                {
                    new DynamicJsonArray
                    {
                        "50%",histogramValue.Median
                    },
                    new DynamicJsonArray
                    {
                        "75%", histogramValue.Percentile75
                    },
                    new DynamicJsonArray
                    {
                        "95%", histogramValue.Percentile95
                    },
                    new DynamicJsonArray
                    {
                        "98%", histogramValue.Percentile98
                    },
                    new DynamicJsonArray
                    {
                        "99%", histogramValue.Percentile99
                    },
                    new DynamicJsonArray
                    {
                        "99.9%", histogramValue.Percentile999
                    }
                } 
            };
        }

        

        public static DynamicJsonValue CreateMeterDataJsonValue(this MeterMetric self)
        {
            var meterValue = self.Value;
            return new DynamicJsonValue
            {
                ["Count"] = meterValue.Count,
                ["FifteenMinuteRate"] = Math.Round(meterValue.FifteenMinuteRate, 3),
                ["FiveMinuteRate"] = Math.Round(meterValue.FiveMinuteRate, 3),
                ["MeanRate"] = Math.Round(meterValue.MeanRate, 3),
                ["OneMinuteRate"] = Math.Round(meterValue.OneMinuteRate, 3),
            };
        }
    }
}
