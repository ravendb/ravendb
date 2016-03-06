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
using Raven.Server.Json.Parsing;
using Raven.Server.Utils.Metrics;


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
        public MeterMetric RequestsMeter { get; private set; }

        public MeterMetric RequestsPerSecondCounter { get; private set; }
        public MeterMetric DocPutsPerSecond { get; set; }
        public MeterMetric IndexedPerSecond { get; private set; }

        public long ConcurrentRequestsCount;
        private readonly ActionScheduler _actionScheduler;
        
        public MetricsCountersManager(ActionScheduler actionScheduler)
        {
            _actionScheduler = actionScheduler;
            RequestsMeter = new MeterMetric(_actionScheduler);

            RequestsPerSecondCounter = new MeterMetric(_actionScheduler);

            DocPutsPerSecond = new MeterMetric(_actionScheduler);
            
            IndexedPerSecond = new MeterMetric(_actionScheduler);

        }

        public void Dispose()
        {
            RequestsMeter.Dispose();
            RequestsPerSecondCounter.Dispose();
            DocPutsPerSecond.Dispose();
            IndexedPerSecond.Dispose();
        }
    }

    public static class MetricsExtentions
    {
        public static DynamicJsonValue CreateMetricsStatsJsonValue(this MetricsCountersManager self)
        {
            var metricsStatsJsonValue = new DynamicJsonValue
            {
                ["DocsPerSecond"] = self.DocPutsPerSecond.CreateMeterDataJsonValue(),
                ["IndexedPerSecond"] = self.DocPutsPerSecond.CreateMeterDataJsonValue(),
                ["RequestDuationMetric"] = self.DocPutsPerSecond.CreateMeterDataJsonValue(),
                ["RequestsMeter "] = self.RequestsMeter.CreateMeterDataJsonValue(),
                ["RequestsPerSecondCounter"] = self.RequestsPerSecondCounter.CreateMeterDataJsonValue(),
                ["ConcurrentRequestsCount"] = self.ConcurrentRequestsCount,

            };
            return metricsStatsJsonValue;
        }

        public static DynamicJsonValue CreateMeterDataJsonValue(this MeterMetric self)
        {
            var meterValue = self;
            
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
