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
using Raven.Server.Utils.Metrics;
using Sparrow.Json.Parsing;


namespace Raven.Database.Util
{
    public class MetricsCountersManager : IDisposable
    {
        public MeterMetric RequestsMeter { get; private set; }
        public MeterMetric RequestsPerSecondCounter { get; private set; }
        public MeterMetric DocPutsPerSecond { get; set; }
        public MeterMetric IndexedPerSecond { get; private set; }
        public MeterMetric MapReduceMappedPerSecond { get; set; }
        public MeterMetric MapReduceReducedPerSecond { get; set; }

        public long ConcurrentRequestsCount;
        private readonly MetricsScheduler _metricsScheduler;
        
        public MetricsCountersManager(MetricsScheduler metricsScheduler)
        {
            _metricsScheduler = metricsScheduler;
            RequestsMeter = new MeterMetric(_metricsScheduler);

            RequestsPerSecondCounter = new MeterMetric(_metricsScheduler);

            DocPutsPerSecond = new MeterMetric(_metricsScheduler);
            
            IndexedPerSecond = new MeterMetric(_metricsScheduler);
            MapReduceMappedPerSecond = new MeterMetric(_metricsScheduler);
            MapReduceReducedPerSecond = new MeterMetric(_metricsScheduler);
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
                ["MapReduceMappedPerSecond"] = self.MapReduceMappedPerSecond.CreateMeterDataJsonValue(),
                ["MapReduceReducedPerSecond"] = self.MapReduceReducedPerSecond.CreateMeterDataJsonValue(),
                ["ConcurrentRequestsCount"] = self.ConcurrentRequestsCount
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
