// -----------------------------------------------------------------------
//  <copyright file="MetricsCountersManager.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;

using Raven.Server.Utils.Metrics;

using Sparrow.Json.Parsing;

namespace Raven.Server.Utils
{
    public class MetricsCountersManager : IDisposable
    {
        private readonly MetricsScheduler _metricsScheduler;

        public MeterMetric RequestsMeter { get; private set; }

        public MeterMetric RequestsPerSecondCounter { get; private set; }

        public MeterMetric DocPutsPerSecond { get; private set; }
        public MeterMetric IndexedPerSecond { get; private set; }

        public MeterMetric SubscriptionDocsPerSecond { get; set; }

        public MeterMetric MapReduceMappedPerSecond { get; private set; }
        public MeterMetric MapReduceReducedPerSecond { get; private set; }
        public MeterMetric SqlReplicationBatchSizeMeter { get; private set; }
        

        public long ConcurrentRequestsCount;

        public MetricsCountersManager(MetricsScheduler metricsScheduler)
        {
            _metricsScheduler = metricsScheduler;
            CreateNew();
        }

        public void Dispose()
        {
            RequestsMeter?.Dispose();
            RequestsPerSecondCounter?.Dispose();
            DocPutsPerSecond?.Dispose();
            IndexedPerSecond?.Dispose();
            MapReduceMappedPerSecond?.Dispose();
            MapReduceReducedPerSecond?.Dispose();
            SqlReplicationBatchSizeMeter?.Dispose();
            SubscriptionDocsPerSecond?.Dispose();
        }

        public void Reset()
        {
            Dispose();
            CreateNew();
        }

        private void CreateNew()
        {
            RequestsMeter = new MeterMetric(_metricsScheduler);

            RequestsPerSecondCounter = new MeterMetric(_metricsScheduler);

            DocPutsPerSecond = new MeterMetric(_metricsScheduler);
            SubscriptionDocsPerSecond = new MeterMetric(_metricsScheduler);

            IndexedPerSecond = new MeterMetric(_metricsScheduler);
            MapReduceMappedPerSecond = new MeterMetric(_metricsScheduler);
            MapReduceReducedPerSecond = new MeterMetric(_metricsScheduler);

            SqlReplicationBatchSizeMeter = new MeterMetric(_metricsScheduler);
        }
    }

    public static class MetricsExtentions
    {
        public static DynamicJsonValue CreateMetricsStatsJsonValue(this MetricsCountersManager self)
        {
            var metricsStatsJsonValue = new DynamicJsonValue
            {
                ["DocsPerSecond"] = self.DocPutsPerSecond.CreateMeterData(),
                ["IndexedPerSecond"] = self.DocPutsPerSecond.CreateMeterData(),
                ["RequestDuationMetric"] = self.DocPutsPerSecond.CreateMeterData(),
                ["RequestsMeter "] = self.RequestsMeter.CreateMeterData(),
                ["SubscriptionDocsPerSecond"] = self.SubscriptionDocsPerSecond.CreateMeterData(),
                ["RequestsPerSecondCounter"] = self.RequestsPerSecondCounter.CreateMeterData(),
                ["MapReduceMappedPerSecond"] = self.MapReduceMappedPerSecond.CreateMeterData(),
                ["MapReduceReducedPerSecond"] = self.MapReduceReducedPerSecond.CreateMeterData(),
                ["ConcurrentRequestsCount"] = self.ConcurrentRequestsCount,
                
            };
            return metricsStatsJsonValue;
        }

        public static DynamicJsonValue CreateMeterData(this MeterMetric self)
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
