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
        public MeterMetric RequestsMeter { get; private set; }

        public MeterMetric RequestsPerSecondCounter { get; private set; }

        public MeterMetric DocPutsPerSecond { get; private set; }
        public MeterMetric IndexedPerSecond { get; private set; }
        
        public MeterMetric MapReduceMappedPerSecond { get; private set; }
        public MeterMetric MapReduceReducedPerSecond { get; private set; }
        public MeterMetric SqlReplicationBatchSizeMeter { get; private set; }
        

        public long ConcurrentRequestsCount;

        public MetricsCountersManager()
        {
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
        }

        public void Reset()
        {
            Dispose();
            CreateNew();
        }

        private void CreateNew()
        {
            RequestsMeter = new MeterMetric();
            RequestsPerSecondCounter = new MeterMetric();
            DocPutsPerSecond = new MeterMetric();
            IndexedPerSecond = new MeterMetric();
            MapReduceMappedPerSecond = new MeterMetric();
            MapReduceReducedPerSecond = new MeterMetric();
            SqlReplicationBatchSizeMeter = new MeterMetric();
        }
    }

    public static class MetricsExtentions
    {
        public static DynamicJsonValue CreateMetricsStatsJsonValue(this MetricsCountersManager self)
        {
            var metricsStatsJsonValue = new DynamicJsonValue
            {
                ["DocPutsPerSecond"] = self.DocPutsPerSecond.CreateMeterData(),
                ["IndexedPerSecond"] = self.IndexedPerSecond.CreateMeterData(),

                ["RequestsMeter "] = self.RequestsMeter.CreateMeterData(),
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


        public static void SetMinimalHumaneMeterData(this MeterMetric self, string name, DynamicJsonValue obj)
        {
            obj["HumaneTotal" + name] = Sizes.Humane(self.Count);
            obj["Humane" + name + "Rate"] = Sizes.Humane((long)self.OneMinuteRate);
        }

        public static void SetMinimalMeterData(this MeterMetric self, string name, DynamicJsonValue obj)
        {
            obj["Total" + name] = self.Count;
            obj[name + "Rate"] = Math.Round(self.OneMinuteRate,2);
        }
    }
}
