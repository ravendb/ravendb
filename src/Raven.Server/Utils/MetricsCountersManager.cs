// -----------------------------------------------------------------------
//  <copyright file="MetricsCountersManager.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using Raven.Server.Utils.Metrics;

using Sparrow.Json.Parsing;
using Sparrow.Utils;

namespace Raven.Server.Utils
{
    public class MetricsCountersManager : IDisposable
    {
        public MeterMetric RequestsMeter { get; private set; }

        public MeterMetric DocPutsPerSecond { get; private set; }
        public MeterMetric BytesPutsPerSecond { get; private set; }
        public MeterMetric IndexedPerSecond { get; private set; }
        
        public MeterMetric MapReduceMappedPerSecond { get; private set; }
        public MeterMetric MapReduceReducedPerSecond { get; private set; }
        public MeterMetric SqlReplicationBatchSizeMeter { get; private set; }

        public MeterMetric AttachmentPutsPerSecond { get; private set; }
        public MeterMetric AttachmentBytesPutsPerSecond { get; private set; }

        public long ConcurrentRequestsCount;

        public MetricsCountersManager()
        {
            CreateNew();
        }

        public void Dispose()
        {
            RequestsMeter?.Dispose();
            DocPutsPerSecond?.Dispose();
            BytesPutsPerSecond?.Dispose();
            IndexedPerSecond?.Dispose();
            MapReduceMappedPerSecond?.Dispose();
            MapReduceReducedPerSecond?.Dispose();
            SqlReplicationBatchSizeMeter?.Dispose();
            AttachmentPutsPerSecond?.Dispose();
            AttachmentBytesPutsPerSecond?.Dispose();
        }

        public void Reset()
        {
            Dispose();
            CreateNew();
        }

        private void CreateNew()
        {
            RequestsMeter = new MeterMetric();
            DocPutsPerSecond = new MeterMetric();
            BytesPutsPerSecond = new MeterMetric();
            IndexedPerSecond = new MeterMetric();
            MapReduceMappedPerSecond = new MeterMetric();
            MapReduceReducedPerSecond = new MeterMetric();
            SqlReplicationBatchSizeMeter = new MeterMetric();
            AttachmentPutsPerSecond = new MeterMetric();
            AttachmentBytesPutsPerSecond = new MeterMetric();
        }
    }

    public static class MetricsExtentions
    {
        public static DynamicJsonValue CreateMetricsStatsJsonValue(this MetricsCountersManager self)
        {
            var metricsStatsJsonValue = new DynamicJsonValue
            {
                ["DocPutsPerSecond"] = self.DocPutsPerSecond.CreateMeterData(),
                ["BytesPutsPerSecond"] = self.BytesPutsPerSecond.CreateMeterData(),
                ["IndexedPerSecond"] = self.IndexedPerSecond.CreateMeterData(),

                ["RequestsMeter "] = self.RequestsMeter.CreateMeterData(),
                ["MapReduceMappedPerSecond"] = self.MapReduceMappedPerSecond.CreateMeterData(),
                ["MapReduceReducedPerSecond"] = self.MapReduceReducedPerSecond.CreateMeterData(),
                ["ConcurrentRequestsCount"] = self.ConcurrentRequestsCount,

                ["AttachmentPutsPerSecond"] = self.AttachmentPutsPerSecond.CreateMeterData(),
                ["AttachmentBytesPutsPerSecond"] = self.AttachmentBytesPutsPerSecond.CreateMeterData()
                
            };
            return metricsStatsJsonValue;
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
