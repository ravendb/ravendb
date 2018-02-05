// -----------------------------------------------------------------------
//  <copyright file="MetricCounters.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using Raven.Server.Utils.Metrics;

using Sparrow.Json.Parsing;
using Sparrow.Utils;

namespace Raven.Server.Utils
{
    public class MetricCounters 
    {
        public readonly RequestCounters Requests = new RequestCounters();

        public readonly DocCounters Docs = new DocCounters();

        public readonly AttachmentCounters Attachments = new AttachmentCounters();

        public readonly MapIndexCounters MapIndexes = new MapIndexCounters();

        public readonly MapReduceIndexCounters MapReduceIndexes = new MapReduceIndexCounters();

        public readonly SqlReplicationCounters SqlReplications = new SqlReplicationCounters();

        public MetricCounters()
        {
            CreateNew();
        }


        public void Reset()
        {
            CreateNew();
        }

        private void CreateNew()
        {
            Requests.RequestsPerSec = new MeterMetric();
            Docs.PutsPerSec = new MeterMetric();
            Docs.BytesPutsPerSec = new MeterMetric();
            MapIndexes.IndexedPerSec = new MeterMetric();
            MapReduceIndexes.MappedPerSec = new MeterMetric();
            MapReduceIndexes.ReducedPerSec = new MeterMetric();
            SqlReplications.BatchSize = new MeterMetric();
            Attachments.PutsPerSec = new MeterMetric();
            Attachments.BytesPutsPerSec = new MeterMetric();
        }

        public class RequestCounters
        {
            public MeterMetric RequestsPerSec { get; internal set; }

            public long ConcurrentRequestsCount;
        }

        public class DocCounters
        {
            public MeterMetric PutsPerSec { get; internal set; }
            public MeterMetric BytesPutsPerSec { get; internal set; }
        }

        public class AttachmentCounters
        {
            public MeterMetric PutsPerSec { get; internal set; }
            public MeterMetric BytesPutsPerSec { get; internal set; }
        }

        public class MapIndexCounters
        {
            public MeterMetric IndexedPerSec { get; internal set; }
        }

        public class MapReduceIndexCounters
        {
            public MeterMetric MappedPerSec { get; internal set; }
            public MeterMetric ReducedPerSec { get; internal set; }
        }

        public class SqlReplicationCounters
        {
            public MeterMetric BatchSize { get; internal set; }
        }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(Requests)] = new DynamicJsonValue
                {
                    [nameof(Requests.RequestsPerSec)] = Requests.RequestsPerSec.CreateMeterData(),
                    [nameof(Requests.ConcurrentRequestsCount)] = Requests.ConcurrentRequestsCount
                },
                [nameof(Docs)] = new DynamicJsonValue
                {
                    [nameof(Docs.BytesPutsPerSec)] = Docs.BytesPutsPerSec.CreateMeterData(),
                    [nameof(Docs.PutsPerSec)] = Docs.PutsPerSec.CreateMeterData()
                },
                [nameof(Attachments)] = new DynamicJsonValue
                {
                    [nameof(Attachments.BytesPutsPerSec)] = Attachments.BytesPutsPerSec.CreateMeterData(),
                    [nameof(Attachments.PutsPerSec)] = Attachments.PutsPerSec.CreateMeterData()
                },
                [nameof(MapIndexes)] = new DynamicJsonValue
                {
                    [nameof(MapIndexes.IndexedPerSec)] = MapIndexes.IndexedPerSec.CreateMeterData()
                },
                [nameof(MapReduceIndexes)] = new DynamicJsonValue
                {
                    [nameof(MapReduceIndexes.MappedPerSec)] = MapReduceIndexes.MappedPerSec.CreateMeterData(),
                    [nameof(MapReduceIndexes.ReducedPerSec)] = MapReduceIndexes.ReducedPerSec.CreateMeterData()
                }
            };
        }
    }

    public static class MetricsExtentions
    {
        public static void SetMinimalHumaneMeterData(this MeterMetric self, string name, DynamicJsonValue obj)
        {
            obj["HumaneTotal" + name] = Sizes.Humane(self.Count);
            obj["Humane" + name + "Rate"] = Sizes.Humane((long)self.OneMinuteRate);
        }

        public static void SetMinimalMeterData(this MeterMetric self, string name, DynamicJsonValue obj)
        {
            obj["Total" + name] = self.Count;
            obj[name + "Rate"] = Math.Round(self.OneMinuteRate, 2);
        }
    }
}
