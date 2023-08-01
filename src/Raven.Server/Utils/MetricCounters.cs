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
    public sealed class MetricCounters 
    {
        public readonly RequestCounters Requests = new RequestCounters();

        public readonly DocCounters Docs = new DocCounters();

        public readonly AttachmentCounters Attachments = new AttachmentCounters();

        public readonly CounterCounters Counters = new CounterCounters();

        public readonly TimeSeriesCounters TimeSeries = new TimeSeriesCounters();

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
            Requests.AverageDuration = new Ewma(1 - Math.Exp(-1.0 / 60 / 5), 1);

            MapIndexes.IndexedPerSec = new MeterMetric();
            MapReduceIndexes.MappedPerSec = new MeterMetric();
            MapReduceIndexes.ReducedPerSec = new MeterMetric();
            SqlReplications.BatchSize = new MeterMetric();

            Docs.Initialize();
            Attachments.Initialize();
            Counters.Initialize();
            TimeSeries.Initialize();
        }

        public sealed class RequestCounters
        {
            public MeterMetric RequestsPerSec { get; internal set; }
            
            public Ewma AverageDuration { get; internal set; }

            public long ConcurrentRequestsCount;

            public void UpdateDuration(long value)
            {
                AverageDuration.Update(value);
                AverageDuration.Tick();
            }
        }

        public abstract class MetricsWritesBase : IDynamicJson
        {
            public MeterMetric PutsPerSec { get; internal set; }

            public MeterMetric BytesPutsPerSec { get; internal set; }

            public void Initialize()
            {
                PutsPerSec = new MeterMetric();
                BytesPutsPerSec = new MeterMetric();
            }

            public DynamicJsonValue ToJson()
            {
                return new DynamicJsonValue
                {
                    [nameof(PutsPerSec)] = PutsPerSec.CreateMeterData(),
                    [nameof(BytesPutsPerSec)] = BytesPutsPerSec.CreateMeterData()
                };
            }
        }

        public sealed class DocCounters : MetricsWritesBase
        {
        }

        public sealed class AttachmentCounters : MetricsWritesBase
        {
        }

        public sealed class CounterCounters : MetricsWritesBase
        {
        }

        public sealed class TimeSeriesCounters : MetricsWritesBase
        {
        }

        public sealed class MapIndexCounters
        {
            public MeterMetric IndexedPerSec { get; internal set; }
        }

        public sealed class MapReduceIndexCounters
        {
            public MeterMetric MappedPerSec { get; internal set; }
            public MeterMetric ReducedPerSec { get; internal set; }
        }

        public sealed class SqlReplicationCounters
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
                    [nameof(Requests.ConcurrentRequestsCount)] = Requests.ConcurrentRequestsCount,
                    [nameof(Requests.AverageDuration)] = Requests.AverageDuration.GetRate()
                },
                [nameof(Docs)] = Docs,
                [nameof(Attachments)] = Attachments,
                [nameof(Counters)] = Counters,
                [nameof(TimeSeries)] = TimeSeries,
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

        public DynamicJsonValue ToJsonForGetPuts(bool empty = true)
        {
            return new DynamicJsonValue
            {
                [nameof(Docs)] = new DynamicJsonValue
                {
                    [nameof(Docs.PutsPerSec)] = Docs.PutsPerSec.CreateMeterData(true, empty)
                },
                [nameof(Attachments)] = new DynamicJsonValue
                {
                    [nameof(Attachments.PutsPerSec)] = Attachments.PutsPerSec.CreateMeterData(true, empty)
                },
                [nameof(Counters)] = new DynamicJsonValue
                {
                    [nameof(Counters.PutsPerSec)] = Counters.PutsPerSec.CreateMeterData(true, empty)
                },
                [nameof(TimeSeries)] = new DynamicJsonValue
                {
                    [nameof(TimeSeries.PutsPerSec)] = TimeSeries.PutsPerSec.CreateMeterData(true, empty)
                }
            };
        }

        public DynamicJsonValue ToJsonForGetBytes(bool empty = true)
        {
            return new DynamicJsonValue
            {
                [nameof(Docs)] = new DynamicJsonValue
                {
                    [nameof(Docs.BytesPutsPerSec)] = Docs.BytesPutsPerSec.CreateMeterData(true, empty)
                },
                [nameof(Attachments)] = new DynamicJsonValue
                {
                    [nameof(Attachments.BytesPutsPerSec)] = Attachments.BytesPutsPerSec.CreateMeterData(true, empty)
                },
                [nameof(Counters)] = new DynamicJsonValue
                {
                    [nameof(Counters.BytesPutsPerSec)] = Counters.BytesPutsPerSec.CreateMeterData(true, empty)
                },
                [nameof(TimeSeries)] = new DynamicJsonValue
                {
                    [nameof(TimeSeries.BytesPutsPerSec)] = TimeSeries.BytesPutsPerSec.CreateMeterData(true, empty)
                }
            };
        }
    }

    public static class MetricsExtensions
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
