// -----------------------------------------------------------------------
//  <copyright file="MetricsCountersManager.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;

using Raven.Bundles.Replication.Tasks;

using metrics;
using metrics.Core;

using System.Linq;
using Raven.Database.Bundles.SqlReplication;

namespace Raven.Database.Util
{
    public class MetricsCountersManager : IDisposable
    {
        readonly Metrics dbMetrics = new Metrics();

        public HistogramMetric StaleIndexMaps { get; private set; }

        public HistogramMetric StaleIndexReduces { get; private set; }

        public HistogramMetric RequestDuationMetric { get; private set; }
        public PerSecondCounterMetric DocsPerSecond { get; private set; }
        public PerSecondCounterMetric FilesPerSecond { get; private set; }

        public PerSecondCounterMetric IndexedPerSecond { get; private set; }

        public PerSecondCounterMetric ReducedPerSecond { get; private set; }

        public MeterMetric ConcurrentRequests { get; private set; }

        public MeterMetric RequestsMeter { get; private set; }
        public PerSecondCounterMetric RequestsPerSecondCounter { get; private set; }

        public ConcurrentDictionary<string, MeterMetric> ReplicationBatchSizeMeter { get; private set; }

        public ConcurrentDictionary<string, MeterMetric> ReplicationDurationMeter { get; private set; }

        public ConcurrentDictionary<string, HistogramMetric> ReplicationBatchSizeHistogram { get; private set; }

        public ConcurrentDictionary<string, HistogramMetric> ReplicationDurationHistogram { get; private set; }

        public SqlReplicationMetricsCountersManager SqlReplicationMetricsCounters { get; private set; }
        
        public MetricsCountersManager()
        {
            StaleIndexMaps = dbMetrics.Histogram("metrics", "stale index maps");

            StaleIndexReduces = dbMetrics.Histogram("metrics", "stale index reduces");

            ConcurrentRequests = dbMetrics.Meter("metrics", "req/sec", "Concurrent Requests Meter", TimeUnit.Seconds);

            RequestDuationMetric = dbMetrics.Histogram("metrics", "req duration");

            DocsPerSecond = dbMetrics.TimedCounter("metrics", "docs/sec", "Docs Per Second Counter");
            FilesPerSecond = dbMetrics.TimedCounter("metrics", "files/sec", "Files Per Second Counter");
            RequestsPerSecondCounter = dbMetrics.TimedCounter("metrics", "req/sec counter", "Requests Per Second");
            ReducedPerSecond = dbMetrics.TimedCounter("metrics", "reduces/sec", "Reduced Per Second Counter");
            IndexedPerSecond = dbMetrics.TimedCounter("metrics", "indexed/sec", "Index Per Second Counter");
            ReplicationBatchSizeMeter = new ConcurrentDictionary<string, MeterMetric>();
            ReplicationDurationMeter = new ConcurrentDictionary<string, MeterMetric>();
            ReplicationBatchSizeHistogram = new ConcurrentDictionary<string, HistogramMetric>();
            ReplicationDurationHistogram = new ConcurrentDictionary<string, HistogramMetric>();
            SqlReplicationMetricsCounters = new SqlReplicationMetricsCountersManager(dbMetrics);
            

        }

        public void AddGauge<T>(Type type, string name, Func<T> function)
        {
            dbMetrics.Gauge(type, name, function);
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

        public MeterMetric GetReplicationBatchSizeMetric(ReplicationStrategy destination)
        {
            return ReplicationBatchSizeMeter.GetOrAdd(destination.ConnectionStringOptions.Url,
                s => dbMetrics.Meter("metrics", "docs/min for " + s, "Replication docs/min Counter", TimeUnit.Minutes));
        }

        public MeterMetric GetReplicationDurationMetric(ReplicationStrategy destination)
        {
            return ReplicationDurationMeter.GetOrAdd(destination.ConnectionStringOptions.Url,
                s => dbMetrics.Meter("metrics", "duration for " + s, "Replication duration Counter", TimeUnit.Minutes));
        }

        public HistogramMetric GetReplicationBatchSizeHistogram(ReplicationStrategy destination)
        {
            return ReplicationBatchSizeHistogram.GetOrAdd(destination.ConnectionStringOptions.Url,
                s => dbMetrics.Histogram("metrics", "Replication docs/min Histogram for " + s));
        }

        public HistogramMetric GetReplicationDurationHistogram(ReplicationStrategy destination)
        {
            return ReplicationDurationHistogram.GetOrAdd(destination.ConnectionStringOptions.Url,
                s => dbMetrics.Histogram("metrics", "Replication duration Histogram for " + s));
        }




        public class SqlReplicationMetricsCountersManager
        {
            readonly Metrics dbMetrics;

            public ConcurrentDictionary<string, MeterMetric> SqlReplicationBatchSizeMeter { get; private set; }
            public ConcurrentDictionary<string, MeterMetric> SqlReplicationDurationMeter { get; private set; }
            public ConcurrentDictionary<string, HistogramMetric> SqlReplicationBatchSizeHistogram { get; private set; }
            public ConcurrentDictionary<string, HistogramMetric> SqlReplicationDurationHistogram { get; private set; }
            public ConcurrentDictionary<Tuple<string, string>, MeterMetric> SqlReplicationDeleteActionsMeter { get; private set; }
            public ConcurrentDictionary<Tuple<string, string>, MeterMetric> SqlReplicationInsertActionsMeter { get; private set; }
            public ConcurrentDictionary<Tuple<string, string>, MeterMetric> SqlReplicationDeleteActionsDurationMeter { get; private set; }
            public ConcurrentDictionary<Tuple<string, string>, MeterMetric> SqlReplicationInsertActionsDurationMeter { get; private set; }
            public ConcurrentDictionary<Tuple<string, string>, HistogramMetric> SqlReplicationDeleteActionsHistogram { get; private set; }
            public ConcurrentDictionary<Tuple<string, string>, HistogramMetric> SqlReplicationInsertActionsHistogram { get; private set; }
            public ConcurrentDictionary<Tuple<string, string>, HistogramMetric> SqlReplicationDeleteActionsDurationHistogram { get; private set; }
            public ConcurrentDictionary<Tuple<string, string>, HistogramMetric> SqlReplicationInsertActionsDurationHistogram { get; private set; }

            public SqlReplicationMetricsCountersManager(Metrics dbMetrics)
            {
                this.dbMetrics = dbMetrics;
                SqlReplicationBatchSizeMeter = new ConcurrentDictionary<string, MeterMetric>();
                SqlReplicationDurationMeter = new ConcurrentDictionary<string, MeterMetric>();
                SqlReplicationBatchSizeHistogram = new ConcurrentDictionary<string, HistogramMetric>();
                SqlReplicationDurationHistogram = new ConcurrentDictionary<string, HistogramMetric>();
                SqlReplicationDeleteActionsMeter = new ConcurrentDictionary<Tuple<string, string>, MeterMetric>();
                SqlReplicationInsertActionsMeter = new ConcurrentDictionary<Tuple<string, string>, MeterMetric>();
                SqlReplicationDeleteActionsHistogram = new ConcurrentDictionary<Tuple<string, string>, HistogramMetric>();
                SqlReplicationInsertActionsHistogram = new ConcurrentDictionary<Tuple<string, string>, HistogramMetric>();
                SqlReplicationDeleteActionsDurationMeter = new ConcurrentDictionary<Tuple<string, string>, MeterMetric>();
                SqlReplicationInsertActionsDurationMeter = new ConcurrentDictionary<Tuple<string, string>, MeterMetric>();
                SqlReplicationDeleteActionsDurationHistogram = new ConcurrentDictionary<Tuple<string, string>, HistogramMetric>();
                SqlReplicationInsertActionsDurationHistogram = new ConcurrentDictionary<Tuple<string, string>, HistogramMetric>();
            }

            public MeterMetric GetSqlReplicationBatchSizeMetric(SqlReplicationConfig sqlReplicationConfig)
            {
                return SqlReplicationBatchSizeMeter.GetOrAdd(sqlReplicationConfig.Name,
                    s => dbMetrics.Meter("metrics", "docs/min for " + s, "SQLReplication docs/min Counter", TimeUnit.Minutes));
            }

            public HistogramMetric GetSqlReplicationBatchSizeHistogram(SqlReplicationConfig sqlReplicationConfig)
            {
                return SqlReplicationBatchSizeHistogram.GetOrAdd(sqlReplicationConfig.Name,
                    s => dbMetrics.Histogram("metrics", "SQLReplication docs/min Histogram for " + s));
            }

            public MeterMetric GetSqlReplicationDurationMetric(SqlReplicationConfig sqlReplicationConfig)
            {
                return SqlReplicationDurationMeter.GetOrAdd(sqlReplicationConfig.Name,
                    s => dbMetrics.Meter("metrics", "duration for " + s, "SQLReplication duration Counter", TimeUnit.Minutes));
            }
            

            public HistogramMetric GetSqlReplicationDurationHistogram(SqlReplicationConfig sqlReplicationConfig)
            {
                return SqlReplicationDurationHistogram.GetOrAdd(sqlReplicationConfig.Name,
                    s => dbMetrics.Histogram("metrics", "SQLReplication duration Histogram for " + s));
            }


            public MeterMetric GetSqlReplicationDeletesAmountMetrics(Tuple<string,string> replicationNameReplicationTableKey)
            {
                return SqlReplicationDeleteActionsMeter.GetOrAdd(replicationNameReplicationTableKey,
                    s => dbMetrics.Meter("metrics", "docs/min for " + s, "SQLReplication Delete Commands/min Counter", TimeUnit.Minutes));
            }
            public HistogramMetric GetSqlReplicationDeletesAmountHistogram(Tuple<string, string> replicationNameReplicationTableKey)
            {
                return SqlReplicationInsertActionsHistogram.GetOrAdd(replicationNameReplicationTableKey,
                    s => dbMetrics.Histogram("metrics", "SQLReplication Delete Commands/min Histogram for " + s));
            }

            public MeterMetric GetSqlReplicationInsertsAmountMetrics(Tuple<string, string> replicationNameReplicationTableKey)
            {
                return SqlReplicationInsertActionsMeter.GetOrAdd(replicationNameReplicationTableKey,
                    s => dbMetrics.Meter("metrics", "docs/min for " + s, "SQLReplication Insert Commands/min Counter", TimeUnit.Minutes));
            }

            public HistogramMetric GetSqlReplicationInsertsAmountHistogram(Tuple<string, string> replicationNameReplicationTableKey)
            {
                return SqlReplicationDeleteActionsHistogram.GetOrAdd(replicationNameReplicationTableKey,
                    s => dbMetrics.Histogram("metrics", "SQLReplication Inser Commands/min Histogram for " + s));
            }

            public MeterMetric GetSqlReplicationDeleteDurationMetrics(Tuple<string, string> replicationNameReplicationTableKey)
            {
                return SqlReplicationDeleteActionsDurationMeter.GetOrAdd(replicationNameReplicationTableKey,
                    s => dbMetrics.Meter("metrics", "docs/min for " + s, "SQLReplication Delete Command duration Counter", TimeUnit.Minutes));
            }

            public HistogramMetric GetSqlReplicationDeleteDurationHistogram(Tuple<string, string> replicationNameReplicationTableKey)
            {
                return SqlReplicationDeleteActionsDurationHistogram.GetOrAdd(replicationNameReplicationTableKey,
                    s => dbMetrics.Histogram("metrics", "Replication Delete Command duration Histogram for " + s));
            }

            public MeterMetric GetSqlReplicationInsertDurationMetrics(Tuple<string, string> replicationNameReplicationTableKey)
            {
                return SqlReplicationInsertActionsDurationMeter.GetOrAdd(replicationNameReplicationTableKey,
                    s => dbMetrics.Meter("metrics", "docs/min for " + s, "Replication docs/min Counter", TimeUnit.Minutes));
            }

            public HistogramMetric GetSqlReplicationInsertDurationHistogram(Tuple<string, string> replicationNameReplicationTableKey)
            {
                return SqlReplicationInsertActionsDurationHistogram.GetOrAdd(replicationNameReplicationTableKey,
                    s => dbMetrics.Histogram("metrics", "Replication docs/min Histogram for " + s));
            }
            
        }
    }
}