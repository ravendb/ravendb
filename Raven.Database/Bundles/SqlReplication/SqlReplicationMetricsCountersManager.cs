using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using Raven.Abstractions.Data;
using Raven.Database.Extensions;
using Raven.Database.Util;
using Raven.Imports.metrics;
using Raven.Imports.metrics.Core;

namespace Raven.Database.Bundles.SqlReplication
{
    [CLSCompliant(false)]
    public class SqlReplicationMetricsCountersManager : IDisposable
    {
        readonly Metrics dbMetrics;
        private readonly SqlReplicationConfig sqlReplicationConfig;
        private const string MeterContext = "metrics";
        private readonly string meterName;

        public MeterMetric SqlReplicationBatchSizeMeter { get; private set; }
        public HistogramMetric SqlReplicationBatchSizeHistogram { get; private set; }
        public HistogramMetric SqlReplicationDurationHistogram { get; private set; }
        public ConcurrentDictionary<string, SqlReplicationTableMetrics> TablesMetrics { get; set; }
        public ConcurrentQueue<SqlReplicationPerformanceStats> ReplicationPerformanceStats { get; set; }

        public SqlReplicationMetricsCountersManager(Metrics dbMetrics, SqlReplicationConfig sqlReplicationConfig)
        {
            this.dbMetrics = dbMetrics;
            this.sqlReplicationConfig = sqlReplicationConfig;

            meterName = "SqlReplication Batch docs/min for " + sqlReplicationConfig.Name;
            SqlReplicationBatchSizeMeter = dbMetrics.Meter(MeterContext, meterName, "SQLReplication docs/min Counter", TimeUnit.Minutes);
            MetricsTicker.Instance.AddFiveSecondsIntervalMeterMetric(SqlReplicationBatchSizeMeter);

            SqlReplicationBatchSizeHistogram = dbMetrics.Histogram("metrics", "SqlReplication Batch histogram for " + sqlReplicationConfig.Name);
            SqlReplicationDurationHistogram = dbMetrics.Histogram("metrics", "SQLReplication duration Histogram for " + sqlReplicationConfig.Name);
            TablesMetrics = new ConcurrentDictionary<string, SqlReplicationTableMetrics>();
            ReplicationPerformanceStats = new ConcurrentQueue<SqlReplicationPerformanceStats>();
        }

        public SqlReplicationTableMetrics GetTableMetrics(string tableName)
        {
            return TablesMetrics.GetOrAdd(tableName, s => new SqlReplicationTableMetrics(s, sqlReplicationConfig, dbMetrics));
        }

        public SqlReplicationMetricsData ToSqlReplicationMetricsData()
        {
            return new SqlReplicationMetricsData()
            {
                GeneralMetrics = new Dictionary<string, IMetricsData>()
                {
                    {"Batch Size Meter", SqlReplicationBatchSizeMeter.CreateMeterData()},
                    {"Batch Size Histogram", SqlReplicationBatchSizeHistogram.CreateHistogramData()},
                    {"Duration Histogram", SqlReplicationDurationHistogram.CreateHistogramData()}
                },
                TablesMetrics = TablesMetrics.ToDictionary(x => x.Key, x => x.Value.ToSqlReplicationTableMetricsDataDictionary())
            };
        }

        public class SqlReplicationTableMetrics : IDisposable
        {
            private readonly Metrics dbMetrics;
            public string TableName { get; set; }
            public SqlReplicationConfig Config { get; set; }
            public MeterMetric m_sqlReplicationDeleteActionsMeter;
            public MeterMetric m_sqlReplicationInsertActionsMeter;
            private const string MeterContext = "metrics";
            private readonly string deleteMeterName;
            private readonly string insertMeterName;
            public HistogramMetric m_sqlReplicationDeleteActionsHistogram;
            public HistogramMetric m_sqlReplicationInsertActionsHistogram;
            public HistogramMetric m_sqlReplicationDeleteActionsDurationHistogram;
            public HistogramMetric m_sqlReplicationInsertActionsDurationHistogram;

            public SqlReplicationTableMetrics(string tableName, SqlReplicationConfig cfg, Metrics dbMetrics)
            {
                this.dbMetrics = dbMetrics;
                Config = cfg;
                TableName = tableName;

                deleteMeterName = "SqlReplication Deletes/min for table :" + TableName + " in replication: " + Config.Name;
                insertMeterName = "SqlReplication Inserts/min for table :" + TableName + " in replication: " + Config.Name;
            }

            public MeterMetric SqlReplicationDeleteActionsMeter
            {
                get
                {
                    if (m_sqlReplicationDeleteActionsMeter == null)
                    {
                        m_sqlReplicationDeleteActionsMeter = dbMetrics.Meter(MeterContext, deleteMeterName, "SQLReplication Delete Commands/min Counter", TimeUnit.Minutes);
                        MetricsTicker.Instance.AddFiveSecondsIntervalMeterMetric(m_sqlReplicationDeleteActionsMeter);
                    }

                    return m_sqlReplicationDeleteActionsMeter;
                }
            }
            public HistogramMetric SqlReplicationDeleteActionsHistogram
            {
                get
                {
                    return m_sqlReplicationDeleteActionsHistogram ?? (m_sqlReplicationDeleteActionsHistogram = dbMetrics.Histogram("metrics", "SQLReplication Deletes Commands/min Histogram for table :" + TableName + " in replication: " + Config.Name));
                }
            }
            public MeterMetric SqlReplicationInsertActionsMeter
            {
                get
                {
                    if (m_sqlReplicationInsertActionsMeter == null)
                    {
                        m_sqlReplicationInsertActionsMeter = dbMetrics.Meter(MeterContext, insertMeterName, "SQLReplication Insert Commands/min Counter", TimeUnit.Minutes);
                        MetricsTicker.Instance.AddFiveSecondsIntervalMeterMetric(m_sqlReplicationInsertActionsMeter);
                    }

                    return m_sqlReplicationInsertActionsMeter;
                }
            }

            public HistogramMetric SqlReplicationInsertActionsHistogram
            {
                get
                {
                    return m_sqlReplicationInsertActionsHistogram ?? (m_sqlReplicationInsertActionsHistogram = dbMetrics.Histogram("metrics", "SQLReplication Insert Commands Histogram for table :" + TableName + " in replication: " + Config.Name));
                }
            }
            public HistogramMetric SqlReplicationDeleteActionsDurationHistogram
            {
                get
                {
                    return m_sqlReplicationDeleteActionsDurationHistogram ?? (m_sqlReplicationDeleteActionsDurationHistogram = dbMetrics.Histogram("metrics", "SqlReplication Delete Command duration Histogram for table :" + TableName + " in replication: " + Config.Name));
                }
            }
            public HistogramMetric SqlReplicationInsertActionsDurationHistogram
            {
                get
                {
                    return m_sqlReplicationInsertActionsDurationHistogram ?? (m_sqlReplicationInsertActionsDurationHistogram = dbMetrics.Histogram("metrics", "SqlReplication Insert Commands Duration Histogram for table :" + TableName + " in replication: " + Config.Name));
                }
            }

            public Dictionary<string, IMetricsData> ToSqlReplicationTableMetricsDataDictionary()
            {
                return new Dictionary<string, IMetricsData>()
                {
                    {"Delete Actions Meter",SqlReplicationDeleteActionsMeter.CreateMeterData()},
                    {"Insert Actions Meter",SqlReplicationInsertActionsMeter.CreateMeterData()},
                    {"Delete Actions Histogram",SqlReplicationDeleteActionsHistogram.CreateHistogramData()},
                    {"Insert Actions Duration Histogram",SqlReplicationInsertActionsDurationHistogram.CreateHistogramData()},
                    {"Insert Actions Histogram",SqlReplicationInsertActionsHistogram.CreateHistogramData()},
                    {"Delete Actions Duration Histogram",SqlReplicationDeleteActionsDurationHistogram.CreateHistogramData()}
                };
            }

            public void Dispose()
            {
                if (m_sqlReplicationDeleteActionsMeter != null)
                {
                    dbMetrics.RemoveMeter(MeterContext, deleteMeterName);
                    MetricsTicker.Instance.RemoveFiveSecondsIntervalMeterMetric(m_sqlReplicationDeleteActionsMeter);
                }


                if (m_sqlReplicationInsertActionsMeter != null)
                {
                    dbMetrics.RemoveMeter(MeterContext, insertMeterName);
                    MetricsTicker.Instance.RemoveFiveSecondsIntervalMeterMetric(m_sqlReplicationInsertActionsMeter);
                }
            }
        }

        public void Dispose()
        {
            MetricsTicker.Instance.RemoveFiveSecondsIntervalMeterMetric(SqlReplicationBatchSizeMeter);
            dbMetrics.RemoveMeter(MeterContext, meterName);

            foreach (var tableMetric in TablesMetrics)
            {
                tableMetric.Value.Dispose();
            }
        }
    }

    public class SqlReplicationMetricsData
    {
        public Dictionary<string, IMetricsData> GeneralMetrics { get; set; }
        public Dictionary<string, Dictionary<string, IMetricsData>> TablesMetrics { get; set; }
    }
}
