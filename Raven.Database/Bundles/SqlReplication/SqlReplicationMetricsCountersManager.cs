using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using metrics;
using metrics.Core;
using Raven.Abstractions.Data;
using Raven.Database.Extensions;

namespace Raven.Database.Bundles.SqlReplication
{
    public class SqlReplicationMetricsCountersManager
    {
        readonly Metrics dbMetrics;
        private readonly SqlReplicationConfig sqlReplicationConfig;

        public MeterMetric SqlReplicationBatchSizeMeter { get; private set; }
        public HistogramMetric SqlReplicationBatchSizeHistogram { get; private set; }
        public HistogramMetric SqlReplicationDurationHistogram { get; private set; }
        public ConcurrentDictionary<string, SqlReplicationTableMetrics> TablesMetrics { get; set; }

        public SqlReplicationMetricsCountersManager(Metrics dbMetrics, SqlReplicationConfig sqlReplicationConfig)
        {
            this.dbMetrics = dbMetrics;
            this.sqlReplicationConfig = sqlReplicationConfig;
            SqlReplicationBatchSizeMeter = dbMetrics.Meter("metrics", "SqlReplication Batch docs/min for " + sqlReplicationConfig.Name, "SQLReplication docs/min Counter", TimeUnit.Minutes);
            SqlReplicationBatchSizeHistogram = dbMetrics.Histogram("metrics", "SqlReplication Batch histogram for " + sqlReplicationConfig.Name);
            SqlReplicationDurationHistogram = dbMetrics.Histogram("metrics", "SQLReplication duration Histogram for " + sqlReplicationConfig.Name);
            TablesMetrics = new ConcurrentDictionary<string, SqlReplicationTableMetrics>();
        }

        public SqlReplicationTableMetrics GetTableMetrics(string tableName)
        {
            return TablesMetrics.GetOrAdd(tableName, s => new SqlReplicationTableMetrics(s, sqlReplicationConfig,dbMetrics));
        }

        public SqlReplicationMetricsData ToSqlReplicationMetricsData()
        {
            return new SqlReplicationMetricsData()
            {
                GeneralMetrics = new Dictionary<string, IMetricsData>()
                {
                    {"SqlReplicationBatchSizeMeter", SqlReplicationBatchSizeMeter.CreateMeterData()},
                    {"SqlReplicationBatchSizeHistogram", SqlReplicationBatchSizeHistogram.CreateHistogramData()},
                    {"SqlReplicationDurationHistogram", SqlReplicationDurationHistogram.CreateHistogramData()}
                },
                TablesMetrics = TablesMetrics.ToDictionary(x=>x.Key, x=>x.Value.ToSqlReplicationTableMetricsDataDictionary())
                
            };
        }

        public class SqlReplicationTableMetrics
        {
            private readonly Metrics dbMetrics;
            public string TableName { get; set; }
            public SqlReplicationConfig Config { get; set; }
            public MeterMetric m_sqlReplicationDeleteActionsMeter;
            public MeterMetric m_sqlReplicationInsertActionsMeter;
            public HistogramMetric m_sqlReplicationDeleteActionsHistogram;
            public HistogramMetric m_sqlReplicationInsertActionsHistogram;
            public HistogramMetric m_sqlReplicationDeleteActionsDurationHistogram;
            public HistogramMetric m_sqlReplicationInsertActionsDurationHistogram;

            public SqlReplicationTableMetrics(string tableName, SqlReplicationConfig cfg, Metrics dbMetrics)
            {
                this.dbMetrics = dbMetrics;
                Config = cfg;
                TableName = tableName;
            }

            public MeterMetric SqlReplicationDeleteActionsMeter
            {
                get
                {
                    return m_sqlReplicationDeleteActionsMeter ?? (m_sqlReplicationDeleteActionsMeter = dbMetrics.Meter("metrics", "SqlReplication Deletes/min for table :" + TableName + " in replication: " + Config.Name, "SQLReplication Delete Commands/min Counter", TimeUnit.Minutes));
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
                    return m_sqlReplicationInsertActionsMeter ?? (m_sqlReplicationInsertActionsMeter = dbMetrics.Meter("metrics", "SqlReplication Inserts/min for table :" + TableName + " in replication: " + Config.Name, "SQLReplication Insert Commands/min Counter", TimeUnit.Minutes));
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
                    {"SqlReplicationDeleteActionsMeter",SqlReplicationDeleteActionsMeter.CreateMeterData()},
                    {"SqlReplicationInsertActionsMeter",SqlReplicationInsertActionsMeter.CreateMeterData()},
                    {"SqlReplicationDeleteActionsHistogram",SqlReplicationDeleteActionsHistogram.CreateHistogramData()},
                    {"SqlReplicationInsertActionsDurationHistogram",SqlReplicationInsertActionsDurationHistogram.CreateHistogramData()},
                    {"SqlReplicationInsertActionsHistogram",SqlReplicationInsertActionsHistogram.CreateHistogramData()},
                    {"SqlReplicationDeleteActionsDurationHistogram",SqlReplicationDeleteActionsDurationHistogram.CreateHistogramData()}
                };
            }
        }
    }

    public class SqlReplicationMetricsData
    {
        public Dictionary<string, IMetricsData> GeneralMetrics { get; set; }
        public Dictionary<string, Dictionary<string, IMetricsData>> TablesMetrics { get; set; }
    }
}
