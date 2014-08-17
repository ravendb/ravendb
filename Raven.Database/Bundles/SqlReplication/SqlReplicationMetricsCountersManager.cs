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
        public ConcurrentDictionary<string, MeterMetric> SqlReplicationDeleteActionsMeter { get; private set; }
        public ConcurrentDictionary<string, MeterMetric> SqlReplicationInsertActionsMeter { get; private set; }
        public ConcurrentDictionary<string, HistogramMetric> SqlReplicationDeleteActionsHistogram { get; private set; }
        public ConcurrentDictionary<string, HistogramMetric> SqlReplicationInsertActionsHistogram { get; private set; }
        public ConcurrentDictionary<string, HistogramMetric> SqlReplicationDeleteActionsDurationHistogram { get; private set; }
        public ConcurrentDictionary<string, HistogramMetric> SqlReplicationInsertActionsDurationHistogram { get; private set; }

        public SqlReplicationMetricsCountersManager(Metrics dbMetrics, SqlReplicationConfig sqlReplicationConfig)
        {
            this.dbMetrics = dbMetrics;
            SqlReplicationBatchSizeMeter = dbMetrics.Meter("metrics", "SqlReplication Batch docs/min for " + sqlReplicationConfig.Name, "SQLReplication docs/min Counter", TimeUnit.Minutes);
            SqlReplicationBatchSizeHistogram = dbMetrics.Histogram("metrics", "SqlReplication Batch histogram for " + sqlReplicationConfig.Name);
            SqlReplicationDurationHistogram = dbMetrics.Histogram("metrics", "SQLReplication duration Histogram for " + sqlReplicationConfig.Name);
            SqlReplicationDeleteActionsMeter = new ConcurrentDictionary<string, MeterMetric>();
            SqlReplicationInsertActionsMeter = new ConcurrentDictionary<string, MeterMetric>();
            SqlReplicationDeleteActionsHistogram = new ConcurrentDictionary<string, HistogramMetric>();
            SqlReplicationInsertActionsHistogram = new ConcurrentDictionary<string, HistogramMetric>();
            SqlReplicationDeleteActionsDurationHistogram = new ConcurrentDictionary<string, HistogramMetric>();
            SqlReplicationInsertActionsDurationHistogram = new ConcurrentDictionary<string, HistogramMetric>();
            this.sqlReplicationConfig = sqlReplicationConfig;
        }

        public SqlReplicationMetricsData ToSqlReplicationMetricsData()
        {
            return new SqlReplicationMetricsData()
            {
                SqlReplicationBatchSizeHistogram = SqlReplicationBatchSizeHistogram.CreateHistogramData(),
                SqlReplicationDeleteActionsDurationHistogram = SqlReplicationDeleteActionsDurationHistogram.ToHistogramDataDictionary(),
                SqlReplicationBatchSizeMeter = SqlReplicationBatchSizeMeter.CreateMeterData(),
                SqlReplicationDeleteActionsHistogram = SqlReplicationDeleteActionsHistogram.ToHistogramDataDictionary(),
                SqlReplicationDeleteActionsMeter = SqlReplicationDeleteActionsMeter.ToMeterDataDictionary(),
                SqlReplicationDurationHistogram = SqlReplicationDurationHistogram.CreateHistogramData(),
                SqlReplicationInsertActionsDurationHistogram = SqlReplicationInsertActionsDurationHistogram.ToHistogramDataDictionary(),
                SqlReplicationInsertActionsHistogram = SqlReplicationInsertActionsHistogram.ToHistogramDataDictionary(),
                SqlReplicationInsertActionsMeter = SqlReplicationInsertActionsMeter.ToMeterDataDictionary()
            };
        }

        public MeterMetric GetSqlReplicationDeletesActionsMetrics(string tableName)
        {
            return SqlReplicationDeleteActionsMeter.GetOrAdd(tableName,
                s => dbMetrics.Meter("metrics", "SqlReplication Deletes/min for table :" + tableName + " in replication: " + sqlReplicationConfig.Name, "SQLReplication Delete Commands/min Counter", TimeUnit.Minutes));
        }
        public HistogramMetric GetSqlReplicationDeletesActionsHistogram(string tableName)
        {
            return SqlReplicationDeleteActionsHistogram.GetOrAdd(tableName,
                s => dbMetrics.Histogram("metrics", "SQLReplication Deletes Commands/min Histogram for table :" + tableName + " in replication: " + sqlReplicationConfig.Name));
        }

        public MeterMetric GetSqlReplicationInsertsActionMetrics(string tableName)
        {
            return SqlReplicationInsertActionsMeter.GetOrAdd(tableName,
                s => dbMetrics.Meter("metrics", "SqlReplication Inserts/min for table :" + tableName + " in replication: " + sqlReplicationConfig.Name, "SQLReplication Insert Commands/min Counter", TimeUnit.Minutes));
        }

        public HistogramMetric GetSqlReplicationInsertsActionsHistogram(string tableName)
        {
            return SqlReplicationInsertActionsHistogram.GetOrAdd(tableName,
                s => dbMetrics.Histogram("metrics", "SQLReplication Insert Commands Histogram for table :" + tableName + " in replication: " + sqlReplicationConfig.Name));
        }

        public HistogramMetric GetSqlReplicationDeleteDurationHistogram(string tableName)
        {
            return SqlReplicationDeleteActionsDurationHistogram.GetOrAdd(tableName,
                s => dbMetrics.Histogram("metrics", "SqlReplication Delete Command duration Histogram for table :" + tableName + " in replication: " + sqlReplicationConfig.Name));
        }

        public HistogramMetric GetSqlReplicationInsertDurationHistogram(string tableName)
        {
            return SqlReplicationInsertActionsDurationHistogram.GetOrAdd(tableName,
                s => dbMetrics.Histogram("metrics", "SqlReplication Insert Commands Duration Histogram for table :" + tableName + " in replication: " + sqlReplicationConfig.Name));
        }

    }

    public class SqlReplicationMetricsData
    {
        public MeterData SqlReplicationBatchSizeMeter { get; set; }
        public HistogramData SqlReplicationBatchSizeHistogram { get; set; }
        public HistogramData SqlReplicationDurationHistogram { get; set; }
        public Dictionary<string, MeterData> SqlReplicationDeleteActionsMeter { get; set; }
        public Dictionary<string, HistogramData> SqlReplicationDeleteActionsHistogram { get; set; }
        public Dictionary<string, HistogramData> SqlReplicationDeleteActionsDurationHistogram { get; set; }
        public Dictionary<string, MeterData> SqlReplicationInsertActionsMeter { get; set; }
        public Dictionary<string, HistogramData> SqlReplicationInsertActionsHistogram { get; set; }
        public Dictionary<string, HistogramData> SqlReplicationInsertActionsDurationHistogram { get; set; }
    }
}
