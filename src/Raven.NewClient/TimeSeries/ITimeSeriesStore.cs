using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.Connection;
using Raven.Abstractions.TimeSeries;
using Raven.NewClient.Client.TimeSeries.Changes;
using Raven.NewClient.Client.TimeSeries.Replication;

namespace Raven.NewClient.Client.TimeSeries
{
    public interface ITimeSeriesStore : IDisposalNotification
    {
        TimeSeriesStore.BatchOperationsStore Batch { get; }

        OperationCredentials Credentials { get;  }

        string Name { get; }

        string Url { get; }

        TimeSeriesConvention TimeSeriesConvention { get; set; }

        TimeSeriesReplicationInformer ReplicationInformer { get; }

        TimeSeriesStore.TimeSeriesStoreAdvancedOperations Advanced { get; }

        TimeSeriesStore.TimeSeriesStoreAdminOperations Admin { get; }


        Task CreateTypeAsync(string type, string[] fields, CancellationToken token = default(CancellationToken));

        Task DeleteTypeAsync(string type, CancellationToken token = default(CancellationToken));

        Task AppendAsync(string type, string key, DateTimeOffset at, double value, CancellationToken token = default(CancellationToken));

        [Obsolete("You must use DateTimeOffset", true)]
        Task AppendAsync(string type, string key, DateTime at, double value, CancellationToken token = default(CancellationToken));

        Task AppendAsync(string type, string key, DateTimeOffset at, CancellationToken token, params double[] values);

        [Obsolete("You must use DateTimeOffset", true)]
        Task AppendAsync(string type, string key, DateTime at, CancellationToken token, params double[] values);

        Task AppendAsync(string type, string key, DateTimeOffset at, double[] values, CancellationToken token = default(CancellationToken));

        [Obsolete("You must use DateTimeOffset", true)]
        Task AppendAsync(string type, string key, DateTime at, double[] values, CancellationToken token = default(CancellationToken));

        Task DeleteKeyAsync(string type, string key, CancellationToken token = default(CancellationToken));

        Task DeletePointsAsync(IEnumerable<TimeSeriesPointId> points, CancellationToken token = default(CancellationToken));

        Task DeletePointAsync(string type, string key, DateTimeOffset at, CancellationToken token = default(CancellationToken));

        Task DeleteRangeAsync(string type, string key, DateTimeOffset start, DateTimeOffset end, CancellationToken token = default(CancellationToken));
        
        Task<TimeSeriesStats> GetStatsAsync(CancellationToken token = default (CancellationToken));

        Task<TimeSeriesMetrics> GetTimeSeriesMetricsAsync(CancellationToken token = default (CancellationToken));

        Task<List<TimeSeriesReplicationStats>> GetTimeSeriesReplicationStatsAsync(CancellationToken token = default (CancellationToken));

        Task<TimeSeriesReplicationDocument> GetReplicationsAsync(CancellationToken token = default (CancellationToken));

        Task SaveReplicationsAsync(TimeSeriesReplicationDocument newReplicationDocument, CancellationToken token = default(CancellationToken));

        Task<long> GetLastEtag(string serverId, CancellationToken token = default(CancellationToken));

        void Initialize(bool ensureDefaultTimeSeriesExists = false);

        /// <summary>
        /// Subscribe to change notifications from the server
        /// </summary>
        ITimeSeriesChanges Changes(string timeSeries = null);
    }
}
