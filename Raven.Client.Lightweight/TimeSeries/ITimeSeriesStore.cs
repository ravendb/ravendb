using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.Connection;
using Raven.Abstractions.TimeSeries;
using Raven.Client.TimeSeries.Changes;
using Raven.Client.TimeSeries.Replication;

namespace Raven.Client.TimeSeries
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


		Task CreatePrefixConfigurationAsync(string prefix, byte valueLength, CancellationToken token = default(CancellationToken));

		Task DeletePrefixConfigurationAsync(string prefix, CancellationToken token = default(CancellationToken));

		Task AppendAsync(string prefix, string key, DateTime at, double value, CancellationToken token = default(CancellationToken));

		Task AppendAsync(string prefix, string key, DateTime at, CancellationToken token, params double[] values);

		Task AppendAsync(string prefix, string key, DateTime at, double[] values, CancellationToken token = default(CancellationToken));

		Task DeleteAsync(string prefix, string key, CancellationToken token = default(CancellationToken));

		Task DeleteRangeAsync(string prefix, string key, DateTime start, DateTime end, CancellationToken token = default(CancellationToken));

		
		Task<TimeSeriesStats> GetTimeSeriesStatsAsync(CancellationToken token = default (CancellationToken));

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