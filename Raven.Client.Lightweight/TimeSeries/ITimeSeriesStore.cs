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

		Convention Convention { get; set; }

		TimeSeriesReplicationInformer ReplicationInformer { get; }

		TimeSeriesStore.TimeSeriesStoreAdvancedOperations Advanced { get; }

		TimeSeriesStore.TimeSeriesStoreAdminOperations Admin { get; }

		Task ChangeAsync(string groupName, string timeSeriesName, long delta, CancellationToken token = default(CancellationToken));

		Task IncrementAsync(string groupName, string timeSeriesName, CancellationToken token = default(CancellationToken));

		Task DecrementAsync(string groupName, string timeSeriesName, CancellationToken token = default(CancellationToken));

		Task ResetAsync(string groupName, string timeSeriesName, CancellationToken token = default(CancellationToken));

		Task<long> GetOverallTotalAsync(string groupName, string timeSeriesName, CancellationToken token = default(CancellationToken));

		Task<TimeSeriesStats> GetTimeSeriesStatsAsync(CancellationToken token = default (CancellationToken));

		Task<TimeSeriesMetrics> GetTimeSeriesMetricsAsync(CancellationToken token = default (CancellationToken));

		Task<List<TimeSeriesReplicationStats>> GetTimeSeriesRelicationStatsAsync(CancellationToken token = default (CancellationToken));


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
