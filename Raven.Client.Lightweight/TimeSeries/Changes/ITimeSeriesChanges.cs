using System;
using Raven.Abstractions.TimeSeries.Notifications;
using Raven.Client.Changes;

namespace Raven.Client.TimeSeries.Changes
{
	public interface ITimeSeriesChanges : IConnectableChanges<ITimeSeriesChanges>
    {
		/// <summary>
		/// Subscribe to changes for specified key and time series only.
		/// </summary>
		IObservableWithTask<TimeSeriesKeyNotification> ForKey(string key);

		/// <summary>
		/// Subscribe to all bulk operation changes that belong to a operation with given Id.
		/// </summary>
		IObservableWithTask<TimeSeriesBulkOperationNotification> ForBulkOperation(Guid? operationId = null);
    }
}
