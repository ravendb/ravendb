using System;
using Raven.Abstractions.TimeSeries.Notifications;
using Raven.Client.Changes;

namespace Raven.Client.TimeSeries.Changes
{
	public interface ITimeSeriesChanges : IConnectableChanges<ITimeSeriesChanges>
    {
		/// <summary>
		/// Subscribe to changes for specified group and time series only.
		/// </summary>
		IObservableWithTask<ChangeNotification> ForChange(string groupName, string timeSeriesName);

		/// <summary>
		/// Subscribe to changes for specified group and time series only.
		/// </summary>
		IObservableWithTask<StartingWithNotification> ForTimeSeriesStartingWith(string groupName, string prefixForName);

		/// <summary>
		/// Subscribe to changes for specified group and time series only.
		/// </summary>
		IObservableWithTask<InGroupNotification> ForTimeSeriesInGroup(string groupName);

		/// <summary>
		/// Subscribe to all bulk operation changes that belong to a operation with given Id.
		/// </summary>
		IObservableWithTask<BulkOperationNotification> ForBulkOperation(Guid? operationId = null);
    }
}
