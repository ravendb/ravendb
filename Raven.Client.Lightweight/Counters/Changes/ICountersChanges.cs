using System;
using Raven.Abstractions.Counters.Notifications;
using Raven.Client.Changes;

namespace Raven.Client.Counters.Changes
{
	public interface ICountersChanges : IConnectableChanges<ICountersChanges>
    {
		/// <summary>
		/// Subscribe to changes for specified group and counter only.
		/// </summary>
		IObservableWithTask<ChangeNotification> ForChange(string groupName, string counterName);

		/// <summary>
		/// Subscribe to changes for specified group and counter only.
		/// </summary>
		IObservableWithTask<LocalChangeNotification> ForLocalCounterChange(string groupName, string counterName);

		/// <summary>
		/// Subscribe to changes for specified group and counter only.
		/// </summary>
		IObservableWithTask<ReplicationChangeNotification> ForReplicationChange(string groupName, string counterName);

		/// <summary>
		/// Subscribe to all bulk operation changes that belong to a operation with given Id.
		/// </summary>
		IObservableWithTask<BulkOperationNotification> ForBulkOperation(Guid? operationId = null);
    }
}
