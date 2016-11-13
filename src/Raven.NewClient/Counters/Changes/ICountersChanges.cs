using System;
using Raven.Abstractions.Counters.Notifications;
using Raven.NewClient.Client.Changes;

namespace Raven.NewClient.Client.Counters.Changes
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
        IObservableWithTask<StartingWithNotification> ForCountersStartingWith(string groupName, string prefixForName);

        /// <summary>
        /// Subscribe to changes for specified group and counter only.
        /// </summary>
        IObservableWithTask<InGroupNotification> ForCountersInGroup(string groupName);

        /// <summary>
        /// Subscribe to all bulk operation changes that belong to a operation with given Id.
        /// </summary>
        IObservableWithTask<BulkOperationNotification> ForBulkOperation(Guid? operationId = null);
    }
}
