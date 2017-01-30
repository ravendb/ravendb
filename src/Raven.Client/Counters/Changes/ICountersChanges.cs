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
        IObservableWithTask<CounterChange> ForChange(string groupName, string counterName);

        /// <summary>
        /// Subscribe to changes for specified group and counter only.
        /// </summary>
        IObservableWithTask<StartingWithChange> ForCountersStartingWith(string groupName, string prefixForName);

        /// <summary>
        /// Subscribe to changes for specified group and counter only.
        /// </summary>
        IObservableWithTask<InGroupChange> ForCountersInGroup(string groupName);

        /// <summary>
        /// Subscribe to all bulk operation changes that belong to a operation with given Id.
        /// </summary>
        IObservableWithTask<BulkOperationChange> ForBulkOperation(Guid? operationId = null);
    }
}
