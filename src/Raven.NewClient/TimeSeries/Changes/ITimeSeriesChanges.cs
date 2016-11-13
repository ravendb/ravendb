using System;
using Raven.Abstractions.TimeSeries.Notifications;
using Raven.NewClient.Client.Changes;

namespace Raven.NewClient.Client.TimeSeries.Changes
{
    public interface ITimeSeriesChanges : IConnectableChanges<ITimeSeriesChanges>
    {
        /// <summary>
        /// Subscribe to changes for specified type and key only.
        /// </summary>
        IObservableWithTask<KeyChangeNotification> ForKey(string type, string key);

        /// <summary>
        /// Subscribe to all bulk operation changes that belong to a operation with given Id.
        /// </summary>
        IObservableWithTask<BulkOperationNotification> ForBulkOperation(Guid? operationId = null);
    }
}
