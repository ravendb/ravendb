using System;
using System.Threading.Tasks;
using Raven.Abstractions.TimeSeries.Notifications;
using Raven.Client.Changes;

namespace Raven.Client.TimeSeries.Changes
{
    public class TimeSeriesConnectionState : ConnectionStateBase
    {
        private readonly Func<TimeSeriesConnectionState, Task> ensureConnection;

        public TimeSeriesConnectionState(Func<Task> disconnectAction, Func<TimeSeriesConnectionState, Task> ensureConnection, Task task)
            : base(disconnectAction, task)
        {
            this.ensureConnection = ensureConnection;
        }

        protected override Task EnsureConnection()
        {
            return ensureConnection(this);
        }

        public event Action<KeyChange> OnChangeNotification = (x) => { };
        public void Send(KeyChange keyChange)
        {
            var onTimeSeriesChangeNotification = OnChangeNotification;
            if (onTimeSeriesChangeNotification != null)
                onTimeSeriesChangeNotification(keyChange);
        }

        public event Action<BulkOperationChange> OnBulkOperationNotification = (x) => { };
        public void Send(BulkOperationChange bulkOperationChange)
        {
            var onBulkOperationNotification = OnBulkOperationNotification;
            if (onBulkOperationNotification != null)
                onBulkOperationNotification(bulkOperationChange);
        }
    }
}
