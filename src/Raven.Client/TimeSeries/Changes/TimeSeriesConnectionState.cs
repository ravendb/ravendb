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

        public event Action<KeyChangeNotification> OnChangeNotification = (x) => { };
        public void Send(KeyChangeNotification keyNotification)
        {
            var onTimeSeriesChangeNotification = OnChangeNotification;
            if (onTimeSeriesChangeNotification != null)
                onTimeSeriesChangeNotification(keyNotification);
        }

        public event Action<BulkOperationNotification> OnBulkOperationNotification = (x) => { };
        public void Send(BulkOperationNotification bulkOperationNotification)
        {
            var onBulkOperationNotification = OnBulkOperationNotification;
            if (onBulkOperationNotification != null)
                onBulkOperationNotification(bulkOperationNotification);
        }
    }
}
