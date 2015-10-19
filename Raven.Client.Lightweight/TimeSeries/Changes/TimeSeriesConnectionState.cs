using System;
using System.Threading.Tasks;
using Raven.Abstractions.TimeSeries.Notifications;
using Raven.Client.Changes;

namespace Raven.Client.TimeSeries.Changes
{
    public class TimeSeriesConnectionState : ConnectionStateBase
    {
        private readonly Func<TimeSeriesConnectionState, Task> ensureConnection;

		public TimeSeriesConnectionState(Action onZero, Func<TimeSeriesConnectionState, Task> ensureConnection, Task task)
			: base(onZero, task)
		{
			this.ensureConnection = ensureConnection;
		}

	    protected override Task EnsureConnection()
		{
			return ensureConnection(this);
		}

		public event Action<TimeSeriesChangeNotification> OnChangeNotification = (x) => { };
		public void Send(TimeSeriesChangeNotification changeNotification)
        {
			var onTimeSeriesChangeNotification = OnChangeNotification;
			if (onTimeSeriesChangeNotification != null)
				onTimeSeriesChangeNotification(changeNotification);
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
