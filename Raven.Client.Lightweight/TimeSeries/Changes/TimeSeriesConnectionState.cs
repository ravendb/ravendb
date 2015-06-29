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

		public event Action<TimeSeriesKeyNotification> OnChangeNotification = (x) => { };
		public void Send(TimeSeriesKeyNotification keyNotification)
        {
			var onTimeSeriesChangeNotification = OnChangeNotification;
			if (onTimeSeriesChangeNotification != null)
				onTimeSeriesChangeNotification(keyNotification);
        }

		public event Action<TimeSeriesBulkOperationNotification> OnBulkOperationNotification = (x) => { };
		public void Send(TimeSeriesBulkOperationNotification bulkOperationNotification)
        {
			var onBulkOperationNotification = OnBulkOperationNotification;
			if (onBulkOperationNotification != null)
				onBulkOperationNotification(bulkOperationNotification);
        }
    }
}
