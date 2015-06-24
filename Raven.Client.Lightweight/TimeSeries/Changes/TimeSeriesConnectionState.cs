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

		public event Action<ChangeNotification> OnChangeNotification = (x) => { };
		public void Send(ChangeNotification changeNotification)
        {
			var onTimeSeriesChangeNotification = OnChangeNotification;
			if (onTimeSeriesChangeNotification != null)
				onTimeSeriesChangeNotification(changeNotification);
        }

		public event Action<StartingWithNotification> OnTimeSeriesStartingWithNotification = (x) => { };
		public void Send(StartingWithNotification changeNotification)
		{
			var onTimeSeriesChangeNotification = OnTimeSeriesStartingWithNotification;
			if (onTimeSeriesChangeNotification != null)
				onTimeSeriesChangeNotification(changeNotification);
		}

		public event Action<InGroupNotification> OnTimeSeriesInGroupNotification = (x) => { };
		public void Send(InGroupNotification changeNotification)
		{
			var onTimeSeriesChangeNotification = OnTimeSeriesInGroupNotification;
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

	public abstract class ConnectionState<T> : ConnectionStateBase
		where T: ConnectionStateBase
	{
		private readonly Func<T, Task> ensureConnection;

		private T self;

		public ConnectionState(Action onZero, Func<T, Task> ensureConnection, Task task, T self)
			: base(onZero, task)
		{
			this.ensureConnection = ensureConnection;
			this.self = self;
		}

		protected override Task EnsureConnection()
		{
			return ensureConnection(self);
		}
	}
}
