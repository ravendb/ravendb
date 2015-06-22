using System;
using System.Threading.Tasks;
using Raven.Abstractions.TimeSeries.Notifications;
using Raven.Client.Changes;

namespace Raven.Client.TimeSeries.Changes
{
    public class TimeSeriesConnectionState : IChangesConnectionState
    {
        private readonly Action onZero;
        private readonly Task task;
        private int value;
        public Task Task
        {
            get { return task; }
        }

		public TimeSeriesConnectionState(Action onZero, Task task)
        {
            value = 0;
            this.onZero = onZero;
            this.task = task;
        }

        public void Inc()
        {
            lock (this)
            {
                value++;
            }
        }

        public void Dec()
        {
            lock (this)
            {
                if (--value == 0)
                    onZero();
            }
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

        public event Action<Exception> OnError;

        public void Error(Exception e)
        {
            var onOnError = OnError;
            if (onOnError != null)
                onOnError(e);
        }
    }
}
