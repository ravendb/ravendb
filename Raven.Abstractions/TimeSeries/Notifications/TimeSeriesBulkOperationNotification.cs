using System;

namespace Raven.Abstractions.TimeSeries.Notifications
{
	public class TimeSeriesBulkOperationNotification : TimeSeriesNotification
	{
		public Guid OperationId { get; set; }

		public BatchType Type { get; set; }

		public string Message { get; set; }
	}

	public enum BatchType
	{
		Started,
		Ended,
		Error
	}
}