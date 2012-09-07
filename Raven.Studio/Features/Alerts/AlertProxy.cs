using System;
using Raven.Abstractions.Data;
using Raven.Studio.Infrastructure;

namespace Raven.Studio.Features.Alerts
{
	public class AlertProxy
	{
		public Alert Alert { get; set; }
		public string Title { get; set; }
		public DateTime CreatedAt { get; set; }
		public string Database { get; set; }
		public Observable<bool> Observed { get; set; }
		public string Message { get; set; }
		public AlertLevel AlertLevel { get; set; }

		public AlertProxy(Alert alert)
		{
			Alert = alert;
			Title = alert.Title;
			CreatedAt = alert.CreatedAt;
			Database = alert.Database;
			Observed = new Observable<bool> {Value = alert.Observed};
			Message = alert.Message;
			AlertLevel = alert.AlertLevel;

			Observed.PropertyChanged += (sender, args) =>
			{
				Alert.Observed = Observed.Value;
			};
		}
	}
}