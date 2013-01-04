using System.Collections.Generic;

namespace Raven.Abstractions.Data
{
	public class AlertsDocument
	{
		public List<Alert> Alerts { get; set; }

		public AlertsDocument()
		{
			Alerts = new List<Alert>();
		}
	}
}
