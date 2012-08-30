using System;

namespace Raven.Abstractions.Data
{
	public class Alert
	{
		public string Title { get; set; }
		public DateTime CreatedAt { get; set; }
		public string Database { get; set; }
		public bool Observed { get; set; }
		public string Message { get; set; }
		public AlertLevel AlertLevel { get; set; }
	}

	public enum AlertLevel
	{
		Warning,
		Error
	}
}
