using System;

namespace Raven.Abstractions.Data
{
	public class Alert
	{
		public string Title { get; set; }
		public DateTime CreatedAt { get; set; }
		public bool Observed { get; set; }
        /// <summary>
        /// Purpose of this field is to avoid user from being flooded by recurring errors. We can display error i.e. once per day.
        /// This field might be used to determinate when user dismissed given alert for the last time.
        /// </summary>
        public DateTime? LastDismissedAt { get; set; }
		public string Message { get; set; }
		public AlertLevel AlertLevel { get; set; }
		public string Exception { get; set; }

		public string UniqueKey { get; set; }
	}

	public enum AlertLevel
	{
		Warning,
		Error
	}
}
