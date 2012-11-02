using System;
using System.Collections.Generic;

namespace Raven.Abstractions.Data
{
    public class RestoreStatus
    {
        public const string RavenBackupStatusDocumentKey = "Raven/Restore/Status";

		public enum RestoreMessageSeverity
		{
			Informational,
			Error
		}

		public DateTime Started { get; set; }
		public DateTime? Completed { get; set; }
		public bool IsRunning { get; set; }
		public List<RestoreMessage> Messages { get; set; }

		public class RestoreMessage
		{
			public string Message { get; set; }
			public DateTime Timestamp { get; set; }
			public RestoreMessageSeverity Severity { get; set; }
		}

        public RestoreStatus()
		{
            Messages = new List<RestoreMessage>();
		}
    }
}