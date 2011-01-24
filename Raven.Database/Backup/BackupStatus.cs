//-----------------------------------------------------------------------
// <copyright file="BackupStatus.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;

namespace Raven.Database.Backup
{
	public class BackupStatus
	{
		public const string RavenBackupStatusDocumentKey = "Raven/Backup/Status";

		public DateTime Started { get; set; }
		public DateTime? Completed { get; set; }
		public bool IsRunning { get; set; }
		public List<BackupMessage> Messages { get; set; }

		public class BackupMessage
		{
			public string Message { get; set; }
			public DateTime Timestamp { get; set; }
		}

		public BackupStatus()
		{
			Messages = new List<BackupMessage>();
		}
	}
}
