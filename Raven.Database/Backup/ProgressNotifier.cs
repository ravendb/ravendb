using Raven.Abstractions.Data;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Raven.Database.Backup
{
    public class ProgressNotifier
	{
		public long TotalBytes { get; set; }
		public long TotalBytesWritten { get; private set; }

		private int lastPercentWritten = 0;

		public ProgressNotifier()
		{
			TotalBytes = 0;
			TotalBytesWritten = 0;
		}

		public void UpdateProgress(long bytesWritten, Action<string, BackupStatus.BackupMessageSeverity> Notifier)
		{
			TotalBytesWritten += bytesWritten;
			int percentage = (int)(((double)TotalBytesWritten / (double)TotalBytes) * 100.0);
			if (percentage > lastPercentWritten)
			{
				Notifier("Overall progress " + percentage + "% done", BackupStatus.BackupMessageSeverity.Informational);
				lastPercentWritten = percentage;
			}
		}
	}
}