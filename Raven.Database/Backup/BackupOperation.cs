using System;
using System.Collections.Generic;
using System.IO;
using Microsoft.Isam.Esent.Interop;

namespace Raven.Database.Backup
{
	public class BackupOperation
	{
		private readonly JET_INSTANCE instance;
		private readonly string to;
		private readonly string from;

		public BackupOperation(JET_INSTANCE instance, string from, string to)
		{
			this.instance = instance;
			this.to = to;
			this.from = from;
		}

		public void Execute()
		{
			DateTime lastBackup = DateTime.MinValue;

			var directoryBackups = new List<DirectoryBackup>
			{
				new DirectoryBackup(Path.Combine(from, "IndexDefinitions"), Path.Combine(to, "IndexDefinitions"), lastBackup)
			};
			foreach (var index in Directory.GetDirectories(Path.Combine(from,"Indexes")))
			{
				directoryBackups.Add(
					new DirectoryBackup(Path.Combine(from, "Indexes", index), Path.Combine(to, "Indexes", Path.GetFileName(index)), lastBackup)
					);
			}

			foreach (var directoryBackup in directoryBackups)
			{
				directoryBackup.Notify += Console.WriteLine;
				directoryBackup.Prepare();
			}

			foreach (var directoryBackup in directoryBackups)
			{
				directoryBackup.Execute();
			}

			new EsentBackup(instance, to, false).Execute();
		}
	}
}