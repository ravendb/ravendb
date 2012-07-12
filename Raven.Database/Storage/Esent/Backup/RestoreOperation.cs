//-----------------------------------------------------------------------
// <copyright file="RestoreOperation.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.IO;
using Microsoft.Isam.Esent.Interop;
using Raven.Database;
using Raven.Database.Config;
using Raven.Database.Extensions;
using System.Linq;

namespace Raven.Storage.Esent.Backup
{
	public class RestoreOperation
	{
		private readonly string backupLocation;
		private readonly string databaseLocation;

		public RestoreOperation(string backupLocation, string databaseLocation)
		{
			this.backupLocation = backupLocation.ToFullPath();
			this.databaseLocation = databaseLocation.ToFullPath();
		}

		public void Execute()
		{
			if (File.Exists(Path.Combine(backupLocation, "RavenDB.Backup")) == false)
			{
				throw new InvalidOperationException(backupLocation + " doesn't look like a valid backup");
			}

			if(Directory.Exists(databaseLocation) && Directory.GetFileSystemEntries(databaseLocation).Length > 0)
				throw new IOException("Database already exists, cannot restore to an existing database.");

			if (Directory.Exists(databaseLocation) == false)
				Directory.CreateDirectory(databaseLocation);

			Directory.CreateDirectory(Path.Combine(databaseLocation, "logs"));
			Directory.CreateDirectory(Path.Combine(databaseLocation, "temp"));
			Directory.CreateDirectory(Path.Combine(databaseLocation, "system"));

			CombineIncrementalBackups();

			CopyAll(new DirectoryInfo(Path.Combine(backupLocation, "IndexDefinitions")),
				new DirectoryInfo(Path.Combine(databaseLocation, "IndexDefinitions")));
			CopyAll(new DirectoryInfo(Path.Combine(backupLocation, "Indexes")),
				new DirectoryInfo(Path.Combine(databaseLocation, "Indexes")));


			JET_INSTANCE instance;
			Api.JetCreateInstance(out instance, "restoring " + Guid.NewGuid());
			try
			{
				new TransactionalStorageConfigurator(new RavenConfiguration()).ConfigureInstance(instance, databaseLocation);
				Api.JetRestoreInstance(instance, backupLocation, databaseLocation, StatusCallback);
				
				var fileThatGetsCreatedButDoesntSeemLikeItShould = new FileInfo(Path.Combine(new DirectoryInfo(databaseLocation).Parent.FullName, new DirectoryInfo(databaseLocation).Name + "Data"));
				if (fileThatGetsCreatedButDoesntSeemLikeItShould.Exists)
				{
					fileThatGetsCreatedButDoesntSeemLikeItShould.MoveTo(Path.Combine(databaseLocation, "Data"));
				}
			}
			finally
			{
				Api.JetTerm(instance);
			}
		}

		private string CombineIncrementalBackups()
		{
			var directories = Directory.GetDirectories(backupLocation, "Inc*")
				.OrderBy(dir => dir)
				.ToList();

			foreach (var directory in directories)
			{
				foreach (var file in Directory.GetFiles(directory,"RVN*.log"))
				{
					var justFile = Path.GetFileName(file);
					File.Copy(file, Path.Combine(backupLocation, "new", justFile), true);
				}
			}

			return directories.LastOrDefault() ?? backupLocation;

		}

		private JET_err StatusCallback(JET_SESID sesid, JET_SNP snp, JET_SNT snt, object data)
		{
			Console.WriteLine("Esent Restore: {0} {1} {2}", snp, snt, data);
			return JET_err.Success;
		}

		private static void CopyAll(DirectoryInfo source, DirectoryInfo target)
		{
			// Check if the target directory exists, if not, create it.
			if (Directory.Exists(target.FullName) == false)
			{
				Directory.CreateDirectory(target.FullName);
			}

			// Copy each file into it's new directory.
			foreach (FileInfo fi in source.GetFiles())
			{
				Console.WriteLine(@"Copying {0}\{1}", target.FullName, fi.Name);
				fi.CopyTo(Path.Combine(target.ToString(), fi.Name), true);
			}

			// Copy each subdirectory using recursion.
			foreach (DirectoryInfo diSourceSubDir in source.GetDirectories())
			{
				DirectoryInfo nextTargetSubDir =
					target.CreateSubdirectory(diSourceSubDir.Name);
				CopyAll(diSourceSubDir, nextTargetSubDir);
			}
		}

	}
}
