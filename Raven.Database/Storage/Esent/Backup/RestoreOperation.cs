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
	using System.Threading;

	public class RestoreOperation
	{
		private readonly Action<string> output;
		private readonly bool defrag;
		private readonly string backupLocation;
		private readonly string databaseLocation;

		private bool defragmentationCompleted;

		public RestoreOperation(string backupLocation, string databaseLocation, Action<string> output, bool defrag)
		{
			this.output = output;
			this.defrag = defrag;
			this.backupLocation = backupLocation.ToFullPath();
			this.databaseLocation = databaseLocation.ToFullPath();
		}

		public void Execute()
		{
			if (File.Exists(Path.Combine(backupLocation, "RavenDB.Backup")) == false)
			{
				output(backupLocation + " doesn't look like a valid backup");
				throw new InvalidOperationException(backupLocation + " doesn't look like a valid backup");
			}

			if (Directory.Exists(databaseLocation) && Directory.GetFileSystemEntries(databaseLocation).Length > 0)
			{
				output("Database already exists, cannot restore to an existing database.");
				throw new IOException("Database already exists, cannot restore to an existing database.");
			}

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

			var dataFilePath = Path.Combine(databaseLocation, "Data");

			bool hideTerminationException = false;
			JET_INSTANCE instance;
			Api.JetCreateInstance(out instance, "restoring " + Guid.NewGuid());
			try
			{
				new TransactionalStorageConfigurator(new RavenConfiguration()).ConfigureInstance(instance, databaseLocation);
				Api.JetRestoreInstance(instance, backupLocation, databaseLocation, StatusCallback);
				var fileThatGetsCreatedButDoesntSeemLikeItShould =
					new FileInfo(
						Path.Combine(
							new DirectoryInfo(databaseLocation).Parent.FullName, new DirectoryInfo(databaseLocation).Name + "Data"));
				
				if (fileThatGetsCreatedButDoesntSeemLikeItShould.Exists)
				{
					fileThatGetsCreatedButDoesntSeemLikeItShould.MoveTo(dataFilePath);
				}

				if (defrag)
				{
					DefragmentDatabase(instance, dataFilePath);
				}
			}
			catch(Exception)
			{
				hideTerminationException = true;
				throw;
			}
			finally
			{
				try
				{
					Api.JetTerm(instance);
				}
				catch (Exception)
				{
					if (hideTerminationException == false)
						throw;
				}
			}
		}

		private string CombineIncrementalBackups()
		{
			var directories = Directory.GetDirectories(backupLocation, "Inc*")
				.OrderBy(dir => dir)
				.ToList();

			foreach (var directory in directories)
			{
				foreach (var file in Directory.GetFiles(directory, "RVN*.log"))
				{
					var justFile = Path.GetFileName(file);
					File.Copy(file, Path.Combine(backupLocation, "new", justFile), true);
				}
			}

			return directories.LastOrDefault() ?? backupLocation;
		}

		private void DefragmentDatabase(JET_INSTANCE instance, string dataFilePath)
		{
			JET_SESID sessionId = JET_SESID.Nil;
			JET_DBID dbId = JET_DBID.Nil;

			Api.JetInit(ref instance);

			int passes = 1;
			int seconds = 60;

			defragmentationCompleted = false;

			try
			{
				Api.JetBeginSession(instance, out sessionId, null, null);

				Api.JetAttachDatabase(sessionId, dataFilePath, AttachDatabaseGrbit.None);
				Api.JetOpenDatabase(sessionId, dataFilePath, null, out dbId, OpenDatabaseGrbit.None);

				Api.JetDefragment2(sessionId, dbId, null, ref passes, ref seconds, DefragmentationStatusCallback, DefragGrbit.BatchStart);

				output("Defragmentation started.");
				Console.WriteLine("Defragmentation started.");

				WaitForDefragmentationToComplete();

				output("Defragmentation finished.");
				Console.WriteLine("Defragmentation finished.");
			}
			finally
			{
				Api.JetCloseDatabase(sessionId, dbId, CloseDatabaseGrbit.None);
				Api.JetDetachDatabase(sessionId, dataFilePath);
				Api.JetEndSession(sessionId, EndSessionGrbit.None);
			}
		}

		private JET_err DefragmentationStatusCallback(JET_SESID sesid, JET_DBID dbId, JET_TABLEID tableId, JET_cbtyp cbtyp, object data1, object data2, IntPtr ptr1, IntPtr ptr2)
		{
			defragmentationCompleted = cbtyp == JET_cbtyp.OnlineDefragCompleted;

			return JET_err.Success;
		}

		private void WaitForDefragmentationToComplete()
		{
			while (!defragmentationCompleted)
			{
				output(".");
				Console.Write(".");

				Thread.Sleep(TimeSpan.FromSeconds(1));
			}

			Console.WriteLine();
		}

		private JET_err StatusCallback(JET_SESID sesid, JET_SNP snp, JET_SNT snt, object data)
		{
			output(string.Format("Esent Restore: {0} {1} {2}", snp, snt, data));
			Console.WriteLine("Esent Restore: {0} {1} {2}", snp, snt, data);
			return JET_err.Success;
		}

		private void CopyAll(DirectoryInfo source, DirectoryInfo target)
		{
			// Check if the target directory exists, if not, create it.
			if (Directory.Exists(target.FullName) == false)
			{
				Directory.CreateDirectory(target.FullName);
			}

			// Copy each file into it's new directory.
			foreach (FileInfo fi in source.GetFiles())
			{
				output(string.Format(@"Copying {0}\{1}", target.FullName, fi.Name));
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