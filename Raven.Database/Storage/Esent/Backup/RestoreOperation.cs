//-----------------------------------------------------------------------
// <copyright file="RestoreOperation.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.IO;
using Microsoft.Isam.Esent.Interop;
using Raven.Abstractions.Logging;
using Raven.Database.Config;
using Raven.Database.Extensions;
using Raven.Abstractions.Data;
using System.Linq;

namespace Raven.Storage.Esent.Backup
{
    //TODO : refactor this class to use Raven.Database.Storage.BaseRestoreOperation as base class
    // (this will decrease code duplication between this class and similar class in Voron storage implementations)
	public class RestoreOperation
	{
		private static readonly ILog log = LogManager.GetCurrentClassLogger();

		private readonly Action<string> output;
		private readonly bool defrag;
		private readonly string backupLocation;

		private readonly InMemoryRavenConfiguration configuration;
		private string databaseLocation { get { return configuration.DataDirectory.ToFullPath(); } }
		private string indexLocation { get { return configuration.IndexStoragePath.ToFullPath(); } }

		public RestoreOperation(string backupLocation, InMemoryRavenConfiguration configuration, Action<string> output, bool defrag)
		{
			this.output = output;
			this.defrag = defrag;
			this.backupLocation = backupLocation.ToFullPath();
			this.configuration = configuration;
		}

		public void Execute()
		{
			if (File.Exists(Path.Combine(backupLocation, "RavenDB.Backup")) == false)
			{
				output("Error: " + backupLocation + " doesn't look like a valid backup");
				output("Error: Restore Canceled");
				throw new InvalidOperationException(backupLocation + " doesn't look like a valid backup");
			}

			if (Directory.Exists(databaseLocation) && Directory.GetFileSystemEntries(databaseLocation).Length > 0)
			{
				output("Error: Database already exists, cannot restore to an existing database.");
				output("Error: Restore Canceled");
				throw new IOException("Database already exists, cannot restore to an existing database.");
			}

			if (Directory.Exists(databaseLocation) == false)
				Directory.CreateDirectory(databaseLocation);

			if (Directory.Exists(indexLocation) == false)
				Directory.CreateDirectory(indexLocation);

			var logsPath = databaseLocation;

			if (!string.IsNullOrWhiteSpace(configuration.Settings[Constants.RavenLogsPath]))
			{
				logsPath = configuration.Settings[Constants.RavenLogsPath].ToFullPath();

				if (Directory.Exists(logsPath) == false)
				{
					Directory.CreateDirectory(logsPath);
				}
			}

			Directory.CreateDirectory(Path.Combine(logsPath, "logs"));
			Directory.CreateDirectory(Path.Combine(logsPath, "temp"));
			Directory.CreateDirectory(Path.Combine(logsPath, "system"));

			CombineIncrementalBackups();

			CopyAll(new DirectoryInfo(Path.Combine(backupLocation, "IndexDefinitions")),
				new DirectoryInfo(Path.Combine(databaseLocation, "IndexDefinitions")));

			CopyIndexes();

			var dataFilePath = Path.Combine(databaseLocation, "Data");

			bool hideTerminationException = false;
			JET_INSTANCE instance;
			TransactionalStorage.CreateInstance(out instance, "restoring " + Guid.NewGuid());
			try
			{
				new TransactionalStorageConfigurator(configuration, null).ConfigureInstance(instance, databaseLocation);
				Api.JetRestoreInstance(instance, backupLocation, databaseLocation, RestoreStatusCallback);
				var fileThatGetsCreatedButDoesntSeemLikeItShould =
					new FileInfo(
						Path.Combine(
							new DirectoryInfo(databaseLocation).Parent.FullName, new DirectoryInfo(databaseLocation).Name + "Data"));

				TransactionalStorage.DisableIndexChecking(instance);

				if (fileThatGetsCreatedButDoesntSeemLikeItShould.Exists)
				{
					fileThatGetsCreatedButDoesntSeemLikeItShould.MoveTo(dataFilePath);
				}

				if (defrag)
				{
					output("Esent Restore: Begin Database Compaction");
					TransactionalStorage.Compact(configuration, CompactStatusCallback);
					output("Esent Restore: Database Compaction Completed");
				}
			}
			catch(Exception e)
			{
				output("Esent Restore: Failure! Could not restore database!");
				output(e.ToString());
				log.WarnException("Could not complete restore", e);
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

		private void CopyIndexes()
		{
			var directories = Directory.GetDirectories(backupLocation, "Inc*")
				.OrderByDescending(dir => dir)
				.ToList();

			if (directories.Count == 0)
			{
				foreach (var backupIndex in Directory.GetDirectories(Path.Combine(backupLocation, "Indexes")))
				{
					var indexName = Path.GetFileName(backupIndex);
					var indexPath = Path.Combine(indexLocation, indexName);

					try
					{
						CopyAll(new DirectoryInfo(backupIndex), new DirectoryInfo(indexPath));
					}
					catch (Exception ex)
					{
						ForceIndexReset(indexPath, indexName, ex);
					}
				}

				return;
			}

			var latestIncrementalBackupDirectory = directories.First();
			if(Directory.Exists(Path.Combine(latestIncrementalBackupDirectory, "Indexes")) == false)
				return;

			directories.Add(backupLocation); // add the root (first full backup) to the end of the list (last place to look for)

			foreach (var index in Directory.GetDirectories(Path.Combine(latestIncrementalBackupDirectory, "Indexes")))
			{
				var indexName = Path.GetFileName(index);
				var indexPath = Path.Combine(indexLocation, indexName);

				try
				{
					var filesList = File.ReadAllLines(Path.Combine(index, "index-files.required-for-index-restore"))
						.Where(x=>string.IsNullOrEmpty(x) == false)
						.Reverse();
					
					output("Copying Index: " + indexName);

					if (Directory.Exists(indexPath) == false)
						Directory.CreateDirectory(indexPath);

					foreach (var neededFile in filesList)
					{
						var found = false;

						foreach (var directory in directories)
						{
							var possiblePathToFile = Path.Combine(directory, indexName, neededFile);
							if (File.Exists(possiblePathToFile) == false) 
								continue;

							found = true;
							File.Copy(possiblePathToFile, Path.Combine(indexPath, neededFile));
							break;
						}

						if(found == false)
							output(string.Format("Error: File \"{0}\" is missing from index {1}", neededFile, indexName));
					}
				}
				catch (Exception ex)
				{
					ForceIndexReset(indexPath, indexName, ex);
				}
			}
		}

		private void ForceIndexReset(string indexPath, string indexName, Exception ex)
		{
			if (Directory.Exists(indexPath))
				IOExtensions.DeleteDirectory(indexPath); // this will force index reset

			output(
				string.Format(
					"Error: Index {0} could not be restored. All already copied index files was deleted. " +
					"Index will be recreated after launching Raven instance. Thrown exception:{1}{2}",
					indexName, Environment.NewLine, ex));
		}

		private void CombineIncrementalBackups()
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
		}

		private JET_err RestoreStatusCallback(JET_SESID sesid, JET_SNP snp, JET_SNT snt, object data)
		{
			output(string.Format("Esent Restore: {0} {1} {2}", snp, snt, data));
			Console.WriteLine("Esent Restore: {0} {1} {2}", snp, snt, data);

			return JET_err.Success;
		}

		private JET_err CompactStatusCallback(JET_SESID sesid, JET_SNP snp, JET_SNT snt, object data)
		{
			output(string.Format("Esent Compact: {0} {1} {2}", snp, snt, data));
			Console.WriteLine("Esent Compact: {0} {1} {2}", snp, snt, data);
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
