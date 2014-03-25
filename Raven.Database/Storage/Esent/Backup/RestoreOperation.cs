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
using Raven.Storage.Esent;

namespace Raven.Database.Storage.Esent.Backup
{
	public class RestoreOperation : BaseRestoreOperation
	{
		private readonly bool defrag;

        public RestoreOperation(string backupLocation, InMemoryRavenConfiguration configuration, Action<string> operationOutputCallback, bool defrag)
            : base(backupLocation, configuration, operationOutputCallback)
		{
			this.defrag = defrag;
		}

		public override void Execute()
		{
            var logsPath = ValidateRestorePreconditionsAndReturnLogsPath("RavenDB.Backup");
			
			Directory.CreateDirectory(Path.Combine(logsPath, "logs"));
			Directory.CreateDirectory(Path.Combine(logsPath, "temp"));
			Directory.CreateDirectory(Path.Combine(logsPath, "system"));

			CombineIncrementalBackups();

			CopyIndexDefinitions();

			CopyIndexes();

			var dataFilePath = Path.Combine(databaseLocation, "Data");

			bool hideTerminationException = false;
			JET_INSTANCE instance;
			TransactionalStorage.CreateInstance(out instance, "restoring " + Guid.NewGuid());
			try
			{
			    configuration.Settings["Raven/Esent/LogsPath"] = logsPath;
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
	}
}
