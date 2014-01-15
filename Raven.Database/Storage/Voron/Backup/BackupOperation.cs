using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Logging;
using Raven.Database.Backup;
using Raven.Database.Extensions;
using Raven.Database.Storage.Voron.Impl;
using Raven.Json.Linq;
using Voron;
using Voron.Impl.Backup;

namespace Raven.Database.Storage.Voron.Backup
{
    public class BackupOperation
    {
        private string backupFilePath;
        private readonly DocumentDatabase database;
        private readonly string backupSourceDirectory;

        private static readonly ILog log = LogManager.GetCurrentClassLogger();

        private string backupDestinationDirectory;
	    private readonly StorageEnvironment env;
        private bool incrementalBackup;
	    private readonly DatabaseDocument databaseDocument;

	    public BackupOperation(DocumentDatabase database, string backupSourceDirectory, string backupDestinationDirectory, StorageEnvironment env, bool incrementalBackup, DatabaseDocument databaseDocument)
        {
			if (databaseDocument == null) throw new ArgumentNullException("databaseDocument");
			if (database == null) throw new ArgumentNullException("database");
			if (backupSourceDirectory == null) throw new ArgumentNullException("backupSourceDirectory");
            if (backupDestinationDirectory == null) throw new ArgumentNullException("backupDestinationDirectory");
            if (env == null) throw new ArgumentNullException("env");

            this.database = database;
            this.backupSourceDirectory = backupSourceDirectory;
            this.backupDestinationDirectory = backupDestinationDirectory;
			this.env = env;
            this.incrementalBackup = incrementalBackup;
			this.databaseDocument = databaseDocument;
        }

        public void Execute()
        {
            try
            {
                string incrementalTag = null;
                backupDestinationDirectory = backupDestinationDirectory.ToFullPath();
                backupFilePath = Path.Combine(backupDestinationDirectory.Trim(), BackupMethods.Filename);

                UpdateBackupStatus(string.Format("Started backup process. Backing up data to file path = '{0}'", backupFilePath),BackupStatus.BackupMessageSeverity.Informational);
                if (Directory.Exists(backupDestinationDirectory) && File.Exists(backupFilePath))
                {
                    if(!incrementalBackup)
                        throw new InvalidOperationException("Denying request to perform a full backup to an existing backup folder. Try doing an incremental backup instead.");

                    incrementalTag = SystemTime.UtcNow.ToString("Inc yyyy-MM-dd hh-mm-ss");
                    backupDestinationDirectory = Path.Combine(backupDestinationDirectory, incrementalTag);
					backupFilePath = Path.Combine(backupDestinationDirectory.Trim(), BackupMethods.Filename);
                }
                else
                {
                    incrementalBackup = false; // destination wasn't detected as a backup folder, automatically revert to a full backup if incremental was specified
                }

                UpdateBackupStatus(string.Format("Backing up indexes.."), BackupStatus.BackupMessageSeverity.Informational);

                // Make sure we have an Indexes folder in the backup location
                if (!Directory.Exists(Path.Combine(backupDestinationDirectory, "Indexes")))
                    Directory.CreateDirectory(Path.Combine(backupDestinationDirectory, "Indexes"));

                var directoryBackups = new List<DirectoryBackup>
				{
					new DirectoryBackup(Path.Combine(backupSourceDirectory, "IndexDefinitions"),
                                        Path.Combine(backupDestinationDirectory, "IndexDefinitions"), 
                                        Path.Combine(backupSourceDirectory, "Temp" + Guid.NewGuid().ToString("N")), incrementalBackup)
				};

                database.IndexStorage.Backup(backupDestinationDirectory, null);				

                var progressNotifier = new ProgressNotifier();
                foreach (var directoryBackup in directoryBackups)
                {
                    directoryBackup.Notify += UpdateBackupStatus;
                    var backupSize = directoryBackup.Prepare();
                    progressNotifier.TotalBytes += backupSize;
                }

                foreach (var directoryBackup in directoryBackups)
                {
                    directoryBackup.Execute(progressNotifier);
                }

                UpdateBackupStatus(string.Format("Finished indexes backup. Executing data backup.."), BackupStatus.BackupMessageSeverity.Informational);
				ExecuteBackup(backupFilePath,incrementalBackup);

				if (databaseDocument != null)
					File.WriteAllText(Path.Combine(backupDestinationDirectory, "Database.Document"), RavenJObject.FromObject(databaseDocument).ToString());

			}
            catch (AggregateException e)
            {
                var ne = e.ExtractSingleInnerException();
                log.ErrorException("Failed to complete backup", ne);
                UpdateBackupStatus("Failed to complete backup because: " + ne.Message, BackupStatus.BackupMessageSeverity.Error);
            }
            catch (Exception e)
            {
                log.ErrorException("Failed to complete backup", e);
                UpdateBackupStatus("Failed to complete backup because: " + e.Message, BackupStatus.BackupMessageSeverity.Error);
            }
            finally
            {
                CompleteBackup();
            }
        }

		private void ExecuteBackup(string backupPath, bool isIncrementalBackup)
		{
			if (String.IsNullOrWhiteSpace(backupPath)) throw new ArgumentNullException("backupPath");

			if (isIncrementalBackup)
				BackupMethods.Incremental.ToFile(env, backupPath);
			else
				BackupMethods.Full.ToFile(env, backupPath);
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

        private void CompleteBackup()
        {
            try
            {
                log.Info("Backup completed");
                var jsonDocument = database.Get(BackupStatus.RavenBackupStatusDocumentKey, null);
                if (jsonDocument == null)
                    return;

                var backupStatus = jsonDocument.DataAsJson.JsonDeserialization<BackupStatus>();
                backupStatus.IsRunning = false;
                backupStatus.Completed = SystemTime.UtcNow;
                database.Put(BackupStatus.RavenBackupStatusDocumentKey, null, RavenJObject.FromObject(backupStatus),
                             jsonDocument.Metadata,
                             null);
            }
            catch (Exception e)
            {
                log.WarnException("Failed to update completed backup status, will try deleting document", e);
                try
                {
                    database.Delete(BackupStatus.RavenBackupStatusDocumentKey, null, null);
                }
                catch (Exception ex)
                {
                    log.WarnException("Failed to remove out of date backup status", ex);
                }
            }
        }

        private void UpdateBackupStatus(string newMsg, BackupStatus.BackupMessageSeverity severity)
        {
            try
            {
                log.Info(newMsg);
                var jsonDocument = database.Get(BackupStatus.RavenBackupStatusDocumentKey, null);
                if (jsonDocument == null)
                    return;
                var backupStatus = jsonDocument.DataAsJson.JsonDeserialization<BackupStatus>();
                backupStatus.Messages.Add(new BackupStatus.BackupMessage
                {
                    Message = newMsg,
                    Timestamp = SystemTime.UtcNow,
                    Severity = severity
                });
                database.Put(BackupStatus.RavenBackupStatusDocumentKey, null, RavenJObject.FromObject(backupStatus), jsonDocument.Metadata,
                             null);
            }
            catch (Exception e)
            {
                log.WarnException("Failed to update backup status", e);
            }
        }
    }
}
