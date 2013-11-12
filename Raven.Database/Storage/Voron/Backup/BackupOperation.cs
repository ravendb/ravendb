using System;
using System.Collections.Generic;
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

namespace Raven.Database.Storage.Voron.Backup
{
    public class BackupOperation
    {
        private const string VORON_BACKUP_FILENAME = "RavenDB.Voron.Backup";
        private string backupFilePath;
        private readonly DocumentDatabase database;
        private readonly string backupSourceDirectory;

        private static readonly ILog log = LogManager.GetCurrentClassLogger();

        private string backupDestinationDirectory;
        private readonly TableStorage storage;
        private bool incrementalBackup;

        public BackupOperation(DocumentDatabase database,string backupSourceDirectory,string backupDestinationDirectory, TableStorage storage, bool incrementalBackup)
        {
            if (database == null) throw new ArgumentNullException("database");
            if (backupSourceDirectory == null) throw new ArgumentNullException("backupSourceDirectory");
            if (backupDestinationDirectory == null) throw new ArgumentNullException("backupDestinationDirectory");
            if (storage == null) throw new ArgumentNullException("storage");

            this.database = database;
            this.backupSourceDirectory = backupSourceDirectory;
            this.backupDestinationDirectory = backupDestinationDirectory;
            this.storage = storage;
            this.incrementalBackup = incrementalBackup;
        }

        public void Execute()
        {
            try
            {
                string incrementalTag = null;
                backupDestinationDirectory = backupDestinationDirectory.ToFullPath();
                backupFilePath = Path.Combine(backupDestinationDirectory.Trim(), VORON_BACKUP_FILENAME);

                UpdateBackupStatus(string.Format("Started backup process. Backing up data to file path = '{0}'", backupFilePath),BackupStatus.BackupMessageSeverity.Informational);
                if (Directory.Exists(backupDestinationDirectory) && File.Exists(backupFilePath))
                {
                    if(!incrementalBackup)
                        throw new InvalidOperationException("Denying request to perform a full backup to an existing backup folder. Try doing an incremental backup instead.");

                    incrementalTag = SystemTime.UtcNow.ToString("Inc yyyy-MM-dd hh-mm-ss");
                    backupDestinationDirectory = Path.Combine(backupDestinationDirectory, incrementalTag);
                    backupFilePath = Path.Combine(backupDestinationDirectory.Trim(), VORON_BACKUP_FILENAME);
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

                database.IndexStorage.Backup(backupDestinationDirectory, incrementalTag);

                foreach (var directoryBackup in directoryBackups)
                {
                    directoryBackup.Notify += UpdateBackupStatus;
                    directoryBackup.Prepare();
                }

                foreach (var directoryBackup in directoryBackups)
                {
                    directoryBackup.Execute();
                }

                UpdateBackupStatus(string.Format("Finished indexes backup. Executing data backup.."), BackupStatus.BackupMessageSeverity.Informational);
                using (var backupFileStream = new FileStream(backupFilePath, FileMode.CreateNew))
                {
                    storage.ExecuteBackup(backupFileStream);
                }
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
