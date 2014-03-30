// -----------------------------------------------------------------------
//  <copyright file="BaseBackupOperation.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Logging;
using Raven.Database.Backup;
using Raven.Database.Extensions;
using Raven.Json.Linq;

namespace Raven.Database.Storage
{
    public abstract class BaseBackupOperation
    {
        protected static readonly ILog log = LogManager.GetCurrentClassLogger();

        protected readonly DocumentDatabase database;
        protected readonly string backupSourceDirectory;
        protected string backupDestinationDirectory;
        protected bool incrementalBackup;
        protected readonly DatabaseDocument databaseDocument;

        protected BaseBackupOperation(DocumentDatabase database, string backupSourceDirectory, string backupDestinationDirectory, bool incrementalBackup, DatabaseDocument databaseDocument)
        {
            if (databaseDocument == null) throw new ArgumentNullException("databaseDocument");
            if (database == null) throw new ArgumentNullException("database");
            if (backupSourceDirectory == null) throw new ArgumentNullException("backupSourceDirectory");
            if (backupDestinationDirectory == null) throw new ArgumentNullException("backupDestinationDirectory");

            this.database = database;
            this.backupSourceDirectory = backupSourceDirectory.ToFullPath();
            this.backupDestinationDirectory = backupDestinationDirectory.ToFullPath();
            this.incrementalBackup = incrementalBackup;
            this.databaseDocument = databaseDocument;
        }

        protected abstract bool BackupAlreadyExists { get; }

        protected abstract void ExecuteBackup(string backupPath, bool isIncrementalBackup);

         protected virtual void OperationFinished()
         {
             
         }

        public void Execute()
        {
            try
            {
                string incrementalTag = null;

                log.Info("Starting backup of '{0}' to '{1}'", backupSourceDirectory, backupDestinationDirectory);
                UpdateBackupStatus(
                    string.Format("Started backup process. Backing up data to directory = '{0}'",
                                  backupDestinationDirectory), BackupStatus.BackupMessageSeverity.Informational);

                if (BackupAlreadyExists) // trying to backup to an existing backup folder
                {
                    if (!incrementalBackup)
                        throw new InvalidOperationException("Denying request to perform a full backup to an existing backup folder. Try doing an incremental backup instead.");

                    while (true)
                    {
                        incrementalTag = SystemTime.UtcNow.ToString("Inc yyyy-MM-dd HH-mm-ss");
                        backupDestinationDirectory = Path.Combine(backupDestinationDirectory, incrementalTag);

                        if (Directory.Exists(backupDestinationDirectory) == false)
                            break;
                        Thread.Sleep(100); // wait until the second changes, should only even happen in tests
                    }
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

                database.IndexStorage.Backup(backupDestinationDirectory);

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

                ExecuteBackup(backupDestinationDirectory, incrementalBackup);

                if (databaseDocument != null)
                    File.WriteAllText(Path.Combine(backupDestinationDirectory, "Database.Document"), RavenJObject.FromObject(databaseDocument).ToString());

                OperationFinished();

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

        protected void CompleteBackup()
        {
            try
            {
                log.Info("Backup completed");
                var jsonDocument = database.Documents.Get(BackupStatus.RavenBackupStatusDocumentKey, null);
                if (jsonDocument == null)
                    return;

                var backupStatus = jsonDocument.DataAsJson.JsonDeserialization<BackupStatus>();
                backupStatus.IsRunning = false;
                backupStatus.Completed = SystemTime.UtcNow;
                database.Documents.Put(BackupStatus.RavenBackupStatusDocumentKey, null, RavenJObject.FromObject(backupStatus),
                             jsonDocument.Metadata,
                             null);
            }
            catch (Exception e)
            {
                log.WarnException("Failed to update completed backup status, will try deleting document", e);
                try
                {
                    database.Documents.Delete(BackupStatus.RavenBackupStatusDocumentKey, null, null);
                }
                catch (Exception ex)
                {
                    log.WarnException("Failed to remove out of date backup status", ex);
                }
            }
        }

        protected void UpdateBackupStatus(string newMsg, BackupStatus.BackupMessageSeverity severity)
        {
            try
            {
                log.Info(newMsg);
                var jsonDocument = database.Documents.Get(BackupStatus.RavenBackupStatusDocumentKey, null);
                if (jsonDocument == null)
                    return;
                var backupStatus = jsonDocument.DataAsJson.JsonDeserialization<BackupStatus>();
                backupStatus.Messages.Add(new BackupStatus.BackupMessage
                {
                    Message = newMsg,
                    Timestamp = SystemTime.UtcNow,
                    Severity = severity
                });
                database.Documents.Put(BackupStatus.RavenBackupStatusDocumentKey, null, RavenJObject.FromObject(backupStatus),
                             jsonDocument.Metadata,
                             null);
            }
            catch (Exception e)
            {
                log.WarnException("Failed to update backup status", e);
            }
        }
    }
}