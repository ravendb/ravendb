// -----------------------------------------------------------------------
//  <copyright file="BaseBackupOperation.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.IO;
using System.Security.AccessControl;
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
        protected readonly ResourceBackupState state;
        protected readonly CancellationToken cancellationToken;

        protected BaseBackupOperation(DocumentDatabase database, string backupSourceDirectory, string backupDestinationDirectory, 
            bool incrementalBackup, DatabaseDocument databaseDocument, ResourceBackupState state, CancellationToken cancellationToken)
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
            this.state = state;
            this.cancellationToken = cancellationToken;
        }

        protected abstract bool BackupAlreadyExists { get; }

        protected abstract void ExecuteBackup(string backupPath, bool isIncrementalBackup);

        protected virtual void OperationFinishedSuccessfully()
        {
             state.MarkCompleted();
        }

        public void Execute()
        {
            try
            {
                log.Info("Starting backup of '{0}' to '{1}'", backupSourceDirectory, backupDestinationDirectory);
                UpdateBackupStatus(
                    string.Format("Started backup process. Backing up data to directory = '{0}'",
                        backupDestinationDirectory), null, BackupStatus.BackupMessageSeverity.Informational);

                EnsureBackupDestinationExists();

                if (incrementalBackup)
                {
                    var incrementalBackupState = Path.Combine(backupDestinationDirectory, Constants.IncrementalBackupState);

                    if (File.Exists(incrementalBackupState))
                    {
                        var state = RavenJObject.Parse(File.ReadAllText(incrementalBackupState)).JsonDeserialization<IncrementalBackupState>();

                        if (state.ResourceId != database.TransactionalStorage.Id)
                            throw new InvalidOperationException(string.Format("Can't perform an incremental backup to a given folder because it already contains incremental backup data of different database. Existing incremental data origins from '{0}' database.", state.ResourceName));
                    }
                    else
                    {
                        var state = new IncrementalBackupState()
                        {
                            ResourceId = database.TransactionalStorage.Id,
                            ResourceName = database.Name ?? Constants.SystemDatabase
                        };

                        //TODO:  to rollback
                        File.WriteAllText(incrementalBackupState, RavenJObject.FromObject(state).ToString());
                    }

                    if (CanPerformIncrementalBackup())
                    {
                        backupDestinationDirectory = DirectoryForIncrementalBackup();
                        EnsureBackupDestinationExists();
                    }
                    else
                    {
                        incrementalBackup = false; // destination wasn't detected as a backup folder, automatically revert to a full backup if incremental was specified
                    }
                }
                else if (BackupAlreadyExists)
                {
                    throw new InvalidOperationException("Denying request to perform a full backup to an existing backup folder. Try doing an incremental backup instead.");
                }

                UpdateBackupStatus("Backing up indexes..", null, BackupStatus.BackupMessageSeverity.Informational);

                // Make sure we have an Indexes folder in the backup location
                if (!Directory.Exists(Path.Combine(backupDestinationDirectory, "Indexes")))
                    Directory.CreateDirectory(Path.Combine(backupDestinationDirectory, "Indexes"));

                var directoryBackups = new List<DirectoryBackup>
                {
                    new DirectoryBackup(Path.Combine(backupSourceDirectory, "IndexDefinitions"),
                        Path.Combine(backupDestinationDirectory, "IndexDefinitions"),
                        Path.Combine(backupSourceDirectory, "Temp" + Guid.NewGuid().ToString("N")), incrementalBackup)
                };

                database.IndexStorage.Backup(backupDestinationDirectory, null, UpdateBackupStatus);

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

                UpdateBackupStatus("Finished indexes backup. Executing data backup..", null, BackupStatus.BackupMessageSeverity.Informational);

                ExecuteBackup(backupDestinationDirectory, incrementalBackup);

                if (databaseDocument != null)
                    File.WriteAllText(Path.Combine(backupDestinationDirectory, Constants.DatabaseDocumentFilename), RavenJObject.FromObject(databaseDocument).ToString());

                OperationFinishedSuccessfully();
            }
            catch (OperationCanceledException e)
            {
                //TODO:  handle rollback ?
                state.MarkCanceled();
            }
            catch (AggregateException e)
            {
                var ne = e.ExtractSingleInnerException();
                UpdateBackupStatus("Failed to complete backup because: " + ne.Message, ne, BackupStatus.BackupMessageSeverity.Error);
                state.MarkFaulted("Failed to complete backup because:" + ne.Message, ne);
            }
            catch (Exception e)
            {
                UpdateBackupStatus("Failed to complete backup because: " + e.Message, e, BackupStatus.BackupMessageSeverity.Error);
                state.MarkFaulted("Failed to complete backup because: " + e.Message);
            }
            finally
            {
                CompleteBackup();
            }
        }

        private void EnsureBackupDestinationExists()
        {
            if (Directory.Exists(backupDestinationDirectory))
            {
                var writeTestFile = Path.Combine(backupDestinationDirectory, "write-permission-test");
                try
                {
                    File.Create(writeTestFile).Dispose();
                }
                catch (UnauthorizedAccessException)
                {
                    throw new UnauthorizedAccessException(string.Format("You don't have write access to the path {0}", backupDestinationDirectory));
                }
                IOExtensions.DeleteFile(writeTestFile);
            }
            else
                Directory.CreateDirectory(backupDestinationDirectory); // will throw UnauthorizedAccessException if a user doesn't have write permission
        }

        /// <summary>
        /// The key of this check is to determinate if incremental backup can be executed 
        /// 
        /// For voron: first and subsequent backups are incremental 
        /// For esent: first backup can't be incremental - when user requested incremental esent backup and target directory is empty, we have to start with full backup.
        /// </summary>
        /// <returns></returns>
        protected abstract bool CanPerformIncrementalBackup();

        protected string DirectoryForIncrementalBackup()
        {
            while (true)
            {
                var incrementalTag = SystemTime.UtcNow.ToString("Inc yyyy-MM-dd HH-mm-ss");
                var backupDirectory = Path.Combine(backupDestinationDirectory, incrementalTag);

                if (Directory.Exists(backupDirectory) == false)
                {
                    return backupDirectory;
                }
                Thread.Sleep(100); // wait until the second changes, should only even happen in tests
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
                database.RaiseBackupComplete();
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

        protected void UpdateBackupStatus(string newMsg, Exception exception, BackupStatus.BackupMessageSeverity severity)
        {
            try
            {
                if (exception != null)
                {
                    log.WarnException(newMsg, exception);
                }
                else
                {
                    log.Info(newMsg);
                }
                
                var jsonDocument = database.Documents.Get(BackupStatus.RavenBackupStatusDocumentKey, null);
                if (jsonDocument == null)
                    return;
                var backupStatus = jsonDocument.DataAsJson.JsonDeserialization<BackupStatus>();
                backupStatus.Messages.Add(new BackupStatus.BackupMessage
                {
                    Message = newMsg,
                    Timestamp = SystemTime.UtcNow,
                    Severity = severity,
                    Details = exception?.ExceptionToString(null)
                });
                database.Documents.Put(BackupStatus.RavenBackupStatusDocumentKey, null, RavenJObject.FromObject(backupStatus),
                             jsonDocument.Metadata,
                             null);

                state.MarkProgress(newMsg);
            }
            catch (Exception e)
            {
                log.WarnException("Failed to update backup status", e);
            }
        }
    }
}
