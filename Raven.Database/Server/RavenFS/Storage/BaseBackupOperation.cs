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
using Raven.Abstractions.FileSystem;
using Raven.Abstractions.Logging;
using Raven.Database.Extensions;
using Raven.Json.Linq;

using Voron.Impl.Backup;

namespace Raven.Database.Server.RavenFS.Storage
{
    public abstract class BaseBackupOperation
    {
        protected static readonly ILog log = LogManager.GetCurrentClassLogger();

        protected readonly DocumentDatabase systemDatabase;
        protected readonly RavenFileSystem filesystem;
        protected readonly string backupSourceDirectory;
        protected string backupDestinationDirectory;
        protected bool incrementalBackup;
        protected readonly FileSystemDocument filesystemDocument;

        protected BaseBackupOperation(DocumentDatabase systemDatabase, RavenFileSystem filesystem, string backupSourceDirectory, string backupDestinationDirectory, bool incrementalBackup, FileSystemDocument filesystemDocument)
        {
            if (filesystem == null) throw new ArgumentNullException("filesystem");
            if (filesystemDocument == null) throw new ArgumentNullException("filesystemDocument");
            if (systemDatabase == null) throw new ArgumentNullException("systemDatabase");
            if (backupSourceDirectory == null) throw new ArgumentNullException("backupSourceDirectory");
            if (backupDestinationDirectory == null) throw new ArgumentNullException("backupDestinationDirectory");

            this.systemDatabase = systemDatabase;
            this.filesystem = filesystem;
            this.backupSourceDirectory = backupSourceDirectory.ToFullPath();
            this.backupDestinationDirectory = backupDestinationDirectory.ToFullPath();
            this.incrementalBackup = incrementalBackup;
            this.filesystemDocument = filesystemDocument;
        }

        protected abstract bool BackupAlreadyExists { get; }

        protected abstract void ExecuteBackup(string backupPath, bool isIncrementalBackup);

         protected virtual void OperationFinished()
         {
             
         }

        protected string BackupStatusDocumentKey
        {
            get
            {
                return BackupStatus.RavenFilesystemBackupStatusDocumentKey(filesystemDocument.Id);
            }
        }

        public void Execute()
        {
            try
            {
                log.Info("Starting backup of '{0}' to '{1}'", backupSourceDirectory, backupDestinationDirectory);
                UpdateBackupStatus(
                    string.Format("Started backup process. Backing up data to directory = '{0}'",
                                  backupDestinationDirectory), null, BackupStatus.BackupMessageSeverity.Informational);

                if (incrementalBackup)
                {
                    if (CanPerformIncrementalBackup())
                    {
                        backupDestinationDirectory = DirectoryForIncrementalBackup();
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

                UpdateBackupStatus(string.Format("Backing up indexes.."), null, BackupStatus.BackupMessageSeverity.Informational);

                // Make sure we have an Indexes folder in the backup location
                if (!Directory.Exists(Path.Combine(backupDestinationDirectory, "Indexes")))
                    Directory.CreateDirectory(Path.Combine(backupDestinationDirectory, "Indexes"));

                filesystem.Search.Backup(backupDestinationDirectory);

                UpdateBackupStatus(string.Format("Finished indexes backup. Executing data backup.."), null, BackupStatus.BackupMessageSeverity.Informational);

                ExecuteBackup(backupDestinationDirectory, incrementalBackup);

                if (filesystemDocument != null)
                    File.WriteAllText(Path.Combine(backupDestinationDirectory, BackupMethods.FilesystemDocumentFilename), RavenJObject.FromObject(filesystemDocument).ToString());

                OperationFinished();

            }
            catch (AggregateException e)
            {
                var ne = e.ExtractSingleInnerException();
                log.ErrorException("Failed to complete backup", ne);
                UpdateBackupStatus("Failed to complete backup because: " + ne.Message, ne.ExceptionToString(null), BackupStatus.BackupMessageSeverity.Error);
            }
            catch (Exception e)
            {
                log.ErrorException("Failed to complete backup", e);
                UpdateBackupStatus("Failed to complete backup because: " + e.Message, e.ExceptionToString(null), BackupStatus.BackupMessageSeverity.Error);
            }
            finally
            {
                CompleteBackup();
            }
        }

        /// <summary>
        /// The key of this check is to determinate if incremental backup can be executed 
        /// 
        /// For voron: first and subsequent backups are incremenetal 
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
                var jsonDocument = systemDatabase.Documents.Get(BackupStatusDocumentKey, null);
                if (jsonDocument == null)
                    return;

                var backupStatus = jsonDocument.DataAsJson.JsonDeserialization<BackupStatus>();
                backupStatus.IsRunning = false;
                backupStatus.Completed = SystemTime.UtcNow;
                systemDatabase.Documents.Put(BackupStatusDocumentKey, null, RavenJObject.FromObject(backupStatus),
                             jsonDocument.Metadata,
                             null);
            }
            catch (Exception e)
            {
                log.WarnException("Failed to update completed backup status, will try deleting document", e);
                try
                {
                    systemDatabase.Documents.Delete(BackupStatusDocumentKey, null, null);
                }
                catch (Exception ex)
                {
                    log.WarnException("Failed to remove out of date backup status", ex);
                }
            }
        }

        protected void UpdateBackupStatus(string newMsg, string details, BackupStatus.BackupMessageSeverity severity)
        {
            try
            {
                log.Info(newMsg);
                var jsonDocument = systemDatabase.Documents.Get(BackupStatusDocumentKey, null);
                if (jsonDocument == null)
                    return;
                var backupStatus = jsonDocument.DataAsJson.JsonDeserialization<BackupStatus>();
                backupStatus.Messages.Add(new BackupStatus.BackupMessage
                {
                    Message = newMsg,
                    Timestamp = SystemTime.UtcNow,
                    Severity = severity,
                    Details = details
                });
                systemDatabase.Documents.Put(BackupStatusDocumentKey, null, RavenJObject.FromObject(backupStatus),
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