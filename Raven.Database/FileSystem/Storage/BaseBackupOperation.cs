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
using Raven.Database.Storage;
using Raven.Json.Linq;

using Voron.Impl.Backup;

namespace Raven.Database.FileSystem.Storage
{
    public abstract class BaseBackupOperation
    {
        protected static readonly ILog log = LogManager.GetCurrentClassLogger();

        protected readonly RavenFileSystem filesystem;
        protected readonly string backupSourceDirectory;
        protected string backupDestinationDirectory;
        protected bool incrementalBackup;
        protected readonly FileSystemDocument filesystemDocument;
        protected readonly ResourceBackupState state;
        protected readonly CancellationToken token;

        protected BaseBackupOperation(RavenFileSystem filesystem, string backupSourceDirectory, string backupDestinationDirectory, bool incrementalBackup, 
            FileSystemDocument filesystemDocument, ResourceBackupState state, CancellationToken token)
        {
            if (filesystem == null) throw new ArgumentNullException("filesystem");
            if (filesystemDocument == null) throw new ArgumentNullException("filesystemDocument");
            if (backupSourceDirectory == null) throw new ArgumentNullException("backupSourceDirectory");
            if (backupDestinationDirectory == null) throw new ArgumentNullException("backupDestinationDirectory");

            this.filesystem = filesystem;
            this.backupSourceDirectory = backupSourceDirectory.ToFullPath();
            this.backupDestinationDirectory = backupDestinationDirectory.ToFullPath();
            this.incrementalBackup = incrementalBackup;
            this.filesystemDocument = filesystemDocument;
            this.state = state;
            this.token = token;
        }

        protected abstract bool BackupAlreadyExists { get; }

        protected abstract void ExecuteBackup(string backupPath, bool isIncrementalBackup, CancellationToken token);

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

                        if (state.ResourceId != filesystem.Storage.Id)
                            throw new InvalidOperationException(string.Format("Can't perform an incremental backup to a given folder because it already contains incremental backup data of different file system. Existing incremental data origins from '{0}' file system.", state.ResourceName));
                    }
                    else
                    {
                        var state = new IncrementalBackupState()
                        {
                            ResourceId = filesystem.Storage.Id,
                            ResourceName = filesystem.Name
                        };

                        File.WriteAllText(incrementalBackupState, RavenJObject.FromObject(state).ToString());
                    }

                    token.ThrowIfCancellationRequested();

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

                token.ThrowIfCancellationRequested();

                UpdateBackupStatus(string.Format("Backing up indexes.."), null, BackupStatus.BackupMessageSeverity.Informational);

                // Make sure we have an Indexes folder in the backup location
                if (!Directory.Exists(Path.Combine(backupDestinationDirectory, "Indexes")))
                    Directory.CreateDirectory(Path.Combine(backupDestinationDirectory, "Indexes"));

                token.ThrowIfCancellationRequested();

                filesystem.Search.Backup(backupDestinationDirectory, token);

                UpdateBackupStatus(string.Format("Finished indexes backup. Executing data backup.."), null, BackupStatus.BackupMessageSeverity.Informational);

                ExecuteBackup(backupDestinationDirectory, incrementalBackup, token);

                if (filesystemDocument != null)
                    File.WriteAllText(Path.Combine(backupDestinationDirectory, Constants.FilesystemDocumentFilename), RavenJObject.FromObject(filesystemDocument).ToString());

                token.ThrowIfCancellationRequested();

                OperationFinishedSuccessfully();

            }
            catch (OperationCanceledException e)
            {
                File.WriteAllText(Path.Combine(backupDestinationDirectory, Constants.BackupFailureMarker), e.Message);
                UpdateBackupStatus("Backup was canceled", null, BackupStatus.BackupMessageSeverity.Error);
                state.MarkCanceled();
            }
            catch (AggregateException e)
            {
                var ne = e.ExtractSingleInnerException();
                UpdateBackupStatus("Failed to complete backup because: " + ne.Message, ne, BackupStatus.BackupMessageSeverity.Error);
                state.MarkFaulted("Failed to complete backup because: " + ne.Message, ne);

                File.WriteAllText(Path.Combine(backupDestinationDirectory, Constants.BackupFailureMarker), ne.Message);
            }
            catch (Exception e)
            {
                UpdateBackupStatus("Failed to complete backup because: " + e.Message, e, BackupStatus.BackupMessageSeverity.Error);
                state.MarkFaulted("Failed to complete backup because: " + e.Message, e);

                File.WriteAllText(Path.Combine(backupDestinationDirectory, Constants.BackupFailureMarker), e.Message);
            }
            finally
            {
                CompleteBackup();
            }
        }

        /// <summary>
        /// The key of this check is to determinate if incremental backup can be executed 
        /// 
        /// For voron: first and subsequent backups are incremental 
        /// For esent: first backup can't be incremental - when user requested incremental esent backup and target directory is empty, we have to start with full backup.
        /// </summary>
        /// <returns></returns>
        protected abstract bool CanPerformIncrementalBackup();

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

        private BackupStatus GetBackupStatus()
        {
            RavenJObject backupStatus = null;
            try
            {
                filesystem.Storage.Batch(accessor => backupStatus = accessor.GetConfig(BackupStatus.RavenBackupStatusDocumentKey));
            }
            catch (FileNotFoundException)
            {
                // config doesn't exists
                return null;
            }
            return backupStatus.JsonDeserialization<BackupStatus>();
        }

        private void SetBackupStatus(BackupStatus backupStatus)
        {
            filesystem.Storage.Batch(accessor => accessor.SetConfig(BackupStatus.RavenBackupStatusDocumentKey, RavenJObject.FromObject(backupStatus)));
        }

        private void DeleteBackupStatus()
        {
            filesystem.Storage.Batch(accessor => accessor.DeleteConfig(BackupStatus.RavenBackupStatusDocumentKey));
        }

        protected void CompleteBackup()
        {
            try
            {
                log.Info("Backup completed");
                var backupStatus = GetBackupStatus();
                if (backupStatus == null)
                    return;

                backupStatus.IsRunning = false;
                backupStatus.Completed = SystemTime.UtcNow;
                SetBackupStatus(backupStatus);
            }
            catch (Exception e)
            {
                log.WarnException("Failed to update completed backup status, will try deleting document", e);
                try
                {
                    DeleteBackupStatus();
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
                
                var backupStatus = GetBackupStatus();
                if (backupStatus == null)
                    return;

                backupStatus.Messages.Add(new BackupStatus.BackupMessage
                {
                    Message = newMsg,
                    Timestamp = SystemTime.UtcNow,
                    Severity = severity,
                    Details = exception?.ExceptionToString(null)
                });
                SetBackupStatus(backupStatus);
                state.MarkProgress(newMsg);
            }
            catch (Exception e)
            {
                log.WarnException("Failed to update backup status", e);
            }
        }
    }
}
