using System;
using System.IO;
using System.Threading;
using Raven.Abstractions;
using Raven.Abstractions.Counters;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Logging;
using Raven.Database.Extensions;
using Raven.Database.Storage;
using Raven.Json.Linq;
using Voron;
using Voron.Impl.Backup;

namespace Raven.Database.Counters.Backup
{
    public class BackupOperation
    {
        private readonly CounterStorage storage;
        private readonly string backupDestinationDirectory;
        private readonly StorageEnvironment env;
        private readonly bool incrementalBackup;
        private readonly CounterStorageDocument counterDocument;

        private static readonly ILog _log = LogManager.GetCurrentClassLogger();
        private readonly string backupFilename;
        private readonly string backupSourceDirectory;

        public BackupOperation(CounterStorage storage, string backupSourceDirectory, string backupDestinationDirectory, StorageEnvironment env, bool incrementalBackup, CounterStorageDocument counterDocument)
        {
            this.storage = storage;
            this.backupDestinationDirectory = backupDestinationDirectory;
            this.env = env;
            this.incrementalBackup = incrementalBackup;
            this.counterDocument = counterDocument;
            this.backupSourceDirectory = backupSourceDirectory;
            backupFilename = counterDocument.Id + ".Voron.Backup";

            if (incrementalBackup)
                PrepareForIncrementalBackup();
        }

        public void Execute()
        {
            try
            {
                _log.Info("Starting backup of '{0}' to '{1}'", backupSourceDirectory, backupDestinationDirectory);
                UpdateBackupStatus(
                    string.Format("Started backup process. Backing up data to directory = '{0}'",
                                  backupDestinationDirectory), null, BackupStatus.BackupMessageSeverity.Informational);

                if (incrementalBackup)
                {
                    var backupDestinationIncrementalDirectory = DirectoryForIncrementalBackup();
                    EnsureBackupDestinationExists(backupDestinationIncrementalDirectory);

                    // TODO: cancelation tokens in counters backups are not supported for now
                    BackupMethods.Incremental.ToFile(env, Path.Combine(backupDestinationIncrementalDirectory, backupFilename), CancellationToken.None,
                        infoNotify: s => UpdateBackupStatus(s, null, BackupStatus.BackupMessageSeverity.Informational));
                }
                else
                {
                    // TODO: cancelation tokens in counters backups are not supported for now
                    EnsureBackupDestinationExists();
                    BackupMethods.Full.ToFile(env, Path.Combine(backupDestinationDirectory, backupFilename), CancellationToken.None, 
                        infoNotify: s => UpdateBackupStatus(s, null, BackupStatus.BackupMessageSeverity.Informational));
                }
            }
            finally
            {
                CompleteBackup();
            }
        }

        private void PrepareForIncrementalBackup()
        {
            if (Directory.Exists(backupDestinationDirectory) == false)
                Directory.CreateDirectory(backupDestinationDirectory);

            var incrementalBackupState = Path.Combine(backupDestinationDirectory, Constants.IncrementalBackupState);

            if (File.Exists(incrementalBackupState))
            {
                var state = RavenJObject.Parse(File.ReadAllText(incrementalBackupState)).JsonDeserialization<IncrementalBackupState>();

                if (state.ResourceId != storage.ServerId)
                    throw new InvalidOperationException(string.Format("Can't perform an incremental backup to a given folder because it already contains incremental backup data of different database. Existing incremental data origins from '{0}' database.", state.ResourceName));
            }
            else
            {
                var state = new IncrementalBackupState
                {
                    ResourceId = storage.ServerId,
                    ResourceName = counterDocument.Id
                };

                File.WriteAllText(incrementalBackupState, RavenJObject.FromObject(state).ToString());
            }
        }

        private string DirectoryForIncrementalBackup()
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

        private void CompleteBackup()
        {
            try
            {
                _log.Info("Backup completed");
                var backupStatus = GetBackupStatus();
                if (backupStatus == null)
                    return;

                backupStatus.IsRunning = false;
                backupStatus.Completed = SystemTime.UtcNow;
                SetBackupStatus(backupStatus);
            }
            catch (Exception e)
            {
                _log.WarnException("Failed to update completed backup status, will try deleting backup status", e);
                try
                {
                    DeleteBackupStatus();
                }
                catch (Exception ex)
                {
                    _log.WarnException("Failed to remove backup status", ex);
                }
            }
        }

        private void UpdateBackupStatus(string newMsg, string details, BackupStatus.BackupMessageSeverity severity)
        {
            try
            {
                _log.Info(newMsg);
                var backupStatus = GetBackupStatus();
                if (backupStatus == null)
                    return;

                backupStatus.Messages.Add(new BackupStatus.BackupMessage
                {
                    Message = newMsg,
                    Timestamp = SystemTime.UtcNow,
                    Severity = severity,
                    Details = details
                });
                SetBackupStatus(backupStatus);
            }
            catch (Exception e)
            {
                _log.WarnException("Failed to update backup status", e);
            }
        }

        private BackupStatus GetBackupStatus()
        {
            using (var reader = storage.CreateReader())
            {
                return reader.GetBackupStatus();
            }
        }

        private void SetBackupStatus(BackupStatus backupStatus)
        {
            using (var writer = storage.CreateWriter())
            {
                writer.SaveBackupStatus(backupStatus);
            }
        }

        private void DeleteBackupStatus()
        {
            using (var writer = storage.CreateWriter())
            {
                writer.DeleteBackupStatus();
            }
        }

        private void EnsureBackupDestinationExists(string backupDestination = null)
        {
            var path = backupDestination ?? backupDestinationDirectory;
            if (Directory.Exists(path))
            {
                var writeTestFile = Path.Combine(path, "write-permission-test");
                try
                {
                    File.Create(writeTestFile).Dispose();
                }
                catch (UnauthorizedAccessException)
                {
                    throw new UnauthorizedAccessException(string.Format("You don't have write access to the path {0}", path));
                }
                IOExtensions.DeleteFile(writeTestFile);
            }
            else
                Directory.CreateDirectory(path); // will throw UnauthorizedAccessException if a user doesn't have write permission
        }

        public bool BackupAlreadyExists
        {
            get { return Directory.Exists(backupDestinationDirectory) && File.Exists(Path.Combine(backupDestinationDirectory.Trim(), backupFilename)); }
        }
    }
}
