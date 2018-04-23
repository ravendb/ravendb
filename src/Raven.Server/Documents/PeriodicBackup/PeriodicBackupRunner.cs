using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Util;
using Raven.Server.Config.Settings;
using Raven.Server.Documents.PeriodicBackup.Azure;
using Raven.Server.Documents.PeriodicBackup.Aws;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.NotificationCenter.Notifications.Details;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Smuggler.Documents;
using Raven.Server.Utils;
using Sparrow.Logging;
using DatabaseSmuggler = Raven.Server.Smuggler.Documents.DatabaseSmuggler;
using System.Collections.Concurrent;
using System.Linq;
using NCrontab.Advanced;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.OngoingTasks;
using Raven.Client.Documents.Smuggler;
using Raven.Client.Json.Converters;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Server.Documents.PeriodicBackup.Restore;
using Raven.Server.Rachis;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Commands.PeriodicBackup;
using Raven.Server.Smuggler.Documents.Data;
using Sparrow.Collections;
using Constants = Raven.Client.Constants;

namespace Raven.Server.Documents.PeriodicBackup
{
    public class PeriodicBackupRunner : IDocumentTombstoneAware, IDisposable
    {
        private readonly Logger _logger;

        private readonly DocumentDatabase _database;
        private readonly ServerStore _serverStore;
        private readonly CancellationTokenSource _cancellationToken;
        private readonly PathSetting _tempBackupPath;

        private readonly ConcurrentDictionary<long, PeriodicBackup> _periodicBackups
            = new ConcurrentDictionary<long, PeriodicBackup>();

        private static readonly Dictionary<string, long> EmptyDictionary = new Dictionary<string, long>();

        private readonly ConcurrentSet<Task> _inactiveRunningPeriodicBackupsTasks = new ConcurrentSet<Task>();
        private bool _disposed;

        // interval can be 2^32-2 milliseconds at most
        // this is the maximum interval acceptable in .Net's threading timer
        public readonly TimeSpan MaxTimerTimeout = TimeSpan.FromMilliseconds(Math.Pow(2, 32) - 2);

        public ICollection<PeriodicBackup> PeriodicBackups => _periodicBackups.Values;

        public static string DateTimeFormat => "yyyy-MM-dd-HH-mm";
        private const string InProgressExtension = ".in-progress";

        public PeriodicBackupRunner(DocumentDatabase database, ServerStore serverStore)
        {
            _database = database;
            _serverStore = serverStore;
            _logger = LoggingSource.Instance.GetLogger<PeriodicBackupRunner>(_database.Name);
            _cancellationToken = CancellationTokenSource.CreateLinkedTokenSource(_database.DatabaseShutdown);

            _tempBackupPath = (_database.Configuration.Storage.TempPath ?? _database.Configuration.Core.DataDirectory).Combine("PeriodicBackupTemp");

            _database.DocumentTombstoneCleaner.Subscribe(this);
            IOExtensions.DeleteDirectory(_tempBackupPath.FullPath);
            Directory.CreateDirectory(_tempBackupPath.FullPath);
        }

        private Timer GetTimer(
            PeriodicBackupConfiguration configuration,
            PeriodicBackupStatus backupStatus)
        {
            var nextBackup = GetNextBackupDetails(configuration, backupStatus);
            if (nextBackup == null)
                return null;

            if (_logger.IsInfoEnabled)
                _logger.Info($"Next {(nextBackup.IsFull ? "full" : "incremental")} " +
                             $"backup is in {nextBackup.TimeSpan.TotalMinutes} minutes");

            var backupTaskDetails = new BackupTaskDetails
            {
                IsFullBackup = nextBackup.IsFull,
                TaskId = configuration.TaskId,
                NextBackup = nextBackup.TimeSpan
            };

            var isValidTimeSpanForTimer = IsValidTimeSpanForTimer(backupTaskDetails.NextBackup);
            var timer = isValidTimeSpanForTimer
                ? new Timer(TimerCallback, backupTaskDetails, backupTaskDetails.NextBackup, Timeout.InfiniteTimeSpan)
                : new Timer(LongPeriodTimerCallback, backupTaskDetails, MaxTimerTimeout, Timeout.InfiniteTimeSpan);

            return timer;
        }

        public NextBackup GetNextBackupDetails(
            DatabaseRecord databaseRecord,
            PeriodicBackupConfiguration configuration,
            PeriodicBackupStatus backupStatus)
        {
            var taskStatus = GetTaskStatus(databaseRecord, configuration, skipErrorLog: true);
            return taskStatus == TaskStatus.Disabled ? null : GetNextBackupDetails(configuration, backupStatus, skipErrorLog: true);
        }

        private NextBackup GetNextBackupDetails(
            PeriodicBackupConfiguration configuration,
            PeriodicBackupStatus backupStatus,
            bool skipErrorLog = false)
        {
            var now = SystemTime.UtcNow;
            var lastFullBackup = backupStatus.LastFullBackupInternal ?? now;
            var lastIncrementalBackup = backupStatus.LastIncrementalBackupInternal ?? backupStatus.LastFullBackupInternal ?? now;
            var nextFullBackup = GetNextBackupOccurrence(configuration.FullBackupFrequency,
                lastFullBackup, configuration, skipErrorLog: skipErrorLog);
            var nextIncrementalBackup = GetNextBackupOccurrence(configuration.IncrementalBackupFrequency,
                lastIncrementalBackup, configuration, skipErrorLog: skipErrorLog);

            if (nextFullBackup == null && nextIncrementalBackup == null)
            {
                var message = "Couldn't schedule next backup " +
                              $"full backup frequency: {configuration.FullBackupFrequency}, " +
                              $"incremental backup frequency: {configuration.IncrementalBackupFrequency}";
                if (string.IsNullOrWhiteSpace(configuration.Name) == false)
                    message += $", backup name: {configuration.Name}";

                _database.NotificationCenter.Add(AlertRaised.Create(
                    _database.Name,
                    "Couldn't schedule next backup, this shouldn't happen",
                    message,
                    AlertType.PeriodicBackup,
                    NotificationSeverity.Warning));

                return null;
            }

            Debug.Assert(configuration.TaskId != 0);

            var isFullBackup = IsFullBackup(backupStatus, configuration, nextFullBackup, nextIncrementalBackup);
            var nextBackupDateTime = GetNextBackupDateTime(nextFullBackup, nextIncrementalBackup);
            var nowLocalTime = now.ToLocalTime();
            var nextBackupTimeSpan = (nextBackupDateTime - nowLocalTime).Ticks <= 0 ? TimeSpan.Zero : nextBackupDateTime - nowLocalTime;

            return new NextBackup
            {
                TimeSpan = nextBackupTimeSpan,
                DateTime = DateTime.UtcNow.Add(nextBackupTimeSpan),
                IsFull = isFullBackup
            };
        }

        private bool IsFullBackup(PeriodicBackupStatus backupStatus,
            PeriodicBackupConfiguration configuration,
            DateTime? nextFullBackup, DateTime? nextIncrementalBackup)
        {
            if (backupStatus.LastFullBackup == null ||
                backupStatus.NodeTag != _serverStore.NodeTag ||
                backupStatus.BackupType != configuration.BackupType ||
                backupStatus.LastEtag == null)
            {
                // Reasons to start a new full backup:
                // 1. there is no previous full backup, we are going to create one now
                // 2. the node which is responsible for the backup was replaced
                // 3. the backup type changed (e.g. from backup to snapshot)
                // 4. last etag wasn't updated

                return true;
            }

            // 1. there is a full backup setup but the next incremental backup wasn't setup
            // 2. there is a full backup setup and the next full backup is before the incremental one
            return nextFullBackup != null &&
                   (nextIncrementalBackup == null || nextFullBackup <= nextIncrementalBackup);
        }

        private async Task RunPeriodicBackup(
            PeriodicBackup periodicBackup,
            bool isFullBackup)
        {
            var totalSw = Stopwatch.StartNew();
            var operationCanceled = false;
            var previousBackupStatus = periodicBackup.BackupStatus;
            var configuration = periodicBackup.Configuration;
            var runningBackupStatus = periodicBackup.RunningBackupStatus = new PeriodicBackupStatus
            {
                TaskId = configuration.TaskId,
                BackupType = configuration.BackupType,
                LastEtag = previousBackupStatus.LastEtag,
                LastFullBackup = previousBackupStatus.LastFullBackup,
                LastIncrementalBackup = previousBackupStatus.LastIncrementalBackup,
                LastFullBackupInternal = previousBackupStatus.LastFullBackupInternal,
                LastIncrementalBackupInternal = previousBackupStatus.LastIncrementalBackupInternal,
                IsFull = isFullBackup
            };

            try
            {
                using (_database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (var tx = context.OpenReadTransaction())
                {
                    var backupToLocalFolder = PeriodicBackupConfiguration.CanBackupUsing(configuration.LocalSettings);
                    var now = DateTime.Now.ToString(DateTimeFormat, CultureInfo.InvariantCulture);

                    if (runningBackupStatus.LocalBackup == null)
                        runningBackupStatus.LocalBackup = new LocalBackup();

                    PathSetting backupDirectory;

                    string folderName;
                    // check if we need to do a new full backup
                    if (isFullBackup ||
                        previousBackupStatus.LastFullBackup == null || // no full backup was previously performed
                        previousBackupStatus.NodeTag != _serverStore.NodeTag || // last backup was performed by a different node
                        previousBackupStatus.BackupType != configuration.BackupType || // backup type has changed
                        previousBackupStatus.LastEtag == null || // last document etag wasn't updated
                        backupToLocalFolder && DirectoryContainsBackupFiles(previousBackupStatus.LocalBackup.BackupDirectory, IsFullBackupOrSnapshot) == false)
                        // the local folder already includes a full backup or snapshot
                    {
                        isFullBackup = true;

                        var counter = 0;
                        do
                        {
                            var prefix = counter == 0 ? string.Empty : $"-{counter++:D2}";
                            folderName = $"{now}{prefix}.ravendb-{_database.Name}-{_serverStore.NodeTag}-{configuration.BackupType.ToString().ToLower()}";
                            backupDirectory = backupToLocalFolder ? new PathSetting(configuration.LocalSettings.FolderPath).Combine(folderName) : _tempBackupPath;
                        } while (DirectoryContainsBackupFiles(backupDirectory.FullPath, IsAnyBackupFile));

                        if (Directory.Exists(backupDirectory.FullPath) == false)
                            Directory.CreateDirectory(backupDirectory.FullPath);
                    }
                    else
                    {
                        folderName = previousBackupStatus.FolderName;
                        backupDirectory = backupToLocalFolder ? new PathSetting(previousBackupStatus.LocalBackup.BackupDirectory) : _tempBackupPath;
                    }

                    runningBackupStatus.LocalBackup.BackupDirectory = backupToLocalFolder ? backupDirectory.FullPath : null;
                    runningBackupStatus.LocalBackup.TempFolderUsed = backupToLocalFolder == false;
                    runningBackupStatus.IsFull = isFullBackup;

                    if (_logger.IsInfoEnabled)
                    {
                        var fullBackupText = "a " + (configuration.BackupType == BackupType.Backup ? "full backup" : "snapshot");
                        _logger.Info($"Creating {(isFullBackup ? fullBackupText : "an incremental backup")}");
                    }

                    if (isFullBackup == false)
                    {
                        // no-op if nothing has changed
                        var currentLastEtag = DocumentsStorage.ReadLastEtag(tx.InnerTransaction);
                        if (currentLastEtag == previousBackupStatus.LastEtag)
                        {
                            if (_logger.IsInfoEnabled)
                                _logger.Info("Skipping incremental backup because " +
                                             $"last etag ({currentLastEtag}) hasn't changed since last backup");

                            runningBackupStatus.LastIncrementalBackup = periodicBackup.StartTime;
                            return;
                        }
                    }

                    var startDocumentEtag = isFullBackup == false ? previousBackupStatus.LastEtag : null;
                    var fileName = GetFileName(isFullBackup, backupDirectory.FullPath, now, configuration.BackupType, out string backupFilePath);
                    var lastEtag = CreateLocalBackupOrSnapshot(configuration,
                        isFullBackup, runningBackupStatus, backupFilePath, startDocumentEtag, context, tx);

                    try
                    {
                        await UploadToServer(configuration, runningBackupStatus, backupFilePath, folderName, fileName, isFullBackup);
                    }
                    finally
                    {
                        // if user did not specify local folder we delete temporary file
                        if (backupToLocalFolder == false)
                        {
                            IOExtensions.DeleteFile(backupFilePath);
                        }
                    }

                    runningBackupStatus.LastEtag = lastEtag;
                    runningBackupStatus.FolderName = folderName;
                    if (isFullBackup)
                        runningBackupStatus.LastFullBackup = periodicBackup.StartTime;
                    else
                        runningBackupStatus.LastIncrementalBackup = periodicBackup.StartTime;
                }

                totalSw.Stop();

                if (_logger.IsInfoEnabled)
                {
                    var fullBackupText = "a " + (configuration.BackupType == BackupType.Backup ? " full backup" : " snapshot");
                    _logger.Info($"Successfully created {(isFullBackup ? fullBackupText : "an incremental backup")} " +
                                 $"in {totalSw.ElapsedMilliseconds:#,#;;0} ms");
                }
            }
            catch (OperationCanceledException)
            {
                // shutting down, probably
                operationCanceled = true;
            }
            catch (ObjectDisposedException)
            {
                // shutting down, probably
                operationCanceled = true;
            }
            catch (Exception e)
            {
                const string message = "Error when performing periodic backup";

                runningBackupStatus.Error = new Error
                {
                    Exception = e.ToString(),
                    At = DateTime.UtcNow
                };

                if (_logger.IsOperationsEnabled)
                    _logger.Operations(message, e);

                _database.NotificationCenter.Add(AlertRaised.Create(
                    _database.Name,
                    "Periodic Backup",
                    message,
                    AlertType.PeriodicBackup,
                    NotificationSeverity.Error,
                    details: new ExceptionDetails(e)));
            }
            finally
            {
                if (operationCanceled == false)
                {
                    // whether we succeeded or not,
                    // we need to update the last backup time to avoid
                    // starting a new backup right after this one
                    if (isFullBackup)
                        runningBackupStatus.LastFullBackupInternal = periodicBackup.StartTime;
                    else
                        runningBackupStatus.LastIncrementalBackupInternal = periodicBackup.StartTime;

                    runningBackupStatus.NodeTag = _serverStore.NodeTag;
                    runningBackupStatus.DurationInMs = totalSw.ElapsedMilliseconds;
                    runningBackupStatus.Version = ++previousBackupStatus.Version;

                    periodicBackup.BackupStatus = runningBackupStatus;
                    // save the backup status
                    await WriteStatus(runningBackupStatus);
                }
            }
        }

        private static string GetFileName(
            bool isFullBackup,
            string backupFolder,
            string now,
            BackupType backupType,
            out string backupFilePath)
        {
            var backupExtension = GetBackupExtension(backupType, isFullBackup);
            var fileName = isFullBackup ? 
                GetFileNameFor(backupExtension, now, backupFolder, out backupFilePath, throwWhenFileExists: true) : 
                GetFileNameFor(backupExtension, now, backupFolder, out backupFilePath);

            return fileName;
        }

        private static string GetFileNameFor(
            string backupExtension,
            string now,
            string backupFolder,
            out string backupFilePath,
            bool throwWhenFileExists = false)
        {
            var fileName = $"{now}{backupExtension}";
            backupFilePath = Path.Combine(backupFolder, fileName);

            if (File.Exists(backupFilePath))
            {
                if (throwWhenFileExists)
                    throw new InvalidOperationException($"File '{backupFilePath}' already exists!");

                var counter = 1;
                while (true)
                {
                    fileName = $"{now}-{counter:D2}{backupExtension}";
                    backupFilePath = Path.Combine(backupFolder, fileName);

                    if (File.Exists(backupFilePath) == false)
                        break;

                    counter++;
                }
            }

            return fileName;
        }

        private long CreateLocalBackupOrSnapshot(PeriodicBackupConfiguration configuration,
            bool isFullBackup, PeriodicBackupStatus status, string backupFilePath,
            long? startDocumentEtag, DocumentsOperationContext context, DocumentsTransaction tx)
        {
            long lastEtag;
            using (status.LocalBackup.UpdateStats(isFullBackup))
            {
                try
                {
                    // will rename the file after the backup is finished
                    var tempBackupFilePath = backupFilePath + InProgressExtension;

                    if (configuration.BackupType == BackupType.Backup ||
                        configuration.BackupType == BackupType.Snapshot && isFullBackup == false)
                    {
                        // smuggler backup
                        var options = new DatabaseSmugglerOptionsServerSide
                        {
                            AuthorizationStatus = AuthorizationStatus.DatabaseAdmin,
                        };
                        if (isFullBackup == false)
                            options.OperateOnTypes |= DatabaseItemType.Tombstones;

                        var result = CreateBackup(options, tempBackupFilePath, startDocumentEtag, context);
                        lastEtag = isFullBackup ?
                            DocumentsStorage.ReadLastEtag(tx.InnerTransaction) :
                            result.GetLastEtag();
                    }
                    else
                    {
                        // snapshot backup
                        lastEtag = DocumentsStorage.ReadLastEtag(tx.InnerTransaction);
                        _database.FullBackupTo(tempBackupFilePath);
                    }

                    IOExtensions.RenameFile(tempBackupFilePath, backupFilePath);
                }
                catch (Exception e)
                {
                    status.LocalBackup.Exception = e.ToString();
                    throw;
                }
            }
            return lastEtag;
        }

        private static string GetBackupExtension(BackupType type, bool isFullBackup)
        {
            if (isFullBackup == false)
                return Constants.Documents.PeriodicBackup.IncrementalBackupExtension;

            switch (type)
            {
                case BackupType.Backup:
                    return Constants.Documents.PeriodicBackup.FullBackupExtension;
                case BackupType.Snapshot:
                    return Constants.Documents.PeriodicBackup.SnapshotExtension;
                default:
                    throw new ArgumentOutOfRangeException(nameof(type), type, null);
            }
        }

        private SmugglerResult CreateBackup(DatabaseSmugglerOptionsServerSide options, string backupFilePath, long? startDocumentEtag, DocumentsOperationContext context)
        {
            // the last etag is already included in the last backup
            startDocumentEtag = startDocumentEtag == null ? 0 : ++startDocumentEtag;

            SmugglerResult result;
            using (var file = File.Open(backupFilePath, FileMode.CreateNew))
            {
                var smugglerSource = new DatabaseSource(_database, startDocumentEtag.Value);
                var smugglerDestination = new StreamDestination(file, context, smugglerSource);
                var smuggler = new DatabaseSmuggler(_database,
                    smugglerSource,
                    smugglerDestination,
                    _database.Time,
                    token: _cancellationToken.Token,
                    options: options);

                result = smuggler.Execute();
                file.Flush(flushToDisk: true);
            }
            return result;
        }

        private static bool DirectoryContainsBackupFiles(string fullPath, Func<string, bool> isBackupFile)
        {
            if (Directory.Exists(fullPath) == false)
                return false;

            var files = Directory.GetFiles(fullPath);
            if (files.Length == 0)
                return false;

            return files.Any(isBackupFile);
        }

        private static bool IsFullBackupOrSnapshot(string filePath)
        {
            var extension = Path.GetExtension(filePath);
            return Constants.Documents.PeriodicBackup.FullBackupExtension.Equals(extension, StringComparison.OrdinalIgnoreCase) ||
                   Constants.Documents.PeriodicBackup.SnapshotExtension.Equals(extension, StringComparison.OrdinalIgnoreCase);
        }

        private static bool IsAnyBackupFile(string filePath)
        {
            if (RestoreUtils.IsBackupOrSnapshot(filePath))
                return true;

            var extension = Path.GetExtension(filePath);
            return InProgressExtension.Equals(extension, StringComparison.OrdinalIgnoreCase);
        }

        private async Task WriteStatus(PeriodicBackupStatus status)
        {
            if (_cancellationToken.IsCancellationRequested)
                return;

            try
            {
                var command = new UpdatePeriodicBackupStatusCommand(_database.Name)
                {
                    PeriodicBackupStatus = status
                };

                var result = await _serverStore.SendToLeaderAsync(command);

                if (_logger.IsInfoEnabled)
                    _logger.Info($"Periodic backup status with task id {status.TaskId} was updated");

                await _serverStore.WaitForCommitIndexChange(RachisConsensus.CommitIndexModification.GreaterOrEqual, result.Index);
            }
            catch (Exception e)
            {
                const string message = "Error saving the periodic backup status";

                if (_logger.IsOperationsEnabled)
                    _logger.Operations(message, e);

                _database.NotificationCenter.Add(AlertRaised.Create(
                    _database.Name,
                    "Periodic Backup",
                    message,
                    AlertType.PeriodicBackup,
                    NotificationSeverity.Error,
                    details: new ExceptionDetails(e)));
            }
        }

        private async Task UploadToServer(
            PeriodicBackupConfiguration configuration,
            PeriodicBackupStatus backupStatus,
            string backupPath, string folderName,
            string fileName, bool isFullBackup)
        {
            if (_cancellationToken.IsCancellationRequested)
                return;

            var tasks = new List<Task>();

            CreateUploadTaskIfNeeded(configuration.S3Settings, tasks, backupPath, isFullBackup,
                async (settings, stream, uploadProgress) =>
                {
                    var archiveDescription = GetArchiveDescription(isFullBackup, configuration.BackupType);
                    await UploadToS3(settings, stream, folderName, fileName, uploadProgress, archiveDescription);
                },
                ref backupStatus.UploadToS3, "S3");

            CreateUploadTaskIfNeeded(configuration.GlacierSettings, tasks, backupPath, isFullBackup,
                async (settings, stream, uploadProgress) =>
                    await UploadToGlacier(settings, stream, folderName, fileName, uploadProgress),
                ref backupStatus.UploadToGlacier, "Glacier");

            CreateUploadTaskIfNeeded(configuration.AzureSettings, tasks, backupPath, isFullBackup,
                async (settings, stream, uploadProgress) =>
                {
                    var archiveDescription = GetArchiveDescription(isFullBackup, configuration.BackupType);
                    await UploadToAzure(settings, stream, folderName, fileName, uploadProgress, archiveDescription);
                },
                ref backupStatus.UploadToAzure, "Azure");

            CreateUploadTaskIfNeeded(configuration.FtpSettings, tasks, backupPath, isFullBackup,
                async (settings, stream, uploadProgress) =>
                    await UploadToFtp(settings, stream, folderName, fileName, uploadProgress),
                ref backupStatus.UploadToFtp, "FTP");

            await Task.WhenAll(tasks);
        }

        private static void CreateUploadTaskIfNeeded<S, T>(
            S settings,
            List<Task> tasks,
            string backupPath,
            bool isFullBackup,
            Func<S, FileStream, UploadProgress, Task> uploadToServer,
            ref T uploadStatus,
            string backupDestination)
            where S : BackupSettings
            where T : CloudUploadStatus
        {
            if (PeriodicBackupConfiguration.CanBackupUsing(settings) == false)
                return;

            if (uploadStatus == null)
                uploadStatus = (T)Activator.CreateInstance(typeof(T));

            var localUploadStatus = uploadStatus;

            tasks.Add(Task.Run(async () =>
            {
                using (localUploadStatus.UpdateStats(isFullBackup))
                using (var fileStream = File.OpenRead(backupPath))
                {
                    var uploadProgress = localUploadStatus.UploadProgress;
                    uploadProgress.ChangeState(UploadState.PendingUpload);
                    uploadProgress.SetTotal(fileStream.Length);

                    try
                    {
                        await uploadToServer(settings, fileStream, uploadProgress);
                    }
                    catch (OperationCanceledException e)
                    {
                        // shutting down
                        localUploadStatus.Exception = e.ToString();
                    }
                    catch (Exception e)
                    {
                        localUploadStatus.Exception = e.ToString();
                        throw new InvalidOperationException($"Failed to backup to {backupDestination}", e);
                    }
                    finally
                    {
                        uploadProgress.ChangeState(UploadState.Done);
                    }
                }
            }));
        }

        private async Task UploadToS3(
            S3Settings settings,
            Stream stream,
            string folderName,
            string fileName,
            UploadProgress uploadProgress,
            string archiveDescription)
        {
            using (var client = new RavenAwsS3Client(settings.AwsAccessKey, settings.AwsSecretKey,
                settings.AwsRegionName, settings.BucketName, uploadProgress, _cancellationToken.Token))
            {
                var key = CombinePathAndKey(settings.RemoteFolderName, folderName, fileName);
                await client.PutObject(key, stream, new Dictionary<string, string>
                {
                    {"Description", archiveDescription}
                });

                if (_logger.IsInfoEnabled)
                    _logger.Info(string.Format($"Successfully uploaded backup file '{fileName}' " +
                                               $"to S3 bucket named: {settings.BucketName}, " +
                                               $"with key: {key}"));
            }
        }

        private async Task UploadToGlacier(
            GlacierSettings settings,
            Stream stream,
            string folderName,
            string fileName,
            UploadProgress uploadProgress)
        {
            using (var client = new RavenAwsGlacierClient(settings.AwsAccessKey, settings.AwsSecretKey,
                settings.AwsRegionName, settings.VaultName, uploadProgress, _cancellationToken.Token))
            {
                var key = CombinePathAndKey(_database.Name, folderName, fileName);
                var archiveId = await client.UploadArchive(stream, key);
                if (_logger.IsInfoEnabled)
                    _logger.Info($"Successfully uploaded backup file '{fileName}' to Glacier, archive ID: {archiveId}");
            }
        }

        private async Task UploadToFtp(
            FtpSettings settings,
            Stream stream,
            string folderName,
            string fileName,
            UploadProgress uploadProgress)
        {
            using (var client = new RavenFtpClient(settings.Url, settings.Port, settings.UserName,
                settings.Password, settings.CertificateAsBase64, settings.CertificateFileName, uploadProgress, _cancellationToken.Token))
            {
                await client.UploadFile(folderName, fileName, stream);
                if (_logger.IsInfoEnabled)
                    _logger.Info($"Successfully uploaded backup file '{fileName}' to an ftp server");
            }
        }

        private async Task UploadToAzure(
            AzureSettings settings,
            Stream stream,
            string folderName,
            string fileName,
            UploadProgress uploadProgress,
            string archiveDecription)
        {
            using (var client = new RavenAzureClient(settings.AccountName, settings.AccountKey,
                settings.StorageContainer, uploadProgress, _cancellationToken.Token))
            {
                var key = CombinePathAndKey(settings.RemoteFolderName, folderName, fileName);
                await client.PutBlob(key, stream, new Dictionary<string, string>
                {
                    {"Description", archiveDecription}
                });

                if (_logger.IsInfoEnabled)
                    _logger.Info($"Successfully uploaded backup file '{fileName}' " +
                                 $"to Azure container: {settings.StorageContainer}, with key: {key}");
            }
        }

        private static string CombinePathAndKey(string path, string folderName, string fileName)
        {
            var prefix = string.IsNullOrWhiteSpace(path) == false ? (path + "/") : string.Empty;
            return $"{prefix}{folderName}/{fileName}";
        }

        private string GetArchiveDescription(bool isFullBackup, BackupType backupType)
        {
            var fullBackupText = backupType == BackupType.Backup ? "Full backup" : "A snapshot";
            return $"{(isFullBackup ? fullBackupText : "Incremental backup")} for db {_database.Name} at {SystemTime.UtcNow}";
        }

        private static DateTime GetNextBackupDateTime(DateTime? nextFullBackup, DateTime? nextIncrementalBackup)
        {
            Debug.Assert(nextFullBackup != null || nextIncrementalBackup != null);

            if (nextFullBackup == null)
                return nextIncrementalBackup.Value;

            if (nextIncrementalBackup == null)
                return nextFullBackup.Value;

            var nextBackup =
                nextFullBackup <= nextIncrementalBackup ? nextFullBackup.Value : nextIncrementalBackup.Value;

            return nextBackup;
        }

        private DateTime? GetNextBackupOccurrence(string backupFrequency,
            DateTime lastBackupUtc, PeriodicBackupConfiguration configuration, bool skipErrorLog)
        {
            if (string.IsNullOrWhiteSpace(backupFrequency))
                return null;

            try
            {
                var backupParser = CrontabSchedule.Parse(backupFrequency);
                return backupParser.GetNextOccurrence(lastBackupUtc.ToLocalTime());
            }
            catch (Exception e)
            {
                if (skipErrorLog == false)
                {
                    var message = "Couldn't parse periodic backup " +
                                  $"frequency {backupFrequency}, task id: {configuration.TaskId}";
                    if (string.IsNullOrWhiteSpace(configuration.Name) == false)
                        message += $", backup name: {configuration.Name}";

                    message += $", error: {e.Message}";

                    if (_logger.IsInfoEnabled)
                        _logger.Info(message);

                    _database.NotificationCenter.Add(AlertRaised.Create(
                        _database.Name,
                        "Backup frequency parsing error",
                        message,
                        AlertType.PeriodicBackup,
                        NotificationSeverity.Error,
                        details: new ExceptionDetails(e)));
                }

                return null;
            }
        }

        private class BackupTaskDetails
        {
            public long TaskId { get; set; }

            public bool IsFullBackup { get; set; }

            public TimeSpan NextBackup { get; set; }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool IsValidTimeSpanForTimer(TimeSpan nextBackupTimeSpan)
        {
            return nextBackupTimeSpan < MaxTimerTimeout;
        }

        private void TimerCallback(object backupTaskDetails)
        {
            try
            {
                if (_cancellationToken.IsCancellationRequested)
                    return;

                var backupDetails = (BackupTaskDetails)backupTaskDetails;

                if (ShouldRunBackupAfterTimerCallback(backupDetails, out PeriodicBackup periodicBackup) == false)
                    return;

                CreateBackupTask(periodicBackup, backupDetails.IsFullBackup);
            }
            catch (Exception e)
            {
                _logger.Operations("Error during timer callback", e);
            }
        }

        public string WhoseTaskIsIt(long taskId)
        {
            if (_periodicBackups.TryGetValue(taskId, out var periodicBackup) == false)
            {
                throw new InvalidOperationException($"Backup task id: {taskId} doesn't exist");
            }

            if (periodicBackup.Configuration.Disabled)
            {
                throw new InvalidOperationException($"Backup task id: {taskId} is disabled");
            }

            if (periodicBackup.Configuration.HasBackup() == false)
            {
                throw new InvalidOperationException($"All backup destinations are disabled for backup task id: {taskId}");
            }

            var databaseRecord = GetDatabaseRecord();
            var backupStatus = GetBackupStatus(taskId);
            return _database.WhoseTaskIsIt(databaseRecord.Topology, periodicBackup.Configuration, backupStatus, useLastResponsibleNodeIfNoAvailableNodes: true);
        }

        public void StartBackupTask(long taskId, bool isFullBackup)
        {
            if (_periodicBackups.TryGetValue(taskId, out var periodicBackup) == false)
            {
                throw new InvalidOperationException($"Backup task id: {taskId} doesn't exist");
            }

            CreateBackupTask(periodicBackup, isFullBackup);
        }

        private void CreateBackupTask(PeriodicBackup periodicBackup, bool isFullBackup)
        {
            periodicBackup.UpdateBackupTask(() =>
            {
                if (periodicBackup.RunningTask != null)
                {
                    // backup is already running
                    return;
                }

                periodicBackup.StartTime = SystemTime.UtcNow;
                periodicBackup.RunningTask = Task.Run(async () =>
                {
                    try
                    {
                        periodicBackup.BackupStatus = GetBackupStatus(periodicBackup.Configuration.TaskId, periodicBackup.BackupStatus);

                        await RunPeriodicBackup(periodicBackup, isFullBackup);
                    }
                    catch (Exception e)
                    {
                        _logger.Operations("Error during create backup task", e);
                    }
                    finally
                    {
                        periodicBackup.RunningTask = null;
                        periodicBackup.RunningBackupStatus = null;

                        if (periodicBackup.HasScheduledBackup() &&
                            _cancellationToken.IsCancellationRequested == false)
                        {
                            var newBackupTimer = GetTimer(periodicBackup.Configuration, periodicBackup.BackupStatus);
                            periodicBackup.UpdateTimer(newBackupTimer, discardIfDisabled: true);
                        }
                    }
                }, _cancellationToken.Token);
            });
        }

        private DatabaseRecord GetDatabaseRecord()
        {
            using (_serverStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (context.OpenReadTransaction())
            {
                return _serverStore.Cluster.ReadDatabase(context, _database.Name);
            }
        }

        private void LongPeriodTimerCallback(object backupTaskDetails)
        {
            try
            {
                if (_cancellationToken.IsCancellationRequested)
                    return;

                var backupDetails = (BackupTaskDetails)backupTaskDetails;

                if (ShouldRunBackupAfterTimerCallback(backupDetails, out PeriodicBackup periodicBackup) == false)
                    return;

                var remainingInterval = backupDetails.NextBackup - MaxTimerTimeout;
                if (remainingInterval.TotalMilliseconds <= 0)
                {
                    CreateBackupTask(periodicBackup, backupDetails.IsFullBackup);
                    return;
                }

                periodicBackup.UpdateTimer(GetTimer(periodicBackup.Configuration, periodicBackup.BackupStatus));
            }
            catch (Exception e)
            {
                _logger.Operations("Error during long timer callback", e);
            }
        }

        private bool ShouldRunBackupAfterTimerCallback(BackupTaskDetails backupInfo, out PeriodicBackup periodicBackup)
        {
            if (_periodicBackups.TryGetValue(backupInfo.TaskId, out periodicBackup) == false)
            {
                // periodic backup doesn't exist anymore
                return false;
            }

            var databaseRecord = GetDatabaseRecord();
            if (databaseRecord == null)
                return false;

            var taskStatus = GetTaskStatus(databaseRecord, periodicBackup.Configuration);
            return taskStatus == TaskStatus.ActiveByCurrentNode;
        }

        public PeriodicBackupStatus GetBackupStatus(long taskId)
        {
            PeriodicBackupStatus inMemoryBackupStatus = null;
            if (_periodicBackups.TryGetValue(taskId, out PeriodicBackup periodicBackup))
                inMemoryBackupStatus = periodicBackup.BackupStatus;

            return GetBackupStatus(taskId, inMemoryBackupStatus);
        }

        private PeriodicBackupStatus GetBackupStatus(long taskId, PeriodicBackupStatus inMemoryBackupStatus)
        {
            var backupStatus = GetBackupStatusFromCluster(taskId);
            if (backupStatus == null)
            {
                backupStatus = inMemoryBackupStatus ?? new PeriodicBackupStatus
                {
                    TaskId = taskId
                };
            }
            else if (inMemoryBackupStatus?.Version > backupStatus.Version &&
                     inMemoryBackupStatus?.NodeTag == backupStatus.NodeTag)
            {
                // the in memory backup status is more updated
                // and is of the same node (current one)
                backupStatus = inMemoryBackupStatus;
            }

            return backupStatus;
        }

        private PeriodicBackupStatus GetBackupStatusFromCluster(long taskId)
        {
            using (_serverStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (context.OpenReadTransaction())
            {
                var statusBlittable = _serverStore.Cluster.Read(context, PeriodicBackupStatus.GenerateItemName(_database.Name, taskId));

                if (statusBlittable == null)
                    return null;

                var periodicBackupStatusJson = JsonDeserializationClient.PeriodicBackupStatus(statusBlittable);
                return periodicBackupStatusJson;
            }
        }

        public void UpdateConfigurations(DatabaseRecord databaseRecord)
        {
            if (_disposed)
                return;

            if (databaseRecord.PeriodicBackups == null)
            {
                foreach (var periodicBackup in _periodicBackups)
                {
                    periodicBackup.Value.DisableFutureBackups();

                    TryAddInactiveRunningPeriodicBackups(periodicBackup.Value.RunningTask);
                }
                return;
            }

            var allBackupTaskIds = new List<long>();
            foreach (var periodicBackupConfiguration in databaseRecord.PeriodicBackups)
            {
                var newBackupTaskId = periodicBackupConfiguration.TaskId;
                allBackupTaskIds.Add(newBackupTaskId);

                var taskState = GetTaskStatus(databaseRecord, periodicBackupConfiguration);

                UpdatePeriodicBackup(newBackupTaskId, periodicBackupConfiguration, taskState);
            }

            RemoveInactiveCompletedTasks();

            var deletedBackupTaskIds = _periodicBackups.Keys.Except(allBackupTaskIds).ToList();
            foreach (var deletedBackupId in deletedBackupTaskIds)
            {
                if (_periodicBackups.TryRemove(deletedBackupId, out var deletedBackup) == false)
                    continue;

                // stopping any future backups
                // currently running backups will continue to run
                deletedBackup.DisableFutureBackups();
                TryAddInactiveRunningPeriodicBackups(deletedBackup.RunningTask);
            }
        }

        public void RemoveInactiveCompletedTasks()
        {
            if (_inactiveRunningPeriodicBackupsTasks.Count == 0)
                return;

            var tasksToRemove = new List<Task>();
            foreach (var inactiveTask in _inactiveRunningPeriodicBackupsTasks)
            {
                if (inactiveTask.IsCompleted == false)
                    continue;

                tasksToRemove.Add(inactiveTask);
            }

            foreach (var taskToRemove in tasksToRemove)
            {
                _inactiveRunningPeriodicBackupsTasks.TryRemove(taskToRemove);
            }
        }

        private void UpdatePeriodicBackup(long taskId,
            PeriodicBackupConfiguration newConfiguration,
            TaskStatus taskState)
        {
            Debug.Assert(taskId == newConfiguration.TaskId);

            var backupStatus = GetBackupStatus(taskId, inMemoryBackupStatus: null);
            if (_periodicBackups.TryGetValue(taskId, out var existingBackupState) == false)
            {
                var newPeriodicBackup = new PeriodicBackup
                {
                    Configuration = newConfiguration
                };

                _periodicBackups.TryAdd(taskId, newPeriodicBackup);

                if (taskState == TaskStatus.ActiveByCurrentNode)
                    newPeriodicBackup.UpdateTimer(GetTimer(newConfiguration, backupStatus));

                return;
            }

            existingBackupState.Configuration = newConfiguration;

            if (taskState != TaskStatus.ActiveByCurrentNode)
            {
                // this node isn't responsible for the backup task
                existingBackupState.DisableFutureBackups();
                return;
            }

            if (existingBackupState.RunningTask != null)
            {
                // a backup is already running 
                // the next one will be re-scheduled by the backup task
                return;
            }

            if (existingBackupState.Configuration.HasBackupFrequencyChanged(newConfiguration) == false &&
                existingBackupState.HasScheduledBackup())
            {
                // backup frequency hasn't changed
                // and we have a scheduled backup
                return;
            }

            existingBackupState.UpdateTimer(GetTimer(newConfiguration, backupStatus));
        }

        private enum TaskStatus
        {
            Disabled,
            ActiveByCurrentNode,
            ActiveByOtherNode
        }

        private TaskStatus GetTaskStatus(
            DatabaseRecord databaseRecord,
            PeriodicBackupConfiguration configuration,
            bool skipErrorLog = false)
        {
            if (configuration.Disabled)
                return TaskStatus.Disabled;

            if (configuration.HasBackup() == false)
            {
                if (skipErrorLog == false)
                {
                    var message = $"All backup destinations are disabled for backup task id: {configuration.TaskId}";
                    _database.NotificationCenter.Add(AlertRaised.Create(
                        _database.Name,
                        "Periodic Backup",
                        message,
                        AlertType.PeriodicBackup,
                        NotificationSeverity.Info));
                }

                return TaskStatus.Disabled;
            }

            var backupStatus = GetBackupStatus(configuration.TaskId);
            var whoseTaskIsIt = _database.WhoseTaskIsIt(databaseRecord.Topology, configuration, backupStatus, useLastResponsibleNodeIfNoAvailableNodes: true);
            if (whoseTaskIsIt == null)
                return TaskStatus.Disabled;

            if (whoseTaskIsIt == _serverStore.NodeTag)
                return TaskStatus.ActiveByCurrentNode;

            if (_logger.IsInfoEnabled)
                _logger.Info($"Backup job is skipped at {SystemTime.UtcNow}, because it is managed " +
                             $"by '{whoseTaskIsIt}' node and not the current node ({_serverStore.NodeTag})");

            return TaskStatus.ActiveByOtherNode;
        }

        private void TryAddInactiveRunningPeriodicBackups(Task runningTask)
        {
            if (runningTask == null ||
                runningTask.IsCompleted)
                return;

            _inactiveRunningPeriodicBackupsTasks.Add(runningTask);
        }

        public void Dispose()
        {
            if (_disposed)
                return;

            lock (this)
            {
                if (_disposed)
                    return;

                _disposed = true;
                _database.DocumentTombstoneCleaner.Unsubscribe(this);

                using (_cancellationToken)
                {
                    _cancellationToken.Cancel();

                    foreach (var periodicBackup in _periodicBackups)
                    {
                        periodicBackup.Value.DisableFutureBackups();

                        var task = periodicBackup.Value.RunningTask;
                        WaitForTaskCompletion(task);
                    }

                    foreach (var task in _inactiveRunningPeriodicBackupsTasks)
                    {
                        WaitForTaskCompletion(task);
                    }
                }

                if (_tempBackupPath != null)
                    IOExtensions.DeleteDirectory(_tempBackupPath.FullPath);
            }
        }

        private void WaitForTaskCompletion(Task task)
        {
            try
            {
                task?.Wait();
            }
            catch (ObjectDisposedException)
            {
                // shutting down, probably
            }
            catch (AggregateException e) when (e.InnerException is OperationCanceledException)
            {
                // shutting down
            }
            catch (Exception e)
            {
                if (_logger.IsInfoEnabled)
                    _logger.Info("Error when disposing periodic backup runner task", e);
            }
        }

        public bool HasRunningBackups()
        {
            foreach (var periodicBackup in _periodicBackups)
            {
                if (periodicBackup.Value.RunningTask != null &&
                    periodicBackup.Value.RunningTask.IsCompleted == false)
                    return true;
            }

            return false;
        }

        public BackupInfo GetBackupInfo()
        {
            if (_periodicBackups.Count == 0)
                return null;

            var allBackupTicks = new List<long>();
            var allNextBackupTimeSpanSeconds = new List<double>();
            foreach (var periodicBackup in _periodicBackups)
            {
                var configuration = periodicBackup.Value.Configuration;
                var backupStatus = GetBackupStatus(configuration.TaskId, periodicBackup.Value.BackupStatus);
                if (backupStatus == null)
                    continue;

                if (backupStatus.LastFullBackup != null)
                    allBackupTicks.Add(backupStatus.LastFullBackup.Value.Ticks);

                if (backupStatus.LastIncrementalBackup != null)
                    allBackupTicks.Add(backupStatus.LastIncrementalBackup.Value.Ticks);

                var nextBackup = GetNextBackupDetails(configuration, backupStatus, skipErrorLog: true);
                if (nextBackup != null)
                {
                    allNextBackupTimeSpanSeconds.Add(nextBackup.TimeSpan.TotalSeconds);
                }
            }

            return new BackupInfo
            {
                LastBackup = allBackupTicks.Count == 0 ? (DateTime?)null : new DateTime(allBackupTicks.Max()),
                IntervalUntilNextBackupInSec = allNextBackupTimeSpanSeconds.Count == 0 ? 0 : allNextBackupTimeSpanSeconds.Min()
            };
        }

        public RunningBackup OnGoingBackup(long taskId)
        {
            if (_periodicBackups.TryGetValue(taskId, out var periodicBackup) == false)
                return null;

            if (periodicBackup.RunningTask == null)
                return null;

            return new RunningBackup
            {
                StartTime = periodicBackup.StartTime,
                IsFull = periodicBackup.RunningBackupStatus?.IsFull ?? false
            };
        }

        public Dictionary<string, long> GetLastProcessedDocumentTombstonesPerCollection()
        {
            if (_periodicBackups.Count == 0)
                return EmptyDictionary;

            var processedTombstonesPerCollection = new Dictionary<string, long>();

            var minLastEtag = long.MaxValue;
            foreach (var periodicBackup in _periodicBackups.Values)
            {
                if (periodicBackup.BackupStatus?.LastEtag != null &&
                    minLastEtag > periodicBackup.BackupStatus?.LastEtag)
                {
                    minLastEtag = periodicBackup.BackupStatus.LastEtag.Value;
                }
            }

            if (minLastEtag == long.MaxValue)
                minLastEtag = 0;

            using (_database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (context.OpenReadTransaction())
            {
                foreach (var collection in _database.DocumentsStorage.GetCollections(context))
                {
                    processedTombstonesPerCollection[collection.Name] = minLastEtag;
                }
            }

            return processedTombstonesPerCollection;
        }
    }
}
