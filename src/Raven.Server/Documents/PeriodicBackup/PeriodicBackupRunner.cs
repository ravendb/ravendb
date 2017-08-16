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
using Raven.Client.Documents.Smuggler;
using Raven.Client.Json.Converters;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Client.ServerWide.PeriodicBackup;
using Raven.Server.Rachis;
using Raven.Server.ServerWide.Commands.PeriodicBackup;
using Sparrow.Collections;
using Constants = Raven.Client.Constants;

namespace Raven.Server.Documents.PeriodicBackup
{
    public class PeriodicBackupRunner : IDisposable
    {
        private readonly Logger _logger;

        private readonly DocumentDatabase _database;
        private readonly ServerStore _serverStore;
        private readonly CancellationTokenSource _cancellationToken;
        private readonly PathSetting _tempBackupPath;
        private readonly ConcurrentDictionary<long, PeriodicBackup> _periodicBackups
            = new ConcurrentDictionary<long, PeriodicBackup>();
        private readonly ConcurrentSet<Task> _inactiveRunningPeriodicBackupsTasks = new ConcurrentSet<Task>();
        private bool _disposed;

        // interval can be 2^32-2 milliseconds at most
        // this is the maximum interval acceptable in .Net's threading timer
        public readonly TimeSpan MaxTimerTimeout = TimeSpan.FromMilliseconds(Math.Pow(2, 32) - 2);

        public ICollection<PeriodicBackup> PeriodicBackups => _periodicBackups.Values;

        public static string DateTimeFormat => "yyyy-MM-dd-HH-mm";

        public PeriodicBackupRunner(DocumentDatabase database, ServerStore serverStore)
        {
            _database = database;
            _serverStore = serverStore;
            _logger = LoggingSource.Instance.GetLogger<PeriodicBackupRunner>(_database.Name);
            _cancellationToken = CancellationTokenSource.CreateLinkedTokenSource(_database.DatabaseShutdown);

            _tempBackupPath = (_database.Configuration.Storage.TempPath ?? _database.Configuration.Core.DataDirectory).Combine("PeriodicBackupTemp");

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
            var timer = isValidTimeSpanForTimer ?
                new Timer(TimerCallback, backupTaskDetails, backupTaskDetails.NextBackup, Timeout.InfiniteTimeSpan) :
                new Timer(LongPeriodTimerCallback, backupTaskDetails, MaxTimerTimeout, Timeout.InfiniteTimeSpan);

            return timer;
        }

        public NextBackup GetNextBackupDetails(
            DatabaseRecord databaseRecord,
            PeriodicBackupConfiguration configuration,
            PeriodicBackupStatus backupStatus)
        {
            var taskStatus = GetTaskStatus(databaseRecord, configuration, skipErrorLog: true);
            return taskStatus == TaskStatus.Disabled ? null :
                GetNextBackupDetails(configuration, backupStatus, skipErrorLog: true);
        }

        private NextBackup GetNextBackupDetails(
            PeriodicBackupConfiguration configuration,
            PeriodicBackupStatus backupStatus,
            bool skipErrorLog = false)
        {
            var now = SystemTime.UtcNow;
            var lastFullBackup = backupStatus.LastFullBackup ?? now;
            var lastIncrementalBackup = backupStatus.LastIncrementalBackup ?? backupStatus.LastFullBackup ?? now;
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

                _database.NotificationCenter.Add(AlertRaised.Create("Couldn't schedule next backup, this shouldn't happen",
                    message,
                    AlertType.PeriodicBackup,
                    NotificationSeverity.Warning));

                return null;
            }

            Debug.Assert(configuration.TaskId != 0);

            var isFullBackup = IsFullBackup(backupStatus, configuration, nextFullBackup, nextIncrementalBackup);
            var nextBackupDateTime = GetNextBackupDateTime(nextFullBackup, nextIncrementalBackup);
            var nextBackupTimeSpan = (nextBackupDateTime - now).Ticks <= 0 ?
                TimeSpan.Zero : nextBackupDateTime - now;

            return new NextBackup
            {
                TimeSpan = nextBackupTimeSpan,
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
            PeriodicBackupConfiguration configuration,
            PeriodicBackupStatus status,
            bool isFullBackup)
        {
            var backupStarted = SystemTime.UtcNow;
            var totalSw = Stopwatch.StartNew();
            status.BackupType = configuration.BackupType;

            try
            {
                using (_database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (var tx = context.OpenReadTransaction())
                {
                    var backupToLocalFolder = CanBackupUsing(configuration.LocalSettings);
                    var now = SystemTime.UtcNow.ToString(DateTimeFormat, CultureInfo.InvariantCulture);

                    if (status.LocalBackup == null)
                        status.LocalBackup = new LocalBackup();

                    PathSetting backupDirectory;

                    string folderName;
                    // check if we need to do a new full backup
                    if (isFullBackup ||
                        status.LastFullBackup == null || // no full backup was previously performed
                        status.NodeTag != _serverStore.NodeTag || // last backup was performed by a different node
                        status.BackupType != configuration.BackupType || // backup type has changed
                        status.LastEtag == null || // last document etag wasn't updated
                        backupToLocalFolder && DirectoryContainsFullBackupOrSnapshot(status.LocalBackup.BackupDirectory, configuration.BackupType) == false)
                    // the local folder has a missing full backup
                    {
                        isFullBackup = true;

                        folderName = $"{now}.ravendb-{_database.Name}-{_serverStore.NodeTag}-{configuration.BackupType.ToString().ToLower()}";

                        backupDirectory = backupToLocalFolder ?
                            new PathSetting(configuration.LocalSettings.FolderPath).Combine(folderName) :
                            _tempBackupPath;

                        if (Directory.Exists(backupDirectory.FullPath) == false)
                            Directory.CreateDirectory(backupDirectory.FullPath);

                        status.LocalBackup.TempFolderUsed = backupToLocalFolder == false;
                        status.LocalBackup.BackupDirectory = backupToLocalFolder ? backupDirectory.FullPath : null;
                    }
                    else
                    {
                        backupDirectory = backupToLocalFolder ? new PathSetting(status.LocalBackup.BackupDirectory) : _tempBackupPath;
                        folderName = status.FolderName;
                    }

                    if (_logger.IsInfoEnabled)
                    {
                        var fullBackupText = "a " + (configuration.BackupType == BackupType.Backup ? "full backup" : "snapshot");
                        _logger.Info($"Creating {(isFullBackup ? fullBackupText : "an incremental backup")}");
                    }

                    if (isFullBackup == false)
                    {
                        // no-op if nothing has changed
                        var currentLastEtag = DocumentsStorage.ReadLastEtag(tx.InnerTransaction);
                        if (currentLastEtag == status.LastEtag)
                        {
                            if (_logger.IsInfoEnabled)
                                _logger.Info("Skipping incremental backup because " +
                                             $"last etag ({currentLastEtag}) hasn't changed since last backup");

                            status.DurationInMs = totalSw.ElapsedMilliseconds;
                            status.LastIncrementalBackup = backupStarted;

                            return;
                        }
                    }

                    var startDocumentEtag = isFullBackup == false ? status.LastEtag : null;
                    var fileName = GetFileName(isFullBackup, backupDirectory.FullPath, now, configuration.BackupType, out string backupFilePath);
                    var lastEtag = CreateLocalBackupOrSnapshot(configuration,
                        isFullBackup, status, backupFilePath, startDocumentEtag, context, tx);

                    try
                    {
                        await UploadToServer(configuration, status, backupFilePath, folderName, fileName, isFullBackup);
                    }
                    finally
                    {
                        // if user did not specify local folder we delete temporary file
                        if (backupToLocalFolder == false)
                        {
                            IOExtensions.DeleteFile(backupFilePath);
                        }
                    }

                    status.LastEtag = lastEtag;
                    status.FolderName = folderName;
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
            }
            catch (ObjectDisposedException)
            {
                // shutting down, probably
            }
            catch (Exception e)
            {
                const string message = "Error when performing periodic backup";

                if (_logger.IsOperationsEnabled)
                    _logger.Operations(message, e);

                _database.NotificationCenter.Add(AlertRaised.Create("Periodic Backup",
                    message,
                    AlertType.PeriodicBackup,
                    NotificationSeverity.Error,
                    details: new ExceptionDetails(e)));
            }
            finally
            {
                // whether we succeded or not,
                // we need to update the last backup time to avoid
                // starting a new backup right after this one
                if (isFullBackup)
                    status.LastFullBackup = backupStarted;
                else
                    status.LastIncrementalBackup = backupStarted;

                status.NodeTag = _serverStore.NodeTag;
                status.DurationInMs = totalSw.ElapsedMilliseconds;
                status.Version++;

                // save the backup status
                await WriteStatus(status);
            }
        }

        private static string GetFileName(
            bool isFullBackup,
            string backupFolder,
            string now,
            BackupType backupType,
            out string backupFilePath)
        {
            string fileName;

            if (isFullBackup)
            {
                // create file name for full backup/snapshot
                fileName = GetFileNameFor(() => GetFullBackupExtension(backupType),
                    now, backupFolder, out backupFilePath);
            }
            else
            {
                // create file name for incremental backup
                fileName = GetFileNameFor(() => Constants.Documents.PeriodicBackup.IncrementalBackupExtension,
                    now, backupFolder, out backupFilePath);
            }

            return fileName;
        }

        private static string GetFileNameFor(Func<string> getBackupExtension,
            string now, string backupFolder, out string backupFilePath)
        {
            var fileName = $"{now}{getBackupExtension()}";
            backupFilePath = Path.Combine(backupFolder, fileName);

            if (File.Exists(backupFilePath))
            {
                var counter = 1;
                while (true)
                {
                    fileName = $"{now}-{counter}${getBackupExtension()}";
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
                    if (configuration.BackupType == BackupType.Backup ||
                        configuration.BackupType == BackupType.Snapshot && isFullBackup == false)
                    {
                        // smuggler backup
                        var result = CreateBackup(backupFilePath, startDocumentEtag, context);
                        lastEtag = result.GetLastEtag();
                    }
                    else
                    {
                        // snapshot backup
                        lastEtag = DocumentsStorage.ReadLastEtag(tx.InnerTransaction);
                        _database.FullBackupTo(backupFilePath);
                    }
                }
                catch (Exception e)
                {
                    status.LocalBackup.Exception = e;
                    throw;
                }
            }
            return lastEtag;
        }

        private static string GetFullBackupExtension(BackupType type)
        {
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

        private SmugglerResult CreateBackup(string backupFilePath,
            long? startDocumentEtag, DocumentsOperationContext context)
        {
            // the last etag is already included in the last backup
            startDocumentEtag = startDocumentEtag == null ? 0 : ++startDocumentEtag;

            SmugglerResult result;
            using (var file = File.Open(backupFilePath, FileMode.CreateNew))
            {
                var smugglerSource = new DatabaseSource(_database, startDocumentEtag.Value);
                var smugglerDestination = new StreamDestination(file, context, smugglerSource);
                var smuggler = new DatabaseSmuggler(
                    smugglerSource,
                    smugglerDestination,
                    _database.Time,
                    token: _cancellationToken.Token);

                result = smuggler.Execute();
            }
            return result;
        }

        private static bool DirectoryContainsFullBackupOrSnapshot(string fullPath, BackupType backupType)
        {
            if (Directory.Exists(fullPath) == false)
                return false;

            var files = Directory.GetFiles(fullPath);
            if (files.Length == 0)
                return false;

            var backupExtension = GetFullBackupExtension(backupType);
            return files.Any(file =>
            {
                var extension = Path.GetExtension(file);
                return backupExtension.Equals(extension, StringComparison.OrdinalIgnoreCase);
            });
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

                await _serverStore.WaitForCommitIndexChange(RachisConsensus.CommitIndexModification.GreaterOrEqual, result.Etag);
            }
            catch (Exception e)
            {
                const string message = "Error saving the periodic backup status";

                if (_logger.IsOperationsEnabled)
                    _logger.Operations(message, e);

                _database.NotificationCenter.Add(AlertRaised.Create("Periodic Backup",
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
            if (CanBackupUsing(settings) == false)
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
                        localUploadStatus.Exception = e;
                    }
                    catch (Exception e)
                    {
                        localUploadStatus.Exception = e;
                        throw new InvalidOperationException($"Failed to backup to {backupDestination}", e);
                    }
                    finally
                    {
                        uploadProgress.ChangeState(UploadState.Done);
                    }
                }
            }));
        }

        private static bool CanBackupUsing(BackupSettings settings)
        {
            return settings != null &&
                   settings.Disabled == false &&
                   settings.HasSettings();
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
                nextFullBackup <= nextIncrementalBackup ?
                    nextFullBackup.Value :
                    nextIncrementalBackup.Value;

            return nextBackup;
        }

        private DateTime? GetNextBackupOccurrence(string backupFrequency,
            DateTime now, PeriodicBackupConfiguration configuration, bool skipErrorLog)
        {
            if (string.IsNullOrWhiteSpace(backupFrequency))
                return null;

            try
            {
                var backupParser = CrontabSchedule.Parse(backupFrequency);
                return backupParser.GetNextOccurrence(now);
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
            if (_cancellationToken.IsCancellationRequested)
                return;

            var backupDetails = (BackupTaskDetails)backupTaskDetails;

            if (ShouldRunBackupAfterTimerCallback(backupDetails, out PeriodicBackup periodicBackup) == false)
                return;

            CreateBackupTask(periodicBackup, backupDetails);
        }

        private void CreateBackupTask(PeriodicBackup periodicBackup, BackupTaskDetails backupDetails)
        {
            periodicBackup.RunningTask = Task.Run(async () =>
            {
                periodicBackup.BackupStatus = GetBackupStatus(periodicBackup.Configuration.TaskId, periodicBackup.BackupStatus);

                try
                {
                    await RunPeriodicBackup(periodicBackup.Configuration,
                        periodicBackup.BackupStatus, backupDetails.IsFullBackup);
                }
                finally
                {
                    if (_cancellationToken.IsCancellationRequested == false &&
                        periodicBackup.Disposed == false)
                    {
                        periodicBackup.BackupTimer.Dispose();
                        periodicBackup.BackupTimer = GetTimer(periodicBackup.Configuration, periodicBackup.BackupStatus);
                    }
                }
            }, _database.DatabaseShutdown);
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
            if (_cancellationToken.IsCancellationRequested)
                return;

            var backupDetails = (BackupTaskDetails)backupTaskDetails;

            if (ShouldRunBackupAfterTimerCallback(backupDetails, out PeriodicBackup periodicBackup) == false)
                return;

            var remainingInterval = backupDetails.NextBackup - MaxTimerTimeout;
            var shouldExecuteTimer = remainingInterval.TotalMilliseconds <= 0;
            if (shouldExecuteTimer)
            {
                CreateBackupTask(periodicBackup, backupDetails);
                return;
            }

            backupDetails.NextBackup = remainingInterval;
            var nextBackupTimeSpan = IsValidTimeSpanForTimer(remainingInterval) ? remainingInterval : MaxTimerTimeout;
            periodicBackup.BackupTimer.Change(nextBackupTimeSpan, Timeout.InfiniteTimeSpan);
        }

        private bool ShouldRunBackupAfterTimerCallback(BackupTaskDetails backupInfo, out PeriodicBackup periodicBackup)
        {
            if (_periodicBackups.TryGetValue(backupInfo.TaskId, out periodicBackup) == false)
            {
                // periodic backup doesn't exist anymore
                return false;
            }

            if (periodicBackup.Disposed)
            {
                // this periodic backup was canceled
                return false;
            }

            var databaseRecord = GetDatabaseRecord();
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
                     inMemoryBackupStatus.NodeTag == backupStatus.NodeTag)
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

                UpdatePeriodicBackups(newBackupTaskId, periodicBackupConfiguration, taskState);
            }

            RemoveInactiveCompletedTasks();

            var deletedBackupTaskIds = _periodicBackups.Keys.Except(allBackupTaskIds).ToList();
            foreach (var deletedBackupId in deletedBackupTaskIds)
            {
                if (_periodicBackups.TryRemove(deletedBackupId, out PeriodicBackup deletedBackup) == false)
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

        private void UpdatePeriodicBackups(long taskId,
            PeriodicBackupConfiguration newConfiguration,
            TaskStatus taskState)
        {
            Debug.Assert(taskId == newConfiguration.TaskId);

            var backupStatus = GetBackupStatus(taskId, inMemoryBackupStatus: null);
            if (_periodicBackups.TryGetValue(taskId, out PeriodicBackup existingBackupState) == false)
            {
                var newPeriodicBackup = new PeriodicBackup
                {
                    Configuration = newConfiguration
                };

                if (taskState == TaskStatus.ActiveByCurrentNode)
                    newPeriodicBackup.BackupTimer = GetTimer(newConfiguration, backupStatus);

                _periodicBackups.TryAdd(taskId, newPeriodicBackup);
                return;
            }

            if (existingBackupState.Configuration.Equals(newConfiguration))
            {
                // the username/password for the cloud backups might have changed,
                // and it will be reloaded on the next backup re-scheduling
                existingBackupState.Configuration = newConfiguration;
                if (taskState == TaskStatus.ActiveByCurrentNode)
                    existingBackupState.BackupTimer = GetTimer(newConfiguration, backupStatus);
                return;
            }

            // the backup configuration changed
            existingBackupState.DisableFutureBackups();
            TryAddInactiveRunningPeriodicBackups(existingBackupState.RunningTask);
            _periodicBackups.TryRemove(taskId, out _);

            var periodicBackup = new PeriodicBackup
            {
                Configuration = newConfiguration
            };

            if (taskState == TaskStatus.ActiveByCurrentNode)
                periodicBackup.BackupTimer = GetTimer(newConfiguration, backupStatus);

            _periodicBackups.TryAdd(taskId, periodicBackup);
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

            if (CanBackupUsing(configuration.LocalSettings) == false &&
                CanBackupUsing(configuration.S3Settings) == false &&
                CanBackupUsing(configuration.GlacierSettings) == false &&
                CanBackupUsing(configuration.AzureSettings) == false &&
                CanBackupUsing(configuration.FtpSettings) == false)
            {
                if (skipErrorLog == false)
                {
                    var message = $"All backup destinations are disabled for backup task id: {configuration.TaskId}";
                    _database.NotificationCenter.Add(AlertRaised.Create(
                        "Periodic Backup",
                        message,
                        AlertType.PeriodicBackup,
                        NotificationSeverity.Info));
                }

                return TaskStatus.Disabled;
            }

            var whoseTaskIsIt = databaseRecord.Topology.WhoseTaskIsIt(configuration, _serverStore.IsPassive());
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
            var allNextBackupTimeSpanSeconds = new List<int>();
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
                    allNextBackupTimeSpanSeconds.Add(nextBackup.TimeSpan.Seconds);
                }
            }

            return new BackupInfo
            {
                LastBackup = allBackupTicks.Count == 0 ? (DateTime?)null : new DateTime(allBackupTicks.Max()),
                IntervalUntilNextBackupInSec = allNextBackupTimeSpanSeconds.Count == 0 ?
                    0 : allNextBackupTimeSpanSeconds.Min()
            };
        }
    }
}
