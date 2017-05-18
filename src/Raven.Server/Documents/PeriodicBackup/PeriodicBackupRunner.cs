using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Smuggler;
using Raven.Client.Server.PeriodicBackup;
using Raven.Client.Util;
using Raven.Server.Config.Settings;
using Raven.Server.Documents.PeriodicBackup.Azure;
using Raven.Server.Documents.PeriodicBackup.Aws;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.NotificationCenter.Notifications.Details;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Smuggler.Documents;
using Raven.Server.Smuggler.Documents.Data;
using Raven.Server.Utils;
using Sparrow.Logging;
using DatabaseSmuggler = Raven.Server.Smuggler.Documents.DatabaseSmuggler;
using System.Collections.Concurrent;
using System.Linq;
using NCrontab.Advanced;
using Constants = Raven.Client.Constants;

namespace Raven.Server.Documents.PeriodicBackup
{
    public class LastBackupInfo
    {
        public BackupType Type { get; set; }

        public BackupDestination BackupDestination { get; set; }

        public DateTime LastFullBackup { get; set; }

        public DateTime LastIncrementalBackup { get; set; }
    }

    public enum BackupDestination
    {
        Local,
        Glacier,
        Aws,
        Azure
    }

    public class PeriodicBackupRunner : IDisposable
    {
        private readonly Logger _logger;

        private readonly DocumentDatabase _database;
        private readonly ServerStore _serverStore;
        private readonly CancellationTokenSource _cancellationToken;
        private readonly ConcurrentDictionary<long, PeriodicBackup> _periodicBackups
            = new ConcurrentDictionary<long, PeriodicBackup>();
        private readonly List<Task> _inactiveRunningPeriodicBackupsTasks = new List<Task>();

        //interval can be 2^32-2 milliseconds at most
        //this is the maximum interval acceptable in .Net's threading timer
        public readonly TimeSpan MaxTimerTimeout = TimeSpan.FromMilliseconds(Math.Pow(2, 32) - 2);

        private int? _exportLimit; //TODO: do we need this?
        
        public PeriodicBackupRunner(DocumentDatabase database, ServerStore serverStore)
        {
            _database = database;
            _serverStore = serverStore;
            _logger = LoggingSource.Instance.GetLogger<PeriodicBackupRunner>(_database.Name);
            _cancellationToken = CancellationTokenSource.CreateLinkedTokenSource(_database.DatabaseShutdown);
        }

        private Timer GenerateTimer(
            PeriodicBackupConfiguration configuration,
            PeriodicBackupStatus backupStatus)
        {
            var now = SystemTime.UtcNow;
            var nextFullBackup = GetNextBackupOccurrence(configuration.FullBackupFrequency, now);
            var nextIncrementalBackup = GetNextBackupOccurrence(configuration.IncrementalBackupFrequency, now);

            if (nextFullBackup == null && nextIncrementalBackup == null)
                return null;

            Debug.Assert(configuration.TaskId != null);

            if (backupStatus.LastFullBackup == null || 
                backupStatus.NodeTag != _serverStore.NodeTag ||
                backupStatus.BackupType != configuration.Type)
            {
                // Reasons to start a new full backup:
                // 1. there is no previous full backup, we are going to create one now
                // 2. the node which is responsible for the backup was replaced
                // 3. the backup type changed (e.g. from backup to snapshot)

                return CreateNewTimer(nextFullBackup ?? nextIncrementalBackup.Value, now, null,
                    new BackupTaskDetails
                    {
                        IsFullBackup = true,
                        TaskId = configuration.TaskId.Value
                    });
            }

            // we do have a full backup, 
            // let's see if the next task is going to be a full or incremental backup

            // Reasons  to start a new full backup:
            // 1. there is a full backup setup and the next full backup is before the incremental
            // 2. there is a full backup setup but the next incremental backup wasn't setup
            var isFullBackup = nextFullBackup != null && 
                (nextFullBackup <= nextIncrementalBackup || nextIncrementalBackup == null);

            var lastBackup = isFullBackup ? backupStatus.LastFullBackup : backupStatus.LastIncrementalBackup;

            return CreateNewTimer(
                isFullBackup ? nextFullBackup.Value : nextIncrementalBackup.Value,
                now,
                lastBackup,
                new BackupTaskDetails
                {
                    IsFullBackup = isFullBackup,
                    TaskId = configuration.TaskId.Value
                });
        }

        private static DateTime? GetNextBackupOccurrence(string fullBackupFrequency, DateTime now)
        {
            try
            {
                var fullBackupParser = CrontabSchedule.Parse(fullBackupFrequency);
                return fullBackupParser.GetNextOccurrence(now);
            }
            catch (Exception e)
            {
                var exception = e;
                //TODO: error to notification center
                return null;
            }
        }

        private class BackupTaskDetails
        {
            public long TaskId { get; set; }

            public bool IsFullBackup { get; set; }

            public TimeSpan CurrentInterval { get; set; }
        }

        private Timer CreateNewTimer(
            DateTime nextFullBackupDateTime,
            DateTime now,
            DateTime? lastBackup, 
            BackupTaskDetails backupTaskDetails)
        {
            var interval = nextFullBackupDateTime - now;

            if (_logger.IsInfoEnabled)
                _logger.Info($"Next {(backupTaskDetails.IsFullBackup ? "full" : "incremental")} " +
                             $"backup is in {interval.TotalMinutes} minutes");

            Timer timer;
            if (IsValidTimeSpanForTimer(interval))
            {
                var timeSinceLastBackup = now - (lastBackup ?? DateTime.MinValue);
                var nextBackupTimeSpan = timeSinceLastBackup >= interval ? TimeSpan.Zero : interval - timeSinceLastBackup;
                backupTaskDetails.CurrentInterval = nextBackupTimeSpan;
                timer = new Timer(TimerCallback, backupTaskDetails, nextBackupTimeSpan, Timeout.InfiniteTimeSpan);
            }
            else
            {
                backupTaskDetails.CurrentInterval = interval;
                timer = new Timer(LongPeriodTimerCallback, backupTaskDetails, MaxTimerTimeout, Timeout.InfiniteTimeSpan);
            }

            return timer;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool IsValidTimeSpanForTimer(TimeSpan timespan)
        {
            return timespan < MaxTimerTimeout;
        }

        private void TimerCallback(object backupTaskDetails)
        {
            if (_cancellationToken.IsCancellationRequested)
                return;

            var backupInfo = (BackupTaskDetails)backupTaskDetails;

            PeriodicBackup periodicBackup;
            if (_periodicBackups.TryGetValue(backupInfo.TaskId, out periodicBackup) == false)
            {
                // periodic backup doesn't exist anymore
                return;
            }

            if (periodicBackup.Disposed)
                return;

            if (periodicBackup.Configuration.Disabled)
                return;

            using (_serverStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (context.OpenReadTransaction())
            {
                var databaseRecord = _serverStore.Cluster.ReadDatabase(context, _database.Name);
                var whoseTaskIsIt = databaseRecord.Topology.WhoseTaskIsIt(periodicBackup.Configuration);
                if (whoseTaskIsIt != _serverStore.NodeTag)
                {
                    if (_logger.IsInfoEnabled)
                        _logger.Info("Backup job is skipped because it is managed " +
                                     $"by '{whoseTaskIsIt}' node and not this node ({_serverStore.NodeTag})");

                    return;
                }
            }

            periodicBackup.RunningTask = Task.Run(async () =>
            {
                Debug.Assert(periodicBackup.Configuration.TaskId != null);
                var backupStatus = GetBackupStatus(periodicBackup.Configuration.TaskId.Value);
                try
                {
                    await RunPeriodicBackup(periodicBackup.Configuration,
                        backupStatus, backupInfo.IsFullBackup);
                }
                finally
                {
                    if (_cancellationToken.IsCancellationRequested == false)
                    {
                        periodicBackup.BackupTimer?.Dispose();

                        periodicBackup.BackupTimer = GenerateTimer(periodicBackup.Configuration, backupStatus);
                    }
                }
            }, _database.DatabaseShutdown);
        }

        private void LongPeriodTimerCallback(object backupTaskDetails)
        {
            if (_cancellationToken.IsCancellationRequested)
                return;

            var backupInfo = (BackupTaskDetails)backupTaskDetails;

            PeriodicBackup periodicBackup;
            if (_periodicBackups.TryGetValue(backupInfo.TaskId, out periodicBackup) == false)
            {
                // periodic backup doesn't exist anymore
                return;
            }

            if (periodicBackup.Disposed)
                return;

            var remainingInterval = backupInfo.CurrentInterval - MaxTimerTimeout;
            var shouldExecuteTimer = remainingInterval.TotalMilliseconds <= 0;
            if (shouldExecuteTimer)
            {
                TimerCallback(backupInfo);
                return;
            }

            backupInfo.CurrentInterval = remainingInterval;

            periodicBackup.BackupTimer?.Dispose();
            periodicBackup.BackupTimer = new Timer(LongPeriodTimerCallback, 
                backupInfo,
                IsValidTimeSpanForTimer(remainingInterval) ? remainingInterval : MaxTimerTimeout, 
                Timeout.InfiniteTimeSpan);



            //TODO: Maybe this is a better option? :

            /*Debug.Assert(periodicBackup.Configuration.TaskId != null);

            var status = GetBackupStatus(periodicBackup.Configuration.TaskId.Value);

            periodicBackup.BackupTimer = GenerateTimer(periodicBackup.Configuration, status);*/
        }

        private async Task RunPeriodicBackup(
            PeriodicBackupConfiguration configuration,
            PeriodicBackupStatus status,
            bool isFullBackup)
        {
            try
            {
                var totalSw = Stopwatch.StartNew();
                DocumentsOperationContext context;
                using (_database.DocumentsStorage.ContextPool.AllocateOperationContext(out context))
                using (var tx = context.OpenReadTransaction())
                {
                    var backupToLocalFolder = CanBackupUsing(configuration.LocalSettings);

                    var backupDirectory = backupToLocalFolder
                        ? new PathSetting(configuration.LocalSettings.FolderPath)
                        : _database.Configuration.Core.DataDirectory.Combine("PeriodicBackup-Temp");

                    if (Directory.Exists(backupDirectory.FullPath) == false)
                        Directory.CreateDirectory(backupDirectory.FullPath);

                    var now = SystemTime.UtcNow.ToString("yyyy-MM-dd-HH-mm", CultureInfo.InvariantCulture);

                    // check if we need to do a new full backup
                    if (isFullBackup ||
                        status.NodeTag != _serverStore.NodeTag || // last backup was performed by a different node
                        status.LocalBackupStatus?.BackupDirectory == null || // no previous backups
                        DirectoryExistsOrContainsFiles(status.LocalBackupStatus) == false || // folder doesn't contain any backups
                        status.LastEtag == null) // last document etag wasn't updated
                    {
                        isFullBackup = true;
                        if (status.LocalBackupStatus == null)
                            status.LocalBackupStatus = new LocalBackupStatus();

                        status.LocalBackupStatus.BackupDirectory =
                            backupDirectory.Combine($"{now}.ravendb-{_database.Name}-{_serverStore.NodeTag}-{configuration.Type.ToString().ToLower()}").FullPath;
                        Directory.CreateDirectory(status.LocalBackupStatus.BackupDirectory);
                    }

                    if (_logger.IsInfoEnabled)
                    {
                        var fullBackupText = "a " + (configuration.Type == BackupType.Backup ? "full backup" : "snapshot");
                        _logger.Info($"Creating {(isFullBackup ? fullBackupText : "an incremental backup")}");
                    }

                    if (isFullBackup == false)
                    {
                        var currentLastEtag = DocumentsStorage.ReadLastEtag(tx.InnerTransaction);
                        // no-op if nothing has changed
                        if (currentLastEtag == status.LastEtag)
                            return;
                    }

                    string backupFilePath;
                    var startDocumentEtag = isFullBackup == false ? status.LastEtag : null;
                    var fileName = GetFileName(configuration, status, isFullBackup, now, out backupFilePath);

                    var lastEtag = CreateLocalBackupOrSnapshot(configuration,
                        isFullBackup, status, backupFilePath, startDocumentEtag, context, tx);

                    if (isFullBackup == false &&
                        lastEtag == status.LastEtag)
                    {
                        // no-op if nothing has changed

                        if (_logger.IsInfoEnabled)
                            _logger.Info("Periodic backup returned prematurely, " +
                                         "nothing has changed since last backup");
                        return;
                    }

                    try
                    {
                        await UploadToServer(configuration, status, backupFilePath, fileName, isFullBackup);
                    }
                    finally
                    {
                        // if user did not specify local folder we delete temporary file.
                        if (backupToLocalFolder == false)
                        {
                            IOExtensions.DeleteFile(backupFilePath);
                        }
                    }

                    status.LastEtag = lastEtag;
                }

                totalSw.Stop();

                if (_logger.IsInfoEnabled)
                {
                    var fullBackupText = "a " + (configuration.Type == BackupType.Backup ? " full backup" : " snapshot");
                    _logger.Info($"Successfully created {(isFullBackup ? fullBackupText : "an incremental backup")} " +
                                 $"in {totalSw.ElapsedMilliseconds:#,#;;0} ms");
                }

                status.DurationInMs = totalSw.ElapsedMilliseconds;
                status.NodeTag = _serverStore.NodeTag;
                WriteStatus(status);

                _exportLimit = null;
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
                _exportLimit = 100;
                const string message = "Error when performing periodic backup";

                if (_logger.IsOperationsEnabled)
                    _logger.Operations(message, e);

                _database.NotificationCenter.Add(AlertRaised.Create("Periodic Backup",
                    message,
                    AlertType.PeriodicBackup,
                    NotificationSeverity.Error,
                    details: new ExceptionDetails(e)));
            }
        }

        private string GetFileName(PeriodicBackupConfiguration configuration, 
            PeriodicBackupStatus status, bool isFullBackup, string now, out string backupFilePath)
        {
            string fileName;
            if (isFullBackup)
            {
                // create filename for full backup/snapshot
                fileName = $"{now}.ravendb-{GetFullBackupName(configuration.Type)}";
                backupFilePath = Path.Combine(status.LocalBackupStatus.BackupDirectory, fileName);
                if (File.Exists(backupFilePath))
                {
                    var counter = 1;
                    while (true)
                    {
                        fileName = $"{now} - {counter}.${GetFullBackupExtension(configuration.Type)}";
                        backupFilePath = Path.Combine(status.LocalBackupStatus.BackupDirectory, fileName);

                        if (File.Exists(backupFilePath) == false)
                            break;

                        counter++;
                    }
                }
            }
            else
            {
                // create filename for incremental backup
                fileName = $"{now}-0.${Constants.Documents.PeriodicBackup.IncrementalBackupExtension}";
                backupFilePath = Path.Combine(status.LocalBackupStatus.BackupDirectory, fileName);
                if (File.Exists(backupFilePath))
                {
                    var counter = 1;
                    while (true)
                    {
                        fileName = $"{now}-{counter}.${Constants.Documents.PeriodicBackup.IncrementalBackupExtension}";
                        backupFilePath = Path.Combine(status.LocalBackupStatus.BackupDirectory, fileName);

                        if (File.Exists(backupFilePath) == false)
                            break;

                        counter++;
                    }
                }
            }

            return fileName;
        }

        private long CreateLocalBackupOrSnapshot(PeriodicBackupConfiguration configuration, bool isFullBackup, PeriodicBackupStatus status, string backupFilePath,
            long? startDocumentEtag, DocumentsOperationContext context, DocumentsTransaction tx)
        {
            long lastEtag;
            var exception = new Reference<Exception>();
            using (status.LocalBackupStatus.Update(isFullBackup, exception))
            {
                try
                {
                    if (configuration.Type == BackupType.Backup ||
                        configuration.Type == BackupType.Snapshot && isFullBackup == false)
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
                    exception.Value = e;
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

        private string GetFullBackupName(BackupType type)
        {
            switch (type)
            {
                case BackupType.Backup:
                    return "full-backup";
                case BackupType.Snapshot:
                    return "snapshot";
                default:
                    throw new ArgumentOutOfRangeException(nameof(type), type, null);
            }
        }

        private SmugglerResult CreateBackup(string backupFilePath, 
            long? startDocsEtag, DocumentsOperationContext context)
        {
            SmugglerResult result;
            using (var file = File.Open(backupFilePath, FileMode.CreateNew))
            {
                var smugglerSource = new DatabaseSource(_database, startDocsEtag ?? 0);
                var smugglerDestination = new StreamDestination(file, context, smugglerSource);
                var smuggler = new DatabaseSmuggler(
                    smugglerSource,
                    smugglerDestination,
                    _database.Time,
                    new DatabaseSmugglerOptions
                    {
                        RevisionDocumentsLimit = _exportLimit
                    },
                    token: _cancellationToken.Token);

                result = smuggler.Execute();
            }
            return result;
        }

        public bool DirectoryExistsOrContainsFiles(LocalBackupStatus localStatus)
        {
            if (Directory.Exists(localStatus.BackupDirectory) == false)
                return false;

            return Directory.GetFiles(localStatus.BackupDirectory).Length != 0;
        }

        private void WriteStatus(PeriodicBackupStatus status)
        {
            if (_cancellationToken.IsCancellationRequested)
                return;

            //TODO: write status to raft

            DocumentsOperationContext context;
            using (_database.DocumentsStorage.ContextPool.AllocateOperationContext(out context))
            using (var tx = context.OpenWriteTransaction())
            {
                _database
                    .ConfigurationStorage
                    .PeriodicBackupStorage
                    .SetDatabasePeriodicBackupStatus(context, status);

                tx.Commit();
            }
        }

        private async Task UploadToServer(
            PeriodicBackupConfiguration configuration,
            PeriodicBackupStatus backupStatus,
            string backupPath, string fileName, bool isFullBackup)
        {
            if (_cancellationToken.IsCancellationRequested)
                return;

            var tasks = new List<Task>();

            if (CanBackupUsing(configuration.S3Settings))
            {
                tasks.Add(Task.Run(async () =>
                {
                    if (backupStatus.S3BackupStatus == null)
                        backupStatus.S3BackupStatus = new S3BackupStatus();

                    var exception = new Reference<Exception>();
                    using (backupStatus.S3BackupStatus.Update(isFullBackup, exception))
                    {
                        try
                        {
                            await UploadToS3(configuration.S3Settings, backupPath, fileName, isFullBackup, configuration.Type);
                        }
                        catch (Exception e)
                        {
                            exception.Value = e;
                            throw;
                        }
                    }
                }));
            }

            if (CanBackupUsing(configuration.GlacierSettings))
            {
                tasks.Add(Task.Run(async () =>
                {
                    if (backupStatus.GlacierBackupStatus == null)
                        backupStatus.GlacierBackupStatus = new GlacierBackupStatus();

                    var exception = new Reference<Exception>();
                    using (backupStatus.GlacierBackupStatus.Update(isFullBackup, exception))
                    {
                        try
                        {
                            await UploadToGlacier(configuration.GlacierSettings, backupPath, fileName);
                        }
                        catch (Exception e)
                        {
                            exception.Value = e;
                            throw;
                        }
                    }
                }));
            }

            if (CanBackupUsing(configuration.AzureSettings))
            {
                tasks.Add(Task.Run(async () =>
                {
                    if (backupStatus.AzureBackupStatus == null)
                        backupStatus.AzureBackupStatus = new AzureBackupStatus();

                    var exception = new Reference<Exception>();
                    using (backupStatus.AzureBackupStatus.Update(isFullBackup, exception))
                    {
                        try
                        {
                            await UploadToAzure(configuration.AzureSettings, backupPath, fileName, isFullBackup, configuration.Type);
                        }
                        catch (Exception e)
                        {
                            exception.Value = e;
                            throw;
                        }
                    }
                }));
            }

            await Task.WhenAll(tasks);
        }

        private static bool CanBackupUsing(BackupSettings settings)
        {
            return settings != null &&
                   settings.Disabled == false &&
                   settings.HasSettings();
        }

        private async Task UploadToS3(S3Settings settings, string backupPath, string fileName, bool isFullBackup, BackupType backupType)
        {
            if (settings.AwsAccessKey == Constants.Documents.Encryption.DataCouldNotBeDecrypted ||
                settings.AwsSecretKey == Constants.Documents.Encryption.DataCouldNotBeDecrypted)
            {
                throw new InvalidOperationException("Could not decrypt the AWS access settings, " +
                                                    "if you are running on IIS, " +
                                                    "make sure that load user profile is set to true.");
            }

            using (var client = new RavenAwsS3Client(settings.AwsAccessKey, settings.AwsSecretKey, settings.AwsRegionName ?? RavenAwsClient.DefaultRegion))
            using (var fileStream = File.OpenRead(backupPath))
            {
                var key = CombinePathAndKey(settings.RemoteFolderName, fileName);
                await client.PutObject(settings.BucketName, key, fileStream, new Dictionary<string, string>
                {
                    {"Description", GetArchiveDescription(isFullBackup, backupType)}
                }, 60 * 60);

                if (_logger.IsInfoEnabled)
                    _logger.Info(string.Format("Successfully uploaded backup {0} to S3 bucket {1}, " +
                                               "with key {2}", fileName, settings.BucketName, key));
            }
        }

        private async Task UploadToGlacier(GlacierSettings settings, string backupPath, string fileName)
        {

            if (settings.AwsAccessKey == Constants.Documents.Encryption.DataCouldNotBeDecrypted ||
                settings.AwsSecretKey == Constants.Documents.Encryption.DataCouldNotBeDecrypted)
            {
                throw new InvalidOperationException("Could not decrypt the AWS access settings, " +
                                                    "if you are running on IIS, " +
                                                    "make sure that load user profile is set to true.");
            }

            using (var client = new RavenAwsGlacierClient(settings.AwsAccessKey, settings.AwsSecretKey, settings.AwsRegionName ?? RavenAwsClient.DefaultRegion))
            using (var fileStream = File.OpenRead(backupPath))
            {
                var archiveId = await client.UploadArchive(settings.VaultName, fileStream, fileName, 60 * 60);
                if (_logger.IsInfoEnabled)
                    _logger.Info($"Successfully uploaded backup {fileName} to Glacier, archive ID: {archiveId}");
            }
        }

        private async Task UploadToAzure(AzureSettings settings, string backupPath, string fileName, bool isFullBackup, BackupType backupType)
        {
            if (settings.StorageAccount == Constants.Documents.Encryption.DataCouldNotBeDecrypted ||
                settings.StorageKey == Constants.Documents.Encryption.DataCouldNotBeDecrypted)
            {
                throw new InvalidOperationException("Could not decrypt the Azure access settings, " +
                                                    "if you are running on IIS, " +
                                                    "make sure that load user profile is set to true.");
            }

            using (var client = new RavenAzureClient(settings.StorageAccount, settings.StorageKey, settings.StorageContainer))
            {
                await client.PutContainer();
                using (var fileStream = File.OpenRead(backupPath))
                {
                    var key = CombinePathAndKey(settings.RemoteFolderName, fileName);
                    await client.PutBlob(key, fileStream, new Dictionary<string, string>
                    {
                        {"Description", GetArchiveDescription(isFullBackup, backupType)}
                    });

                    if (_logger.IsInfoEnabled)
                        _logger.Info($"Successfully uploaded backup {fileName} " +
                                     $"to Azure container {settings.StorageContainer}, with key {key}");
                }
            }
        }

        private static string CombinePathAndKey(string path, string fileName)
        {
            return string.IsNullOrEmpty(path) == false ? path + "/" + fileName : fileName;
        }

        private string GetArchiveDescription(bool isFullBackup, BackupType backupType)
        {
            var fullBackupText = backupType == BackupType.Backup ? "Full backup" : "A snapshot";
            return $"{(isFullBackup ? fullBackupText : "Incremental backup")} for db {_database.Name} at {SystemTime.UtcNow}";
        }

        public void Dispose()
        {
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

        private Timer GetNewBackupTimer(PeriodicBackupConfiguration configuration)
        {
            Debug.Assert(configuration.TaskId != null);

            var periodicBackupStatus = GetBackupStatus(configuration.TaskId.Value);
            return GenerateTimer(configuration, periodicBackupStatus);
        }

        private PeriodicBackupStatus GetBackupStatus(long taskId)
        {
            //TODO: get this from raft
            throw new NotImplementedException();
        }

        public void UpdateConfigurations(DatabaseRecord databaseRecord)
        {
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
            foreach (var periodicBackup in databaseRecord.PeriodicBackups)
            {
                var newBackupTaskId = periodicBackup.Key;
                allBackupTaskIds.Add(newBackupTaskId);
                
                var taskState = GetTaskStatus(databaseRecord, periodicBackup.Value);
                var newConfiguration = periodicBackup.Value;

                _periodicBackups.AddOrUpdate(periodicBackup.Key, _ =>
                    {
                        var newPeriodicBackupState = new PeriodicBackup
                        {
                            Configuration = newConfiguration
                        };

                        if (taskState == TaskStatus.ActiveByCurrentNode)
                            newPeriodicBackupState.BackupTimer = GetNewBackupTimer(newConfiguration);

                        return newPeriodicBackupState;
                }, (_, existingBackupState) =>
                    {
                        var existingFullBackupFrequency = existingBackupState.Configuration.FullBackupFrequency;
                        var existingIncrementalBackupFrequency = existingBackupState.Configuration.IncrementalBackupFrequency;

                        existingBackupState.Configuration = newConfiguration;

                        if (taskState != TaskStatus.ActiveByCurrentNode)
                        {
                            // disable all future backups
                            existingBackupState.DisableFutureBackups();
                            TryAddInactiveRunningPeriodicBackups(existingBackupState.RunningTask);
                            return existingBackupState;
                        }

                        if (existingFullBackupFrequency == newConfiguration.FullBackupFrequency ||
                            existingIncrementalBackupFrequency == newConfiguration.IncrementalBackupFrequency)
                        {
                            // the backup frequency hasn't changed
                            // no need to generate new timers
                            // the new configuration will reload on timer callback
                            return existingBackupState;
                        }

                        existingBackupState.DisableFutureBackups();
                        TryAddInactiveRunningPeriodicBackups(existingBackupState.RunningTask);

                        var newPeriodicBackupState = new PeriodicBackup
                        {
                            Configuration = newConfiguration,
                            BackupTimer = GetNewBackupTimer(newConfiguration)
                        };

                        return newPeriodicBackupState;
                    }
                );
            }

            var deletedBackupTaskIds = _periodicBackups.Keys.Except(allBackupTaskIds).ToList();
            foreach (var deletedBackupId in deletedBackupTaskIds)
            {
                PeriodicBackup deletedBackup;
                if (_periodicBackups.TryRemove(deletedBackupId, out deletedBackup) == false)
                    continue;

                // stopping any future backups
                // currently running backups will continue to run
                deletedBackup.DisableFutureBackups();

                if (deletedBackup.RunningTask == null ||
                    deletedBackup.RunningTask.IsCompleted)
                    continue;

                TryAddInactiveRunningPeriodicBackups(deletedBackup.RunningTask);
            }
        }

        private enum TaskStatus
        {
            Disabled,
            ActiveByCurrentNode,
            ActiveByOtherNode
        }

        private TaskStatus GetTaskStatus(
            DatabaseRecord databaseRecord, 
            PeriodicBackupConfiguration configuration)
        {
            if (configuration.Disabled == false)
                return TaskStatus.Disabled;

            var whoseTaskIsIt = databaseRecord.Topology.WhoseTaskIsIt(configuration);
            if (whoseTaskIsIt == _serverStore.NodeTag)
                return TaskStatus.ActiveByCurrentNode;

            if (_logger.IsInfoEnabled)
                _logger.Info("Backup job is skipped because it is managed " +
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
    }
}