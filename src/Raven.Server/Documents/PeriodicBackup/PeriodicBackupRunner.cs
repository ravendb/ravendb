using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client;
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

namespace Raven.Server.Documents.PeriodicBackup
{
    /*public class BackupInfo
    {
        public LastBackupInfo LastBackupInfo { get; set; }

        public NextBackupInfo NextBackupInfo { get; set; }
    }*/

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

    public class NextBackupInfo
    {
        public DateTime NextFullBackup { get; set; }

        public DateTime NextIncrementalBackup { get; set; }
    }

    public class PeriodicBackupState
    {
        public Timer FullBackupTimer { get; set; }

        public DateTime? NextFullBackup { get; set; }

        public Timer IncrementalBackupTimer { get; set; }

        public DateTime? NextIncrementalBackup { get; set; }

        public Task RunningTask { get; set; }

        public PeriodicBackupConfiguration Configuration { get; set; }

        public string WhoseTaskIsIt { get; set; }

        public void SetupFutureBackups()
        {
            SetupFutureFullBackup();
            SetupFutureIncrementalBackup();
        }

        public void SetupFutureFullBackup()
        {
            Debug.Assert(Configuration != null);
            //TODO: start timer for full backup
            //Configuration.FullBackupFrequency;
        }

        public void SetupFutureIncrementalBackup()
        {
            Debug.Assert(Configuration != null);
            //TODO: start timer for incremental backup
            //Configuration.IncrementalBackupFrequency;
        }

        public void DisableFutureBackups()
        {
            FullBackupTimer?.Dispose();
            IncrementalBackupTimer?.Dispose();
        }

        public void RecreateFutureBackupsIfNeeded(
            string existingFullBackupFrequency, 
            string existingIncrementalBackupFrequency, 
            Action onConfigurationChange)
        {
            if (existingFullBackupFrequency != Configuration.FullBackupFrequency)
            {
                // recreate the future full backup timer
                FullBackupTimer?.Dispose();
                onConfigurationChange();
                SetupFutureFullBackup();
            }

            if (existingIncrementalBackupFrequency != Configuration.IncrementalBackupFrequency)
            {
                // recreate the future incremental backup timer
                IncrementalBackupTimer?.Dispose();
                onConfigurationChange();
                SetupFutureIncrementalBackup();
            }
        }
    }

    public class PeriodicBackupRunner : IDisposable
    {
        private readonly Logger _logger;

        private readonly DocumentDatabase _database;
        private readonly ServerStore _serverStore;
        //TODO: private readonly PeriodicBackupConfiguration _configuration;
        //TODO: private readonly PeriodicBackupStatus _status;
        private readonly ConcurrentDictionary<long, PeriodicBackupState> _periodicBackups
            = new ConcurrentDictionary<long, PeriodicBackupState>();
        private readonly List<Task> _inactiveRunningPeriodicBackupsTasks = new List<Task>();

        //TODO: public PeriodicBackupConfiguration Configuration => _configuration;

        // This will be canceled once the configuration document will be changed
        private readonly CancellationTokenSource _cancellationToken;

        /*private Timer _incrementalExportTimer;
        private Timer _fullExportTimer;*/
        private TimeSpan _incrementalIntermediateInterval;
        private TimeSpan _fullExportIntermediateInterval;

        public readonly TimeSpan FullExportInterval;
        public readonly TimeSpan IncrementalInterval;

        //public DateTime FullExportTime => _status.LastFullBackupAt;
        //public DateTime ExportTime => _status.LastBackupAt;

        private int? _exportLimit;

        //interval can be 2^32-2 milliseconds at most
        //this is the maximum interval acceptable in .Net's threading timer
        public readonly TimeSpan MaxTimerTimeout = TimeSpan.FromMilliseconds(Math.Pow(2, 32) - 2);

        /* TODO: How should we set this value, in the configuration document? If so, how do we encrypt them? */
        /*private string _awsAccessKey, _awsSecretKey;
        private string _azureStorageAccount, _azureStorageKey;*/

        //private Task _runningTask;

        public PeriodicBackupRunner(DocumentDatabase database, ServerStore serverStore)
        {
            _database = database;
            _serverStore = serverStore;
            _logger = LoggingSource.Instance.GetLogger<PeriodicBackupRunner>(_database.Name);
            _cancellationToken = CancellationTokenSource.CreateLinkedTokenSource(_database.DatabaseShutdown);

            /*if (configuration.IncrementalIntervalInMilliseconds.HasValue && configuration.IncrementalIntervalInMilliseconds.Value > 0)
            {
                _incrementalIntermediateInterval = IncrementalInterval = TimeSpan.FromMilliseconds(configuration.IncrementalIntervalInMilliseconds.Value);
                if (_logger.IsInfoEnabled)
                    _logger.Info($"Incremental periodic export started, will export every {IncrementalInterval.TotalMinutes} minutes");

                if (IsValidTimespanForTimer(IncrementalInterval))
                {
                    var timeSinceLastExport = SystemTime.UtcNow - _status.LastBackupAt;
                    var nextExport = timeSinceLastExport >= IncrementalInterval ? TimeSpan.Zero : IncrementalInterval - timeSinceLastExport;

                    _incrementalExportTimer = new Timer(TimerCallback, false, nextExport, IncrementalInterval);
                }
                else
                {
                    _incrementalExportTimer = new Timer(LongPeriodTimerCallback, false, MaxTimerTimeout, Timeout.InfiniteTimeSpan);
                }
            }
            else
            {
                if (_logger.IsInfoEnabled)
                    _logger.Info("Incremental periodic export interval is set to zero or less, incremental periodic export is now disabled");
            }

            if (configuration.FullExportIntervalInMilliseconds.HasValue && configuration.FullExportIntervalInMilliseconds.Value > 0)
            {
                _fullExportIntermediateInterval = FullExportInterval = TimeSpan.FromMilliseconds(configuration.FullExportIntervalInMilliseconds.Value);
                if (_logger.IsInfoEnabled)
                    _logger.Info("Full periodic export started, will export every" + FullExportInterval.TotalMinutes + "minutes");

                if (IsValidTimespanForTimer(FullExportInterval))
                {
                    var timeSinceLastExport = SystemTime.UtcNow - _status.LastFullBackupAt;
                    var nextExport = timeSinceLastExport >= FullExportInterval ? TimeSpan.Zero : FullExportInterval - timeSinceLastExport;

                    _fullExportTimer = new Timer(TimerCallback, true, nextExport, FullExportInterval);
                }
                else
                {
                    _fullExportTimer = new Timer(LongPeriodTimerCallback, true, MaxTimerTimeout, Timeout.InfiniteTimeSpan);
                }
            }
            else
            {
                if (_logger.IsInfoEnabled)
                    _logger.Info("Full periodic export interval is set to zero or less, full periodic export is now disabled");
            }*/
        }

        private void LongPeriodTimerCallback(object fullExport)
        {
            if (_cancellationToken.IsCancellationRequested)
                return;

            /*lock (this)
            {
                if ((bool)fullExport)
                {
                    _fullExportTimer?.Dispose();
                    _fullExportTimer = ScheduleNextLongTimer(true);
                }
                else
                {
                    _incrementalExportTimer?.Dispose();
                    _incrementalExportTimer = ScheduleNextLongTimer(false);
                }
            }*/
        }

        private Timer ScheduleNextLongTimer(bool isFullbackup)
        {
            var intermediateTimespan = isFullbackup ? _fullExportIntermediateInterval : _incrementalIntermediateInterval;
            var remainingInterval = intermediateTimespan - MaxTimerTimeout;
            var shouldExecuteTimer = remainingInterval.TotalMilliseconds <= 0;
            if (shouldExecuteTimer)
            {
                TimerCallback(isFullbackup);
            }

            if (isFullbackup)
                _fullExportIntermediateInterval = shouldExecuteTimer ? FullExportInterval : remainingInterval;
            else
                _incrementalIntermediateInterval = shouldExecuteTimer ? IncrementalInterval : remainingInterval;

            return new Timer(LongPeriodTimerCallback, isFullbackup, shouldExecuteTimer ? MaxTimerTimeout : remainingInterval, Timeout.InfiniteTimeSpan);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool IsValidTimespanForTimer(TimeSpan timespan)
        {
            return timespan < MaxTimerTimeout;
        }

        private void TimerCallback(object fullBackup)
        {
            /*if (_runningTask != null || _cancellationToken.IsCancellationRequested)
                return;*/


            // we have shared lock for both incremental and full backup.
            /*lock (this)
            {
                if (_runningTask != null || _cancellationToken.IsCancellationRequested)
                    return;
                _runningTask = Task.Run(async () =>
                {
                    try
                    {
                        await RunPeriodicExport((bool)fullBackup);
                    }
                    finally
                    {
                        lock (this)
                        {
                            _runningTask = null;
                        }
                    }
                }, _database.DatabaseShutdown);
            }*/
        }

        private async Task RunPeriodicBackup(PeriodicBackupConfiguration configuration, bool isFullBackup)
        {
            if (_cancellationToken.IsCancellationRequested)
                return;

            if (configuration.Disabled)
                return;

            using (_serverStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (context.OpenReadTransaction())
            {
                var databaseRecord = _serverStore.Cluster.ReadDatabase(context, _database.Name);
                var whoseTaskIsIt = databaseRecord.Topology.WhoseTaskIsIt(configuration);
                if (whoseTaskIsIt != _serverStore.NodeTag)
                {
                    if (_logger.IsInfoEnabled)
                        _logger.Info("Backup job is skipped because it is managed " +
                                     $"by '{whoseTaskIsIt}' node and not this node ({_serverStore.NodeTag})");

                    return;
                }
            }

            try
            {
                DocumentsOperationContext context;
                using (_database.DocumentsStorage.ContextPool.AllocateOperationContext(out context))
                {
                    var totalSw = Stopwatch.StartNew();
                    using (var tx = context.OpenReadTransaction())
                    {
                        var status = new PeriodicBackupStatus(); //TODO: get this from raft
                        var backupToLocalFolder = CanBackupUsing(configuration.LocalSettings);

                        var backupDirectory = backupToLocalFolder ? 
                            new PathSetting(configuration.LocalSettings.FolderPath) : 
                            _database.Configuration.Core.DataDirectory.Combine("PeriodicBackup-Temp");

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
                            //TODO: add node tag
                            status.LocalBackupStatus.BackupDirectory = 
                                backupDirectory.Combine($"{now}.ravendb-{_database.Name}-{_serverStore.NodeTag}-{configuration.Type.ToString().ToLower()}").FullPath;
                            Directory.CreateDirectory(status.LocalBackupStatus.BackupDirectory);
                        }

                        if (_logger.IsInfoEnabled) //TODO: explain backup type
                            _logger.Info($"Creating {(isFullBackup ? "a full " : "an incremental backup")}");

                        if (isFullBackup == false)
                        {
                            var currentLastEtag = DocumentsStorage.ReadLastEtag(tx.InnerTransaction);
                            // no-op if nothing has changed
                            if (currentLastEtag == status.LastEtag)
                                return;
                        }

                        string backupFilePath;
                        long? startDocumentEtag = null;
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

                            startDocumentEtag = status.LastEtag;
                        }

                        var sw = Stopwatch.StartNew();
                        long lastEtag;
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

                        status.LocalBackupStatus.Update(isFullBackup, sw.ElapsedMilliseconds);

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

                        //TODO: write status to raft
                        WriteStatus(status);
                    }
                    if (_logger.IsInfoEnabled) //TODO:
                        _logger.Info($"Successfully created a {(isFullBackup ? "full" : "incremental")} " +
                                     $"export in {totalSw.ElapsedMilliseconds:#,#;;0} ms.");

                    _exportLimit = null;
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
                _exportLimit = 100;
                const string message = "Error when performing periodic export";

                if (_logger.IsOperationsEnabled)
                    _logger.Operations(message, e);

                _database.NotificationCenter.Add(AlertRaised.Create("Periodic Backup",
                    message,
                    AlertType.PeriodicExport,
                    NotificationSeverity.Error,
                    details: new ExceptionDetails(e)));
            }
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
                        RevisionDocumentsLimit = _exportLimit //TODO?
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
            string exportPath, string fileName, bool isFullBackup)
        {
            if (_cancellationToken.IsCancellationRequested)
                return;

            var tasks = new List<Task>();

            if (CanBackupUsing(configuration.S3Settings))
            {
                tasks.Add(Task.Run(async () =>
                {
                    var sp = Stopwatch.StartNew();
                    await UploadToS3(configuration.S3Settings, exportPath, fileName, isFullBackup);
                    
                    if (backupStatus.S3BackupStatus == null)
                        backupStatus.S3BackupStatus = new S3BackupStatus();

                    backupStatus.S3BackupStatus.Update(isFullBackup, sp.ElapsedMilliseconds);
                }));
            }

            if (CanBackupUsing(configuration.GlacierSettings))
            {
                tasks.Add(Task.Run(async () =>
                {
                    var sp = Stopwatch.StartNew();
                    await UploadToGlacier(configuration.GlacierSettings, exportPath, fileName, isFullBackup);

                    if (backupStatus.GlacierBackupStatus == null)
                        backupStatus.GlacierBackupStatus = new GlacierBackupStatus();

                    backupStatus.GlacierBackupStatus.Update(isFullBackup, sp.ElapsedMilliseconds);
                }));
            }

            if (CanBackupUsing(configuration.AzureSettings))
            {
                tasks.Add(Task.Run(async () =>
                {
                    var sp = Stopwatch.StartNew();
                    await UploadToAzure(configuration.AzureSettings, exportPath, fileName, isFullBackup);

                    if (backupStatus.AzureBackupStatus == null)
                        backupStatus.AzureBackupStatus = new AzureBackupStatus();

                    backupStatus.AzureBackupStatus.Update(isFullBackup, sp.ElapsedMilliseconds);
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

        private async Task UploadToS3(S3Settings settings, string exportPath, string fileName, bool isFullBackup)
        {
            if (settings.AwsAccessKey == Constants.Documents.Encryption.DataCouldNotBeDecrypted ||
                settings.AwsSecretKey == Constants.Documents.Encryption.DataCouldNotBeDecrypted)
            {
                throw new InvalidOperationException("Could not decrypt the AWS access settings, " +
                                                    "if you are running on IIS, " +
                                                    "make sure that load user profile is set to true.");
            }

            using (var client = new RavenAwsS3Client(settings.AwsAccessKey, settings.AwsSecretKey, settings.AwsRegionName ?? RavenAwsClient.DefaultRegion))
            using (var fileStream = File.OpenRead(exportPath))
            {
                var key = CombinePathAndKey(settings.RemoteFolderName, fileName);
                await client.PutObject(settings.BucketName, key, fileStream, new Dictionary<string, string>
                {
                    {"Description", GetArchiveDescription(isFullBackup)}
                }, 60 * 60);

                if (_logger.IsInfoEnabled)
                    _logger.Info(string.Format("Successfully uploaded backup {0} to S3 bucket {1}, " +
                                               "with key {2}", fileName, settings.BucketName, key));
            }
        }

        private async Task UploadToGlacier(GlacierSettings settings, string exportPath, string fileName, bool isFullExport)
        {

            if (settings.AwsAccessKey == Constants.Documents.Encryption.DataCouldNotBeDecrypted ||
                settings.AwsSecretKey == Constants.Documents.Encryption.DataCouldNotBeDecrypted)
            {
                throw new InvalidOperationException("Could not decrypt the AWS access settings, " +
                                                    "if you are running on IIS, " +
                                                    "make sure that load user profile is set to true.");
            }

            using (var client = new RavenAwsGlacierClient(settings.AwsAccessKey, settings.AwsSecretKey, settings.AwsRegionName ?? RavenAwsClient.DefaultRegion))
            using (var fileStream = File.OpenRead(exportPath))
            {
                var archiveId = await client.UploadArchive(settings.VaultName, fileStream, fileName, 60 * 60);
                if (_logger.IsInfoEnabled)
                    _logger.Info($"Successfully uploaded backup {fileName} to Glacier, archive ID: {archiveId}");
            }
        }

        private async Task UploadToAzure(AzureSettings settings, string exportPath, string fileName, bool isFullExport)
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
                using (var fileStream = File.OpenRead(exportPath))
                {
                    var key = CombinePathAndKey(settings.RemoteFolderName, fileName);
                    await client.PutBlob(key, fileStream, new Dictionary<string, string>
                    {
                        {"Description", GetArchiveDescription(isFullExport)}
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

        private string GetArchiveDescription(bool isFullBackup)
        {
            //TODO
            return $"{(isFullBackup ? "Full" : "Incremental")} periodic backup for db {_database.Name} at {SystemTime.UtcNow}";
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
                

                var isCurrentNodeTask = GetTaskStatus(databaseRecord, periodicBackup.Value, out string whoseTaskIsIt);

                var configuration = periodicBackup.Value;
                var updatedPeriodicBackup = _periodicBackups.AddOrUpdate(periodicBackup.Key, _ =>
                    {
                        var newPeriodicBackupState = new PeriodicBackupState
                        {
                            Configuration = configuration
                        };

                        if (isCurrentNodeTask == TaskStatus.ActiveByCurrentNode)
                            newPeriodicBackupState.SetupFutureBackups();

                        return newPeriodicBackupState;
                }, (_, existingBackup) =>
                    {
                        var existingFullBackupFrequency = existingBackup.Configuration.FullBackupFrequency;
                        var existingIncrementalBackupFrequency = existingBackup.Configuration.IncrementalBackupFrequency;

                        existingBackup.Configuration = configuration;

                        if (isCurrentNodeTask != TaskStatus.ActiveByCurrentNode)
                        {
                            // disable all future backups
                            existingBackup.DisableFutureBackups();
                            TryAddInactiveRunningPeriodicBackups(existingBackup.RunningTask);
                            return existingBackup;
                        }

                        existingBackup.RecreateFutureBackupsIfNeeded(
                            existingFullBackupFrequency,
                            existingIncrementalBackupFrequency,
                            () => TryAddInactiveRunningPeriodicBackups(existingBackup.RunningTask));

                        return existingBackup;
                    }
                );

                updatedPeriodicBackup.WhoseTaskIsIt = whoseTaskIsIt;
            }

            var deletedBackupTaskIds = _periodicBackups.Keys.Except(allBackupTaskIds).ToList();
            foreach (var deletedBackupId in deletedBackupTaskIds)
            {
                PeriodicBackupState deletedBackup;
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

        private TaskStatus GetTaskStatus(DatabaseRecord databaseRecord, 
            PeriodicBackupConfiguration configuration, out string whoseTaskIsIt)
        {
            if (configuration.Disabled == false)
            {
                whoseTaskIsIt = null;
                return TaskStatus.Disabled;
            }

            whoseTaskIsIt = databaseRecord.Topology.WhoseTaskIsIt(configuration);
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