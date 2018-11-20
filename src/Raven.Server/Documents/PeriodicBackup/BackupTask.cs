using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Smuggler;
using Raven.Client.ServerWide;
using Raven.Client.Util;
using Raven.Server.Config.Settings;
using Raven.Server.Documents.PeriodicBackup.Aws;
using Raven.Server.Documents.PeriodicBackup.Azure;
using Raven.Server.Documents.PeriodicBackup.Restore;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.NotificationCenter.Notifications.Details;
using Raven.Server.Rachis;
using Raven.Server.Routing;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Commands.PeriodicBackup;
using Raven.Server.ServerWide.Context;
using Raven.Server.Smuggler.Documents;
using Raven.Server.Smuggler.Documents.Data;
using Raven.Server.Utils;
using Raven.Server.Utils.Metrics;
using Sparrow;
using Sparrow.Logging;
using DatabaseSmuggler = Raven.Server.Smuggler.Documents.DatabaseSmuggler;

namespace Raven.Server.Documents.PeriodicBackup
{
    public class BackupTask
    {
        public static string DateTimeFormat => "yyyy-MM-dd-HH-mm";
        private const string InProgressExtension = ".in-progress";

        private readonly ServerStore _serverStore;
        private readonly DocumentDatabase _database;
        private readonly DateTime _startTime;
        private readonly PeriodicBackup _periodicBackup;
        private readonly PeriodicBackupConfiguration _configuration;
        private readonly PeriodicBackupStatus _previousBackupStatus;
        private readonly bool _isFullBackup;
        private readonly bool _backupToLocalFolder;
        private readonly long _operationId;
        private readonly PathSetting _tempBackupPath;
        private readonly Logger _logger;
        private readonly CancellationToken _databaseShutdownCancellationToken;

        public readonly OperationCancelToken TaskCancelToken;
        private readonly BackupResult _backupResult;

        public BackupTask(
            ServerStore serverStore, 
            DocumentDatabase database,
            PeriodicBackup periodicBackup,
            bool isFullBackup,
            bool backupToLocalFolder,
            long operationId,
            PathSetting tempBackupPath,
            Logger logger,
            CancellationToken databaseShutdownCancellationToken)
        {
            _serverStore = serverStore;
            _database = database;
            _startTime = periodicBackup.StartTime;
            _periodicBackup = periodicBackup;
            _configuration = periodicBackup.Configuration;
            _previousBackupStatus = periodicBackup.BackupStatus;
            _isFullBackup = isFullBackup;
            _backupToLocalFolder = backupToLocalFolder;
            _operationId = operationId;
            _tempBackupPath = tempBackupPath;
            _logger = logger;
            _databaseShutdownCancellationToken = databaseShutdownCancellationToken;

            TaskCancelToken = new OperationCancelToken(_databaseShutdownCancellationToken);
            _backupResult = GenerateBackupResult();
        }

        public async Task<IOperationResult> RunPeriodicBackup(Action<IOperationProgress> onProgress)
        {
            AddInfo($"Started task: '{_configuration.Name}'", onProgress);

            var totalSw = Stopwatch.StartNew();
            var operationCanceled = false;

            var runningBackupStatus = _periodicBackup.RunningBackupStatus = new PeriodicBackupStatus
            {
                TaskId = _configuration.TaskId,
                BackupType = _configuration.BackupType,
                LastEtag = _previousBackupStatus.LastEtag,
                LastRaftIndex = _previousBackupStatus.LastRaftIndex,
                LastFullBackup = _previousBackupStatus.LastFullBackup,
                LastIncrementalBackup = _previousBackupStatus.LastIncrementalBackup,
                LastFullBackupInternal = _previousBackupStatus.LastFullBackupInternal,
                LastIncrementalBackupInternal = _previousBackupStatus.LastIncrementalBackupInternal,
                IsFull = _isFullBackup,
                LocalBackup = _previousBackupStatus.LocalBackup,
                LastOperationId = _previousBackupStatus.LastOperationId,
                FolderName = _previousBackupStatus.FolderName
            };

            try
            {
                var now = DateTime.Now.ToString(DateTimeFormat, CultureInfo.InvariantCulture);

                if (runningBackupStatus.LocalBackup == null)
                    runningBackupStatus.LocalBackup = new LocalBackup();

                if (runningBackupStatus.LastRaftIndex == null)
                    runningBackupStatus.LastRaftIndex = new LastRaftIndex();

                if (_logger.IsInfoEnabled)
                {
                    var fullBackupText = "a " + (_configuration.BackupType == BackupType.Backup ? "full backup" : "snapshot");
                    _logger.Info($"Creating {(_isFullBackup ? fullBackupText : "an incremental backup")}");
                }
                var currentLastRaftIndex = GetDatabaseRecord().EtagForBackup;

                if (_isFullBackup == false)
                {
                    // no-op if nothing has changed
                    var currentLastEtag = _database.ReadLastEtag();
                    if ((currentLastEtag == _previousBackupStatus.LastEtag) && (currentLastRaftIndex == _previousBackupStatus.LastRaftIndex.LastEtag))
                    {
                        var message = "Skipping incremental backup because " +
                                      $"last etag ({currentLastEtag:#,#;;0}) hasn't changed since last backup";

                        if (_logger.IsInfoEnabled)
                            _logger.Info(message);

                        UpdateOperationId(runningBackupStatus);
                        runningBackupStatus.LastIncrementalBackup = _startTime;
                        DatabaseSmuggler.EnsureProcessed(_backupResult);
                        AddInfo(message, onProgress);

                        return _backupResult;
                    }
                }

                GenerateFolderNameAndBackupDirectory(now, out var folderName, out var backupDirectory);
                var startDocumentEtag = _isFullBackup == false ? _previousBackupStatus.LastEtag : null;
                var fileName = GetFileName(_isFullBackup, backupDirectory.FullPath, now, _configuration.BackupType, out string backupFilePath);
                var lastEtag = CreateLocalBackupOrSnapshot(runningBackupStatus, backupFilePath, startDocumentEtag, onProgress);

                runningBackupStatus.LocalBackup.BackupDirectory = _backupToLocalFolder ? backupDirectory.FullPath : null;
                runningBackupStatus.LocalBackup.TempFolderUsed = _backupToLocalFolder == false;
                runningBackupStatus.IsFull = _isFullBackup;

                try
                {
                    await UploadToServer(backupFilePath, folderName, fileName, onProgress);
                }
                finally
                {
                    runningBackupStatus.UploadToS3 = _backupResult.S3Backup;
                    runningBackupStatus.UploadToAzure = _backupResult.AzureBackup;
                    runningBackupStatus.UploadToGlacier = _backupResult.GlacierBackup;
                    runningBackupStatus.UploadToFtp = _backupResult.FtpBackup;

                    // if user did not specify local folder we delete the temporary file
                    if (_backupToLocalFolder == false)
                    {
                        IOExtensions.DeleteFile(backupFilePath);
                    }
                }

                UpdateOperationId(runningBackupStatus);
                runningBackupStatus.LastEtag = lastEtag;
                runningBackupStatus.LastRaftIndex.LastEtag = currentLastRaftIndex;
                runningBackupStatus.FolderName = folderName;
                if (_isFullBackup)
                    runningBackupStatus.LastFullBackup = _periodicBackup.StartTime;
                else
                    runningBackupStatus.LastIncrementalBackup = _periodicBackup.StartTime;

                totalSw.Stop();

                if (_logger.IsInfoEnabled)
                {
                    var fullBackupText = "a " + (_configuration.BackupType == BackupType.Backup ? " full backup" : " snapshot");
                    _logger.Info($"Successfully created {(_isFullBackup ? fullBackupText : "an incremental backup")} " +
                                 $"in {totalSw.ElapsedMilliseconds:#,#;;0} ms");
                }

                return _backupResult;
            }
            catch (OperationCanceledException)
            {
                operationCanceled = TaskCancelToken.Token.IsCancellationRequested && 
                                    _databaseShutdownCancellationToken.IsCancellationRequested;
                throw;
            }
            catch (ObjectDisposedException)
            {
                // shutting down, probably
                operationCanceled = true;
                throw;
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
                    $"Periodic Backup task: '{_periodicBackup.Configuration.Name}'",
                    message,
                    AlertType.PeriodicBackup,
                    NotificationSeverity.Error,
                    details: new ExceptionDetails(e)));

                throw;
            }
            finally
            {
                if (operationCanceled == false)
                {
                    // whether we succeeded or not,
                    // we need to update the last backup time to avoid
                    // starting a new backup right after this one
                    if (_isFullBackup)
                        runningBackupStatus.LastFullBackupInternal = _startTime;
                    else
                        runningBackupStatus.LastIncrementalBackupInternal = _startTime;

                    runningBackupStatus.NodeTag = _serverStore.NodeTag;
                    runningBackupStatus.DurationInMs = totalSw.ElapsedMilliseconds;
                    runningBackupStatus.Version = ++_previousBackupStatus.Version;

                    _periodicBackup.BackupStatus = runningBackupStatus;

                    // save the backup status
                    await WriteStatus(runningBackupStatus, onProgress);
                }
            }
        }

        private DatabaseRecord GetDatabaseRecord()
        {
            using (_serverStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (context.OpenReadTransaction())
            {
                return _serverStore.Cluster.ReadDatabase(context, _database.Name);
            }
        }

        private void GenerateFolderNameAndBackupDirectory(string now, out string folderName, out PathSetting backupDirectory)
        {
            if (_isFullBackup)
            {
                var counter = 0;
                do
                {
                    var prefix = counter++ == 0 ? string.Empty : $"-{counter++:D2}";
                    folderName = $"{now}{prefix}.ravendb-{_database.Name}-{_serverStore.NodeTag}-{_configuration.BackupType.ToString().ToLower()}";
                    backupDirectory = _backupToLocalFolder ? new PathSetting(_configuration.LocalSettings.FolderPath).Combine(folderName) : _tempBackupPath;
                } while (_backupToLocalFolder && DirectoryContainsBackupFiles(backupDirectory.FullPath, IsAnyBackupFile));

                if (Directory.Exists(backupDirectory.FullPath) == false)
                    Directory.CreateDirectory(backupDirectory.FullPath);
            }
            else
            {
                Debug.Assert(_previousBackupStatus.FolderName != null);

                folderName = _previousBackupStatus.FolderName;
                backupDirectory = _backupToLocalFolder ? new PathSetting(_previousBackupStatus.LocalBackup.BackupDirectory) : _tempBackupPath;
            }
        }

        private BackupResult GenerateBackupResult()
        {
            return new BackupResult
            {
                SnapshotBackup =
                {
                    Skipped = _isFullBackup == false || _configuration.BackupType == BackupType.Backup
                },
                S3Backup =
                {
                    // will be set before the actual upload if needed
                    Skipped = true
                },
                AzureBackup =
                {
                    Skipped = true
                },
                GlacierBackup =
                {
                    Skipped = true
                },
                FtpBackup =
                {
                    Skipped = true
                }
            };
        }

        public static bool DirectoryContainsBackupFiles(string fullPath, Func<string, bool> isBackupFile)
        {
            if (Directory.Exists(fullPath) == false)
                return false;

            var files = Directory.GetFiles(fullPath);
            if (files.Length == 0)
                return false;

            return files.Any(isBackupFile);
        }

        private static bool IsAnyBackupFile(string filePath)
        {
            if (RestoreUtils.IsBackupOrSnapshot(filePath))
                return true;

            var extension = Path.GetExtension(filePath);
            return InProgressExtension.Equals(extension, StringComparison.OrdinalIgnoreCase);
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

        private long CreateLocalBackupOrSnapshot(
            PeriodicBackupStatus status, string backupFilePath,
            long? startDocumentEtag, Action<IOperationProgress> onProgress)
        {
            long lastEtag;
            using (status.LocalBackup.UpdateStats(_isFullBackup))
            {
                try
                {
                    // will rename the file after the backup is finished
                    var tempBackupFilePath = backupFilePath + InProgressExtension;

                    if (_configuration.BackupType == BackupType.Backup ||
                        _configuration.BackupType == BackupType.Snapshot && _isFullBackup == false)
                    {
                        var backupType = _configuration.BackupType == BackupType.Snapshot ? "snapshot " : string.Empty;
                        AddInfo($"Started an incremental {backupType}backup", onProgress);

                        // smuggler backup
                        var options = new DatabaseSmugglerOptionsServerSide
                        {
                            AuthorizationStatus = AuthorizationStatus.DatabaseAdmin,
                        };
                        if (_isFullBackup == false)
                            options.OperateOnTypes |= DatabaseItemType.Tombstones;

                        var lastEtagFromStorage = CreateBackup(options, tempBackupFilePath, startDocumentEtag, onProgress);
                        if (_isFullBackup)
                            lastEtag = lastEtagFromStorage;
                        else if (_database.ReadLastEtag() == _previousBackupStatus.LastEtag)
                            lastEtag = startDocumentEtag ?? 0;
                        else
                            lastEtag = _backupResult.GetLastEtag();
                    }
                    else
                    {
                        // snapshot backup
                        AddInfo("Started a snapshot backup", onProgress);

                        lastEtag = _database.ReadLastEtag();
                        var databaseSummary = _database.GetDatabaseSummary();
                        var indexesCount = _database.IndexStore.Count;

                        var totalSw = Stopwatch.StartNew();
                        var sw = Stopwatch.StartNew();
                        var smugglerResult = _database.FullBackupTo(tempBackupFilePath, 
                            info =>
                            {
                                AddInfo(info.Message, onProgress);

                                _backupResult.SnapshotBackup.ReadCount += info.FilesCount;
                                if (sw.ElapsedMilliseconds > 0 && info.FilesCount > 0)
                                {
                                    AddInfo($"Backed up {_backupResult.SnapshotBackup.ReadCount} " +
                                            $"file{(_backupResult.SnapshotBackup.ReadCount > 1 ? "s" : string.Empty)}", onProgress);
                                    sw.Restart();
                                }
                            }, TaskCancelToken.Token);

                        EnsureSnapshotProcessed(databaseSummary, smugglerResult, indexesCount);
                        
                        AddInfo($"Backed up {_backupResult.SnapshotBackup.ReadCount} files, " +
                                $"took: {totalSw.ElapsedMilliseconds:#,#;;0}ms", onProgress);
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

        private void EnsureSnapshotProcessed(DatabaseSummary databaseSummary, SmugglerResult snapshotSmugglerResult, long indexesCount)
        {
            _backupResult.SnapshotBackup.Processed = true;
            _backupResult.DatabaseRecord.Processed = true;
            _backupResult.RevisionDocuments.Attachments.Processed = true;
            _backupResult.Tombstones.Processed = true;
            _backupResult.Indexes.Processed = true;
            _backupResult.Indexes.ReadCount = indexesCount;

            _backupResult.Documents.Processed = true;
            _backupResult.Documents.ReadCount = databaseSummary.DocumentsCount;
            _backupResult.Documents.Attachments.Processed = true;
            _backupResult.Documents.Attachments.ReadCount = databaseSummary.AttachmentsCount;
            _backupResult.Counters.Processed = true;
            _backupResult.Counters.ReadCount = databaseSummary.CountersCount;
            _backupResult.RevisionDocuments.Processed = true;
            _backupResult.RevisionDocuments.ReadCount = databaseSummary.RevisionsCount;
            _backupResult.Conflicts.Processed = true;
            _backupResult.Conflicts.ReadCount = databaseSummary.ConflictsCount;

            _backupResult.Identities.Processed = true;
            _backupResult.Identities.ReadCount = snapshotSmugglerResult.Identities.ReadCount;
            _backupResult.CompareExchange.Processed = true;
            _backupResult.CompareExchange.ReadCount = snapshotSmugglerResult.CompareExchange.ReadCount;
        }

        private void AddInfo(string message, Action<IOperationProgress> onProgress)
        {
            _backupResult.AddInfo(message);
            onProgress.Invoke(_backupResult.Progress);
        }
        
        private long CreateBackup(
            DatabaseSmugglerOptionsServerSide options, string backupFilePath,
            long? startDocumentEtag, Action<IOperationProgress> onProgress)
        {
            // the last etag is already included in the last backup
            startDocumentEtag = startDocumentEtag == null ? 0 : ++startDocumentEtag;

            using (var file = File.Open(backupFilePath, FileMode.CreateNew))
            using (_database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            {
                var smugglerSource = new DatabaseSource(_database, startDocumentEtag.Value);
                var smugglerDestination = new StreamDestination(file, context, smugglerSource);
                var smuggler = new DatabaseSmuggler(_database,
                    smugglerSource,
                    smugglerDestination,
                    _database.Time,
                    options: options,
                    result: _backupResult,
                    onProgress: onProgress,
                    token: TaskCancelToken.Token);

                smuggler.Execute();
                file.Flush(flushToDisk: true);

                return smugglerSource.LastEtag;
            }
        }

        private async Task UploadToServer(string backupPath, string folderName, string fileName, Action<IOperationProgress> onProgress)
        {
            TaskCancelToken.Token.ThrowIfCancellationRequested();

            var tasks = new List<Task>();

            CreateUploadTaskIfNeeded(_configuration.S3Settings, tasks, backupPath, _isFullBackup,
                async (settings, stream, progress) =>
                {
                    var archiveDescription = GetArchiveDescription(_isFullBackup, _configuration.BackupType);
                    await UploadToS3(settings, stream, folderName, fileName, progress, archiveDescription);
                },
                _backupResult.S3Backup, onProgress);

            CreateUploadTaskIfNeeded(_configuration.GlacierSettings, tasks, backupPath, _isFullBackup,
                async (settings, stream, progress) =>
                    await UploadToGlacier(settings, stream, folderName, fileName, progress),
                _backupResult.GlacierBackup, onProgress);

            CreateUploadTaskIfNeeded(_configuration.AzureSettings, tasks, backupPath, _isFullBackup,
                async (settings, stream, progress) =>
                {
                    var archiveDescription = GetArchiveDescription(_isFullBackup, _configuration.BackupType);
                    await UploadToAzure(settings, stream, folderName, fileName, progress, archiveDescription);
                },
                _backupResult.AzureBackup, onProgress);

            CreateUploadTaskIfNeeded(_configuration.FtpSettings, tasks, backupPath, _isFullBackup,
                async (settings, stream, progress) =>
                    await UploadToFtp(settings, stream, folderName, fileName, progress),
                _backupResult.FtpBackup, onProgress);

            await Task.WhenAll(tasks);
        }

        private void CreateUploadTaskIfNeeded<S, T>(
            S settings,
            List<Task> tasks,
            string backupPath,
            bool isFullBackup,
            Func<S, FileStream, Progress, Task> uploadToServer,
            T uploadStatus,
            Action<IOperationProgress> onProgress)
            where S : BackupSettings
            where T : CloudUploadStatus
        {
            if (PeriodicBackupConfiguration.CanBackupUsing(settings) == false)
                return;

            Debug.Assert(uploadStatus != null);

            var localUploadStatus = uploadStatus;

            tasks.Add(Task.Run(async () =>
            {
                using (localUploadStatus.UpdateStats(isFullBackup))
                using (var fileStream = File.OpenRead(backupPath))
                {
                    var uploadProgress = localUploadStatus.UploadProgress;
                    localUploadStatus.Skipped = false;
                    uploadProgress.ChangeState(UploadState.PendingUpload);
                    uploadProgress.SetTotal(fileStream.Length);

                    AddInfo($"Starting {uploadStatus.GetType().AssemblyQualifiedName}", onProgress);

                    try
                    {
                        var bytesPutsPerSec = new MeterMetric();

                        long lastUploadedInBytes = 0;
                        var totalToUpload = new Sparrow.Size(uploadProgress.TotalInBytes, SizeUnit.Bytes).ToString();
                        var sw = Stopwatch.StartNew();
                        var progress = new Progress(uploadProgress)
                        {
                            OnUploadProgress = () =>
                            {
                                if (sw.ElapsedMilliseconds <= 1000)
                                    return;

                                var totalUploadedInBytes = uploadProgress.UploadedInBytes;
                                bytesPutsPerSec.MarkSingleThreaded(totalUploadedInBytes - lastUploadedInBytes);
                                lastUploadedInBytes = totalUploadedInBytes;
                                var uploaded = new Sparrow.Size(totalUploadedInBytes, SizeUnit.Bytes);
                                uploadProgress.BytesPutsPerSec = bytesPutsPerSec.MeanRate;
                                AddInfo($"Uploaded: {uploaded} / {totalToUpload}", onProgress);
                                sw.Restart();
                            }
                        };

                        await uploadToServer(settings, fileStream, progress);

                        AddInfo($"Total uploaded: {totalToUpload}, " +
                                $"took: {MsToHumanReadableString(uploadProgress.UploadTimeInMs)}", onProgress);
                    }
                    catch (OperationCanceledException e)
                    {
                        // shutting down
                        localUploadStatus.Exception = e.ToString();
                        throw;
                    }
                    catch (Exception e)
                    {
                        localUploadStatus.Exception = e.ToString();
                        throw new InvalidOperationException($"Failed to backup to {uploadStatus.GetType().FullName}", e);
                    }
                    finally
                    {
                        uploadProgress.ChangeState(UploadState.Done);
                    }
                }
            }));
        }

        private static string MsToHumanReadableString(long milliseconds)
        {
            var durationsList = new List<string>();
            var timeSpan = TimeSpan.FromMilliseconds(milliseconds);
            var totalDays = (int)timeSpan.TotalDays;
            if (totalDays >= 1)
            {
                durationsList.Add($"{totalDays:#,#;;0} day{Pluralize(totalDays)}");
                timeSpan = timeSpan.Add(TimeSpan.FromDays(-1 * totalDays));
            }

            var totalHours = (int)timeSpan.TotalHours;
            if (totalHours >= 1)
            {
                durationsList.Add($"{totalHours:#,#;;0} hour{Pluralize(totalHours)}");
                timeSpan = timeSpan.Add(TimeSpan.FromHours(-1 * totalHours));
            }

            var totalMinutes = (int)timeSpan.TotalMinutes;
            if (totalMinutes >= 1)
            {
                durationsList.Add($"{totalMinutes:#,#;;0} minute{Pluralize(totalMinutes)}");
                timeSpan = timeSpan.Add(TimeSpan.FromMinutes(-1 * totalMinutes));
            }

            var totalSeconds = (int)timeSpan.TotalSeconds;
            if (totalSeconds >= 1)
            {
                durationsList.Add($"{totalSeconds:#,#;;0} second{Pluralize(totalSeconds)}");
                timeSpan = timeSpan.Add(TimeSpan.FromSeconds(-1 * totalSeconds));
            }

            var totalMilliseconds = (int)timeSpan.TotalMilliseconds;
            if (totalMilliseconds > 0)
            {
                durationsList.Add($"{totalMilliseconds:#,#;;0} ms");
            }

            return string.Join(' ',durationsList.Take(2));
        }

        private static string Pluralize(int number)
        {
            return number > 1 ? "s" : string.Empty;
        }

        private async Task UploadToS3(
            S3Settings settings,
            Stream stream,
            string folderName,
            string fileName,
            Progress progress,
            string archiveDescription)
        {
            using (var client = new RavenAwsS3Client(settings.AwsAccessKey, settings.AwsSecretKey,
                settings.AwsRegionName, settings.BucketName, progress, TaskCancelToken.Token))
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
            Progress progress)
        {
            using (var client = new RavenAwsGlacierClient(settings.AwsAccessKey, settings.AwsSecretKey,
                settings.AwsRegionName, settings.VaultName, progress, TaskCancelToken.Token))
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
            Progress progress)
        {
            using (var client = new RavenFtpClient(settings.Url, settings.Port, settings.UserName,
                settings.Password, settings.CertificateAsBase64, settings.CertificateFileName, progress, TaskCancelToken.Token))
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
            Progress progress,
            string archiveDescription)
        {
            using (var client = new RavenAzureClient(settings.AccountName, settings.AccountKey,
                settings.StorageContainer, progress, TaskCancelToken.Token))
            {
                var key = CombinePathAndKey(settings.RemoteFolderName, folderName, fileName);
                await client.PutBlob(key, stream, new Dictionary<string, string>
                {
                    {"Description", archiveDescription}
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

        private void UpdateOperationId(PeriodicBackupStatus runningBackupStatus)
        {
            runningBackupStatus.LastOperationId = _operationId;
            if (_previousBackupStatus.LastOperationId == null ||
                _previousBackupStatus.NodeTag != _serverStore.NodeTag)
                return;

            // dismiss the previous operation
            var id = $"{NotificationType.OperationChanged}/{_previousBackupStatus.LastOperationId.Value}";
            _database.NotificationCenter.Dismiss(id);
        }

        private async Task WriteStatus(PeriodicBackupStatus status, Action<IOperationProgress> onProgress)
        {
            AddInfo("Saving backup status", onProgress);

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
                    $"Periodic Backup task: '{_periodicBackup.Configuration.Name}'",
                    message,
                    AlertType.PeriodicBackup,
                    NotificationSeverity.Error,
                    details: new ExceptionDetails(e)));
            }
        }
    }
}
