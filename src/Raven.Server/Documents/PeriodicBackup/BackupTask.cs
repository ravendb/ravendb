using System;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Threading;
using Raven.Client;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Smuggler;
using Raven.Client.Extensions;
using Raven.Client.ServerWide.Operations.Configuration;
using Raven.Server.Config.Settings;
using Raven.Server.Documents.PeriodicBackup.Restore;
using Raven.Server.Documents.PeriodicBackup.Retention;
using Raven.Server.Json;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Server.NotificationCenter.Notifications.Details;
using Raven.Server.Routing;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Commands;
using Raven.Server.ServerWide.Context;
using Raven.Server.Smuggler.Documents;
using Raven.Server.Smuggler.Documents.Data;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Logging;
using Sparrow.Server.Json.Sync;
using Sparrow.Server.Logging;
using Sparrow.Utils;
using BackupUtils = Raven.Server.Utils.BackupUtils;
using StorageEnvironmentType = Voron.StorageEnvironmentWithType.StorageEnvironmentType;

namespace Raven.Server.Documents.PeriodicBackup
{
    public class BackupTask
    {
        public static string DateTimeFormat = "yyyy-MM-dd-HH-mm-ss";
        private const string LegacyDateTimeFormat = "yyyy-MM-dd-HH-mm";
        private const string InProgressExtension = ".in-progress";

        protected readonly DocumentDatabase Database;
        protected readonly BackupConfiguration Configuration;
        protected readonly BackupResult BackupResult;
        protected readonly RetentionPolicyBaseParameters RetentionPolicyParameters;

        private readonly PeriodicBackupStatus _previousBackupStatus;
        internal readonly bool _isFullBackup;
        private readonly bool _isOneTimeBackup;
        private readonly bool _backupToLocalFolder;
        private readonly long _operationId;
        private readonly PathSetting _tempBackupPath;
        private readonly RavenLogger _logger;
        public readonly OperationCancelToken TaskCancelToken;
        private readonly bool _isServerWide;
        private readonly bool _isBackupEncrypted;
        private Action<IOperationProgress> _onProgress;
        private readonly string _taskName;
        internal PeriodicBackupRunner.TestingStuff _forTestingPurposes;
        private readonly DateTime _startTimeUtc;
        protected Action OnBackupException;

        public BackupTask(DocumentDatabase database, BackupParameters backupParameters, BackupConfiguration configuration, OperationCancelToken token, RavenLogger logger, PeriodicBackupRunner.TestingStuff forTestingPurposes = null)
        {
            Database = database;
            _taskName = backupParameters.Name;
            _operationId = backupParameters.OperationId;
            _previousBackupStatus = backupParameters.BackupStatus;
            _startTimeUtc = backupParameters.StartTimeUtc;
            _isOneTimeBackup = backupParameters.IsOneTimeBackup;
            _isFullBackup = backupParameters.IsFullBackup;
            _backupToLocalFolder = backupParameters.BackupToLocalFolder;
            _tempBackupPath = backupParameters.TempBackupPath;
            Configuration = configuration;
            _logger = logger;
            _isServerWide = backupParameters.Name?.StartsWith(ServerWideBackupConfiguration.NamePrefix, StringComparison.OrdinalIgnoreCase) ?? false;
            _isBackupEncrypted = IsBackupEncrypted(Database, Configuration);
            _forTestingPurposes = forTestingPurposes;
            BackupResult = GenerateBackupResult();

            TaskCancelToken = token ?? new OperationCancelToken(Database.DatabaseShutdown, CancellationToken.None);

            RetentionPolicyParameters = new RetentionPolicyBaseParameters
            {
                RetentionPolicy = backupParameters.RetentionPolicy,
                DatabaseName = Database.Name,
                IsFullBackup = _isFullBackup,
                OnProgress = AddInfo,
                CancellationToken = TaskCancelToken.Token
            };
        }

        public BackupResult RunPeriodicBackup(Action<IOperationProgress> onProgress, ref PeriodicBackupStatus runningBackupStatus)
        {
            _onProgress = onProgress;
            AddInfo($"Started task: '{_taskName}'");

            var totalSw = Stopwatch.StartNew();
            var operationCanceled = false;

            try
            {
                if (_forTestingPurposes != null && _forTestingPurposes.SimulateFailedBackup)
                    throw new Exception(nameof(_forTestingPurposes.SimulateFailedBackup));
                if (_forTestingPurposes != null && _forTestingPurposes.OnBackupTaskRunHoldBackupExecution != null)
                    _forTestingPurposes.OnBackupTaskRunHoldBackupExecution?.Task.Wait();
                if (Database.ForTestingPurposes != null && Database.ForTestingPurposes.ActionToCallOnGetTempPath != null)
                    Database.ForTestingPurposes.ActionToCallOnGetTempPath?.Invoke(_tempBackupPath);

                if (runningBackupStatus.LocalBackup == null)
                    runningBackupStatus.LocalBackup = new LocalBackup();

                if (runningBackupStatus.LastRaftIndex == null)
                    runningBackupStatus.LastRaftIndex = new LastRaftIndex();

                runningBackupStatus.IsFull = _isFullBackup;

                if (_logger.IsInfoEnabled)
                {
                    var fullBackupText = "a " + (Configuration.BackupType == BackupType.Backup ? "full backup" : "snapshot");
                    _logger.Info($"Creating {(_isFullBackup ? fullBackupText : "an incremental backup")}");
                }

                if (_isFullBackup == false)
                {
                    // if we come from old version the _previousBackupStatus won't have LastRaftIndex
                    _previousBackupStatus.LastRaftIndex ??= new LastRaftIndex();

                    // no-op if nothing has changed
                    var (currentLastEtag, currentChangeVector) = Database.ReadLastEtagAndChangeVector();
                    var currentLastRaftIndex = GetDatabaseEtagForBackup();

                    // if we come from old version the _previousBackupStatus won't have LastRaftIndex
                    _previousBackupStatus.LastRaftIndex ??= new LastRaftIndex();

                    if (currentLastEtag == _previousBackupStatus.LastEtag
                        && currentChangeVector == _previousBackupStatus.LastDatabaseChangeVector
                        && currentLastRaftIndex == _previousBackupStatus.LastRaftIndex.LastEtag)
                    {
                        var message = $"Skipping incremental backup because no changes were made from last full backup on {_previousBackupStatus.LastFullBackup}.";

                        if (_logger.IsInfoEnabled)
                            _logger.Info(message);

                        runningBackupStatus.LastIncrementalBackup = _startTimeUtc;
                        runningBackupStatus.LocalBackup.LastIncrementalBackup = _startTimeUtc;
                        runningBackupStatus.LocalBackup.IncrementalBackupDurationInMs = 0;
                        SmugglerBase.EnsureProcessed(BackupResult);
                        AddInfo(message);

                        return BackupResult;
                    }
                }

                // update the local configuration before starting the local backup
                var localSettings = GetBackupConfigurationFromScript(Configuration.LocalSettings, x => JsonDeserializationServer.LocalSettings(x),
                    settings => PutServerWideBackupConfigurationCommand.UpdateSettingsForLocal(settings, Database.Name));

                GenerateFolderNameAndBackupDirectory(localSettings, _startTimeUtc, out var nowAsString, out var folderName, out var backupDirectory);
                var startEtag = _isFullBackup == false ? _previousBackupStatus.LastEtag : null;
                var startRaftIndex = _isFullBackup == false ? _previousBackupStatus.LastRaftIndex.LastEtag : null;

                var fileName = GetFileName(_isFullBackup, backupDirectory.FullPath, nowAsString, Configuration.BackupType, out string backupFilePath);
                var internalBackupResult = CreateLocalBackupOrSnapshot(runningBackupStatus, backupFilePath, folderName, fileName, startEtag, startRaftIndex);

                runningBackupStatus.LocalBackup.BackupDirectory = _backupToLocalFolder ? backupDirectory.FullPath : null;
                runningBackupStatus.LocalBackup.TempFolderUsed = _backupToLocalFolder == false;
                runningBackupStatus.IsEncrypted = _isBackupEncrypted;

                try
                {
                    UploadToServer(backupFilePath, folderName, fileName);
                }
                finally
                {
                    runningBackupStatus.UploadToS3 = BackupResult.S3Backup;
                    runningBackupStatus.UploadToAzure = BackupResult.AzureBackup;
                    runningBackupStatus.UploadToGoogleCloud = BackupResult.GoogleCloudBackup;
                    runningBackupStatus.UploadToGlacier = BackupResult.GlacierBackup;
                    runningBackupStatus.UploadToFtp = BackupResult.FtpBackup;

                    BackupResult.LocalBackup = new LocalBackup
                    {
                        BackupDirectory = folderName,
                        FileName = fileName
                    };

                    if (_backupToLocalFolder == false)
                    {
                        // if user did not specify a local folder, we delete the temporary file
                        DeleteFile(backupFilePath);
                    }
                }

                runningBackupStatus.LastEtag = internalBackupResult.LastEtag;
                runningBackupStatus.LastDatabaseChangeVector = internalBackupResult.LastDatabaseChangeVector;
                runningBackupStatus.LastRaftIndex.LastEtag = internalBackupResult.LastRaftIndex;
                runningBackupStatus.FolderName = folderName;

                if (_isFullBackup)
                    runningBackupStatus.LastFullBackup = _startTimeUtc;
                else
                    runningBackupStatus.LastIncrementalBackup = _startTimeUtc;

                totalSw.Stop();

                if (_logger.IsInfoEnabled)
                {
                    var fullBackupText = "a " + (Configuration.BackupType == BackupType.Backup ? " full backup" : " snapshot");
                    _logger.Info($"Successfully created {(_isFullBackup ? fullBackupText : "an incremental backup")} " +
                                 $"in {totalSw.ElapsedMilliseconds:#,#;;0} ms");
                }

                return BackupResult;
            }
            catch (ObjectDisposedException)
            {
                // shutting down, probably
                operationCanceled = true;
                throw;
            }
            catch (Exception e) when (e.ExtractSingleInnerException() is OperationCanceledException oce)
            {
                operationCanceled = TaskCancelToken.Token.IsCancellationRequested;

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

                if (_logger.IsErrorEnabled)
                    _logger.Error(message, e);

                Database.NotificationCenter.Add(AlertRaised.Create(
                    Database.Name,
                    $"Periodic Backup task: '{_taskName}'",
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
                    // in periodic backup we need to update the last backup time to avoid
                    // starting a new backup right after this one
                    if (_isFullBackup)
                        runningBackupStatus.LastFullBackupInternal = _startTimeUtc;
                    else
                        runningBackupStatus.LastIncrementalBackupInternal = _startTimeUtc;

                    runningBackupStatus.NodeTag = Database.ServerStore.NodeTag;
                    runningBackupStatus.DurationInMs = totalSw.ElapsedMilliseconds;
                    UpdateOperationId(runningBackupStatus);

                    if (_isOneTimeBackup == false)
                    {
                        runningBackupStatus.Version = ++_previousBackupStatus.Version;
                        // save the backup status
                        // create a local copy of ref `runningBackupStatus` so that it can be used in the anonymous method.
                        var status = runningBackupStatus;
                        BackupUtils.SaveBackupStatus(status, Database.Name, Database.ServerStore, _logger, BackupResult, _onProgress, TaskCancelToken);
                    }

                    _forTestingPurposes?.AfterBackupBatchCompleted?.Invoke();
                }
            }
        }

        protected T GetBackupConfigurationFromScript<T>(T backupSettings, Func<BlittableJsonReaderObject, T> deserializeSettingsFunc,
            Action<T> updateServerWideSettingsFunc)
            where T : BackupSettings
        {
            return GetBackupConfigurationFromScript(backupSettings, deserializeSettingsFunc, Database, updateServerWideSettingsFunc, _isServerWide);
        }

        internal static T GetBackupConfigurationFromScript<T>(T backupSettings, Func<BlittableJsonReaderObject, T> deserializeSettingsFunc, DocumentDatabase documentDatabase,
            Action<T> updateServerWideSettingsFunc, bool serverWide)
            where T : BackupSettings
        {
            if (backupSettings == null)
                return null;

            if (backupSettings.GetBackupConfigurationScript == null || backupSettings.Disabled)
                return backupSettings;

            if (string.IsNullOrEmpty(backupSettings.GetBackupConfigurationScript.Exec))
                return backupSettings;

            var command = backupSettings.GetBackupConfigurationScript.Exec;
            var arguments = backupSettings.GetBackupConfigurationScript.Arguments;

            var startInfo = new ProcessStartInfo
            {
                FileName = command,
                Arguments = arguments,
                UseShellExecute = false,
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                CreateNoWindow = true
            };

            Process process;

            try
            {
                process = Process.Start(startInfo);
            }
            catch (Exception e)
            {
                throw new InvalidOperationException($"Unable to get backup configuration by executing {command} {arguments}. Failed to start process.", e);
            }

            using (var ms = new MemoryStream())
            {
                var readErrors = process.StandardError.ReadToEndAsync();
                var readStdOut = process.StandardOutput.BaseStream.CopyToAsync(ms);
                var timeoutInMs = backupSettings.GetBackupConfigurationScript.TimeoutInMs;

                string GetStdError()
                {
                    try
                    {
                        return readErrors.Result;
                    }
                    catch
                    {
                        return "Unable to get stdout";
                    }
                }

                try
                {
                    readStdOut.Wait(timeoutInMs);
                    readErrors.Wait(timeoutInMs);
                }
                catch (Exception e)
                {
                    throw new InvalidOperationException($"Unable to get backup configuration by executing {command} {arguments}, waited for {timeoutInMs}ms but the process didn't exit. Stderr: {GetStdError()}", e);
                }

                if (process.WaitForExit(timeoutInMs) == false)
                {
                    process.Kill();

                    throw new InvalidOperationException($"Unable to get backup configuration by executing {command} {arguments}, waited for {timeoutInMs}ms but the process didn't exit. Stderr: {GetStdError()}");
                }

                if (process.ExitCode != 0)
                {
                    throw new InvalidOperationException($"Unable to get backup configuration by executing {command} {arguments}, the exit code was {process.ExitCode}. Stderr: {GetStdError()}");
                }

                using (documentDatabase.DocumentsStorage.ContextPool.AllocateOperationContext(out JsonOperationContext context))
                {
                    ms.Position = 0;
                    var configuration = context.Sync.ReadForMemory(ms, "backup-configuration-from-script");
                    var result = deserializeSettingsFunc(configuration);
                    if (serverWide)
                        updateServerWideSettingsFunc?.Invoke(result);

                    return result;
                }
            }
        }

        public static bool IsBackupEncrypted(DocumentDatabase database, BackupConfiguration configuration)
        {
            if (database.MasterKey != null && configuration.BackupEncryptionSettings == null)
                return true;

            return configuration.BackupEncryptionSettings != null &&
                   configuration.BackupEncryptionSettings.EncryptionMode != EncryptionMode.None;
        }

        private long GetDatabaseEtagForBackup()
        {
            using (Database.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (context.OpenReadTransaction())
            {
                var rawRecord = Database.ServerStore.Cluster.ReadRawDatabaseRecord(context, Database.Name);

                return rawRecord.EtagForBackup;
            }
        }

        private void GenerateFolderNameAndBackupDirectory(LocalSettings localSettings, DateTime startTimeInUtc, out string nowAsString, out string folderName, out PathSetting backupDirectory)
        {
            var nowInLocalTime = startTimeInUtc.ToLocalTime();

            if (_isFullBackup)
            {
                while (true)
                {
                    nowAsString = GetFormattedDate(nowInLocalTime);
                    folderName = $"{nowAsString}.ravendb-{Database.Name}-{Database.ServerStore.NodeTag}-{Configuration.BackupType.ToString().ToLower()}";
                    backupDirectory = _backupToLocalFolder ? new PathSetting(localSettings.FolderPath).Combine(folderName) : _tempBackupPath;

                    if (_backupToLocalFolder == false || DirectoryContainsBackupFiles(backupDirectory.FullPath, IsAnyBackupFile) == false)
                        break;

                    // the backup folder contains backup files
                    nowInLocalTime = DateTime.Now;
                }

                if (Directory.Exists(backupDirectory.FullPath) == false)
                    Directory.CreateDirectory(backupDirectory.FullPath);
            }
            else
            {
                Debug.Assert(_previousBackupStatus.FolderName != null);

                folderName = _previousBackupStatus.FolderName;
                backupDirectory = _backupToLocalFolder ? new PathSetting(_previousBackupStatus.LocalBackup.BackupDirectory) : _tempBackupPath;
                nowAsString = GetFormattedDate(nowInLocalTime);
            }
        }

        private static string GetFormattedDate(DateTime dateTime)
        {
            return dateTime.ToString(DateTimeFormat, CultureInfo.InvariantCulture);
        }

        private BackupResult GenerateBackupResult()
        {
            return new BackupResult
            {
                SnapshotBackup =
                {
                    Skipped = _isFullBackup == false || Configuration.BackupType == BackupType.Backup
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
                GoogleCloudBackup =
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
            if (RestorePointsBase.IsBackupOrSnapshot(filePath))
                return true;

            var extension = Path.GetExtension(filePath);
            return InProgressExtension.Equals(extension, StringComparison.OrdinalIgnoreCase);
        }

        private string GetFileName(
            bool isFullBackup,
            string backupFolder,
            string nowAsString,
            BackupType backupType,
            out string backupFilePath)
        {
            var backupExtension = GetBackupExtension(backupType, isFullBackup);
            var fileName = isFullBackup ?
                GetFileNameFor(backupExtension, nowAsString, backupFolder, out backupFilePath, throwWhenFileExists: true) :
                GetFileNameFor(backupExtension, nowAsString, backupFolder, out backupFilePath);

            return fileName;
        }

        private string GetBackupExtension(BackupType type, bool isFullBackup)
        {
            if (isFullBackup == false)
                return _isBackupEncrypted ? Constants.Documents.PeriodicBackup.EncryptedIncrementalBackupExtension :
                    Constants.Documents.PeriodicBackup.IncrementalBackupExtension;

            switch (type)
            {
                case BackupType.Backup:
                    return _isBackupEncrypted ?
                        Constants.Documents.PeriodicBackup.EncryptedFullBackupExtension : Constants.Documents.PeriodicBackup.FullBackupExtension;

                case BackupType.Snapshot:
                    return _isBackupEncrypted ?
                        Constants.Documents.PeriodicBackup.EncryptedSnapshotExtension : Constants.Documents.PeriodicBackup.SnapshotExtension;

                default:
                    throw new ArgumentOutOfRangeException(nameof(type), type, null);
            }
        }

        private static string GetFileNameFor(
            string backupExtension,
            string nowAsString,
            string backupFolder,
            out string backupFilePath,
            bool throwWhenFileExists = false)
        {
            var fileName = $"{nowAsString}{backupExtension}";
            backupFilePath = Path.Combine(backupFolder, fileName);

            if (File.Exists(backupFilePath))
            {
                if (throwWhenFileExists)
                    throw new InvalidOperationException($"File '{backupFilePath}' already exists!");

                while (true)
                {
                    fileName = $"{GetFormattedDate(DateTime.Now)}{backupExtension}";
                    backupFilePath = Path.Combine(backupFolder, fileName);

                    if (File.Exists(backupFilePath) == false)
                        break;
                }
            }

            return fileName;
        }

        private sealed class InternalBackupResult
        {
            public long LastEtag { get; set; }
            public string LastDatabaseChangeVector { get; set; }
            public long LastRaftIndex { get; set; }
        }

        private InternalBackupResult CreateLocalBackupOrSnapshot(PeriodicBackupStatus status, string backupFilePath, string folderName, string fileName, long? startEtag, long? startRaftIndex)
        {
            var internalBackupResult = new InternalBackupResult();

            using (status.LocalBackup.UpdateStats(_isFullBackup))
            {
                // will rename the file after the backup is finished
                var tempBackupFilePath = backupFilePath + InProgressExtension;

                try
                {
                    BackupTypeValidation();

                    AddInfo($"Started {GetBackupDescription(Configuration.BackupType, _isFullBackup)}");
                    if (Configuration.BackupType == BackupType.Backup ||
                        Configuration.BackupType == BackupType.Snapshot && _isFullBackup == false)
                    {
                        // smuggler backup
                        var options = new DatabaseSmugglerOptionsServerSide(AuthorizationStatus.DatabaseAdmin)
                        {
                            IncludeArtificial = true, // we want to include artificial in backup
                            IncludeArchived = true // wa want also to include archived documents in backup
                        };

                        options.OperateOnTypes |= DatabaseItemType.Tombstones;
                        options.OperateOnTypes |= DatabaseItemType.CompareExchangeTombstones;

                        var currentBackupResult = CreateBackup(options, tempBackupFilePath, folderName, fileName, startEtag, startRaftIndex);

                        if (_isFullBackup)
                        {
                            internalBackupResult = currentBackupResult;
                        }
                        else
                        {
                            if (BackupResult.GetLastEtag() == _previousBackupStatus.LastEtag &&
                                BackupResult.GetLastRaftIndex() == _previousBackupStatus.LastRaftIndex.LastEtag)
                            {
                                internalBackupResult.LastEtag = startEtag ?? 0;
                                internalBackupResult.LastDatabaseChangeVector = _previousBackupStatus.LastDatabaseChangeVector;
                                internalBackupResult.LastRaftIndex = startRaftIndex ?? 0;
                            }
                            else
                            {
                                internalBackupResult = currentBackupResult;
                            }
                        }
                    }
                    else
                    {
                        // snapshot backup
                        ValidateFreeSpaceForSnapshot(tempBackupFilePath);

                        (internalBackupResult.LastEtag, internalBackupResult.LastDatabaseChangeVector) = Database.ReadLastEtagAndChangeVector();
                        internalBackupResult.LastRaftIndex = GetDatabaseEtagForBackup();
                        var databaseSummary = Database.GetDatabaseSummary();
                        var indexesCount = Database.IndexStore.Count;

                        var totalSw = Stopwatch.StartNew();
                        var sw = Stopwatch.StartNew();
                        var compressionAlgorithm = Configuration.SnapshotSettings?.CompressionAlgorithm ?? Database.Configuration.Backup.SnapshotCompressionAlgorithm;
                        var compressionLevel = Configuration.SnapshotSettings?.CompressionLevel ?? Database.Configuration.Backup.SnapshotCompressionLevel;
                        var excludeIndexes = Configuration.SnapshotSettings?.ExcludeIndexes ?? false;

                        using (var stream = GetStreamForBackupDestination(tempBackupFilePath, folderName, fileName))
                        {
                            try
                            {
                                var smugglerResult = Database.FullBackupTo(stream, compressionAlgorithm, compressionLevel, excludeIndexes,
                                    info =>
                                    {
                                        AddInfo(info.Message);

                                        BackupResult.SnapshotBackup.ReadCount += info.FilesCount;
                                        if (sw.ElapsedMilliseconds > 0 && info.FilesCount > 0)
                                        {
                                            AddInfo($"Backed up {BackupResult.SnapshotBackup.ReadCount} " +
                                                    $"file{(BackupResult.SnapshotBackup.ReadCount > 1 ? "s" : string.Empty)}");
                                            sw.Restart();
                                        }
                                    }, TaskCancelToken.Token);

                                FlushToDisk(stream);

                                EnsureSnapshotProcessed(databaseSummary, smugglerResult, indexesCount);
                            }
                            catch
                            {
                                OnBackupException?.Invoke();
                                throw;
                            }
                        }

                        AddInfo($"Backed up {BackupResult.SnapshotBackup.ReadCount} files, " +
                                $"took: {totalSw.ElapsedMilliseconds:#,#;;0}ms");
                    }

                    RenameFile(tempBackupFilePath, backupFilePath);

                    status.LocalBackup.Exception = null;
                }
                catch (Exception e)
                {
                    status.LocalBackup.Exception = e.ToString();

                    // deleting the temp backup file if the backup failed
                    DeleteFile(tempBackupFilePath);

                    throw;
                }
            }

            if (_backupToLocalFolder)
            {
                var sp = Stopwatch.StartNew();
                var localRetentionPolicy = new LocalRetentionPolicyRunner(RetentionPolicyParameters, Configuration.LocalSettings.FolderPath);
                localRetentionPolicy.Execute();
                sp.Stop();
                status.LocalRetentionDurationInMs = sp.ElapsedMilliseconds;
            }

            return internalBackupResult;
        }

        protected virtual void RenameFile(string tempBackupFilePath, string backupFilePath)
        {
            IOExtensions.RenameFile(tempBackupFilePath, backupFilePath);
        }

        protected virtual Stream GetStreamForBackupDestination(string filePath, string folderName, string fileName)
        {
            return SafeFileStream.Create(filePath, FileMode.Create);
        }

        public static string GetBackupDescription(BackupType backupType, bool isFull)
        {
            var isFullText = isFull ? "a full" : "an incremental";
            var backupTypeText = backupType == BackupType.Snapshot ? "snapshot backup" : "backup";
            return $"{isFullText} {backupTypeText}";
        }

        protected virtual void DeleteFile(string path)
        {
            try
            {
                IOExtensions.DeleteFile(path);
            }
            catch (Exception e)
            {
                if (_logger.IsInfoEnabled)
                    _logger.Info($"Failed to delete file: {path}", e);
            }
        }

        protected virtual void ValidateFreeSpaceForSnapshot(string filePath)
        {
            long totalUsedSpace = 0;
            foreach (var mountPointUsage in Database.GetMountPointsUsage(includeTempBuffers: false))
            {
                if (mountPointUsage.Type == nameof(StorageEnvironmentType.Index) &&
                   Configuration.SnapshotSettings is { ExcludeIndexes: true })
                    continue;

                totalUsedSpace += mountPointUsage.UsedSpace;
            }

            var directoryPath = Path.GetDirectoryName(filePath);

            BackupHelper.AssertFreeSpaceForSnapshot(directoryPath, totalUsedSpace, "create a snapshot", _logger);
        }

        private void BackupTypeValidation()
        {
            if (Database.MasterKey == null &&
                Configuration.BackupEncryptionSettings?.EncryptionMode == EncryptionMode.UseDatabaseKey)
                throw new InvalidOperationException("Can't use database key for backup encryption, the key doesn't exist");

            if (Configuration.BackupType == BackupType.Snapshot && _isFullBackup &&
                Configuration.BackupEncryptionSettings != null &&
                Configuration.BackupEncryptionSettings.EncryptionMode == EncryptionMode.UseProvidedKey)
                throw new InvalidOperationException("Can't snapshot an encrypted database with a different key");
        }

        private void EnsureSnapshotProcessed(DatabaseSummary databaseSummary, SmugglerResult snapshotSmugglerResult, long indexesCount)
        {
            BackupResult.SnapshotBackup.Processed = true;
            BackupResult.DatabaseRecord.Processed = true;
            BackupResult.RevisionDocuments.Attachments.Processed = true;
            BackupResult.Tombstones.Processed = true;
            BackupResult.Indexes.Processed = true;
            BackupResult.Indexes.ReadCount = indexesCount;

            BackupResult.Documents.Processed = true;
            BackupResult.Documents.ReadCount = databaseSummary.DocumentsCount;
            BackupResult.Documents.Attachments.Processed = true;
            BackupResult.Documents.Attachments.ReadCount = databaseSummary.AttachmentsCount;
            BackupResult.Counters.Processed = true;
            BackupResult.Counters.ReadCount = databaseSummary.CounterEntriesCount;
            BackupResult.RevisionDocuments.Processed = true;
            BackupResult.RevisionDocuments.ReadCount = databaseSummary.RevisionsCount;
            BackupResult.Conflicts.Processed = true;
            BackupResult.Conflicts.ReadCount = databaseSummary.ConflictsCount;

            BackupResult.Identities.Processed = true;
            BackupResult.Identities.ReadCount = snapshotSmugglerResult.Identities.ReadCount;
            BackupResult.CompareExchange.Processed = true;
            BackupResult.CompareExchange.ReadCount = snapshotSmugglerResult.CompareExchange.ReadCount;
            BackupResult.CompareExchangeTombstones.Processed = true;
            BackupResult.Subscriptions.Processed = true;
            BackupResult.Subscriptions.ReadCount = snapshotSmugglerResult.Subscriptions.ReadCount;

            BackupResult.TimeSeries.Processed = true;
            BackupResult.TimeSeries.ReadCount = databaseSummary.TimeSeriesSegmentsCount;
        }

        protected void AddInfo(string message)
        {
            BackupResult.AddInfo(message);
            _onProgress.Invoke(BackupResult.Progress);
        }

        private InternalBackupResult CreateBackup(DatabaseSmugglerOptionsServerSide options, string backupFilePath, string folderName, string fileName, long? startDocumentEtag, long? startRaftIndex)
        {
            // the last etag is already included in the last backup
            var currentBackupResults = new InternalBackupResult();
            startDocumentEtag = startDocumentEtag == null ? 0 : ++startDocumentEtag;
            startRaftIndex = startRaftIndex == null ? 0 : ++startRaftIndex;

            using (var stream = GetStreamForBackupDestination(backupFilePath, folderName, fileName))
            using (var outputStream = GetOutputStream(stream))
            using (Database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (Database.DocumentsStorage.ContextPool.AllocateOperationContext(out JsonOperationContext smugglerContext))
            {
                try
                {
                    var smugglerSource = Database.Smuggler.CreateSource(startDocumentEtag.Value, startRaftIndex.Value, _logger);
                    var smugglerDestination = new StreamDestination(outputStream, context, smugglerSource, Database.Configuration.Backup.CompressionAlgorithm.ToExportCompressionAlgorithm(), Database.Configuration.Backup.CompressionLevel);
                    var smuggler = Database.Smuggler.Create(
                        smugglerSource,
                        smugglerDestination,
                        smugglerContext,
                        options: options,
                        result: BackupResult,
                        onProgress: _onProgress,
                        token: TaskCancelToken.Token);

                    smuggler.ExecuteAsync().Wait();

                    FlushToDisk(outputStream);

                    currentBackupResults.LastEtag = smugglerSource.LastEtag;
                    currentBackupResults.LastDatabaseChangeVector = smugglerSource.LastDatabaseChangeVector;
                    currentBackupResults.LastRaftIndex = smugglerSource.LastRaftIndex;

                    return currentBackupResults;
                }
                catch
                {
                    OnBackupException?.Invoke();
                    throw;
                }
            }
        }

        protected virtual void FlushToDisk(Stream outputStream)
        {
            switch (outputStream)
            {
                case EncryptingXChaCha20Poly1305Stream encryptedStream:
                    encryptedStream.Flush(flushToDisk: true);
                    break;

                case FileStream file:
                    file.Flush(flushToDisk: true);
                    break;

                default:
                    throw new NotSupportedException($" {outputStream.GetType()} not supported");
            }
        }

        public Stream GetOutputStream(Stream fileStream)
        {
            if (_isBackupEncrypted == false)
                return fileStream;

            byte[] encryptionKey;

            if (Database.MasterKey != null && Configuration.BackupEncryptionSettings == null)
                encryptionKey = Database.MasterKey;
            else if (Configuration.BackupEncryptionSettings.EncryptionMode == EncryptionMode.UseDatabaseKey)
                encryptionKey = Database.MasterKey;
            else
                encryptionKey = Convert.FromBase64String(Configuration.BackupEncryptionSettings.Key);
           
            var encryptingStream = new EncryptingXChaCha20Poly1305Stream(fileStream, encryptionKey);

            encryptingStream.Initialize();

            return encryptingStream;
        }

        protected virtual void UploadToServer(string backupFilePath, string folderName, string fileName)
        {
            var s3Settings = GetBackupConfigurationFromScript(Configuration.S3Settings, x => JsonDeserializationServer.S3Settings(x),
                settings => PutServerWideBackupConfigurationCommand.UpdateSettingsForS3(settings, Database.Name));
            var glacierSettings = GetBackupConfigurationFromScript(Configuration.GlacierSettings, x => JsonDeserializationServer.GlacierSettings(x),
                settings => PutServerWideBackupConfigurationCommand.UpdateSettingsForGlacier(settings, Database.Name));
            var azureSettings = GetBackupConfigurationFromScript(Configuration.AzureSettings, x => JsonDeserializationServer.AzureSettings(x),
                settings => PutServerWideBackupConfigurationCommand.UpdateSettingsForAzure(settings, Database.Name));
            var googleCloudSettings = GetBackupConfigurationFromScript(Configuration.GoogleCloudSettings, x => JsonDeserializationServer.GoogleCloudSettings(x),
                settings => PutServerWideBackupConfigurationCommand.UpdateSettingsForGoogleCloud(settings, Database.Name));
            var ftpSettings = GetBackupConfigurationFromScript(Configuration.FtpSettings, x => JsonDeserializationServer.FtpSettings(x),
                settings => PutServerWideBackupConfigurationCommand.UpdateSettingsForFtp(settings, Database.Name));

            TaskCancelToken.Token.ThrowIfCancellationRequested();

            var uploaderSettings = new UploaderSettings(Database.Configuration.Backup)
            {
                S3Settings = s3Settings,
                GlacierSettings = glacierSettings,
                AzureSettings = azureSettings,
                GoogleCloudSettings = googleCloudSettings,
                FtpSettings = ftpSettings,

                FilePath = backupFilePath,
                FolderName = folderName,
                FileName = fileName,
                DatabaseName = Database.Name,
                TaskName = _taskName,

                BackupType = Configuration.BackupType
            };

            var backupUploader = new BackupUploader(uploaderSettings, RetentionPolicyParameters, _logger, BackupResult, _onProgress, TaskCancelToken);
            backupUploader.ExecuteUpload();
        }

        private void UpdateOperationId(PeriodicBackupStatus runningBackupStatus)
        {
            runningBackupStatus.LastOperationId = _operationId;
            if (_previousBackupStatus.LastOperationId == null ||
                _previousBackupStatus.NodeTag != Database.ServerStore.NodeTag ||
                _previousBackupStatus.Error != null)
                return;

            // dismiss the previous operation
            var id = $"{NotificationType.OperationChanged}/{_previousBackupStatus.LastOperationId.Value}";
            Database.NotificationCenter.Dismiss(id);
        }

        public static string GetDateTimeFormat(string fileName)
        {
            return fileName.Length == LegacyDateTimeFormat.Length ? LegacyDateTimeFormat : DateTimeFormat;
        }
    }
}
