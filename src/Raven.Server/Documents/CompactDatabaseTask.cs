using System;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations;
using Raven.Server.Config.Settings;
using Raven.Server.ServerWide;
using Raven.Server.Utils;
using Sparrow;
using Voron;
using Voron.Exceptions;
using Voron.Impl.Compaction;

namespace Raven.Server.Documents
{
    public class CompactDatabaseTask
    {
        private readonly ServerStore _serverStore;
        private readonly string _database;
        private CancellationToken _token;
        private bool _isCompactionInProgress;

        public CompactDatabaseTask(ServerStore serverStore, string database, CancellationToken token)
        {
            _serverStore = serverStore;
            _database = database;
            _token = token;
        }

        public async Task Execute(Action<IOperationProgress> onProgress)
        {
            if (_isCompactionInProgress)
                throw new InvalidOperationException($"Database '{_database}' cannot be compacted because compaction is already in progress.");

            var progress = new DatabaseCompactionProgress
            {
                Message = $"Started database compaction for {_database}"
            };
            onProgress?.Invoke(progress);

            _isCompactionInProgress = true;

            var documentDatabase = await _serverStore.DatabasesLandlord.TryGetOrCreateResourceStore(_database);
            var configuration = _serverStore.DatabasesLandlord.CreateDatabaseConfiguration(_database);

            using (await _serverStore.DatabasesLandlord.UnloadAndLockDatabase(_database))
            {
                using (var src = DocumentsStorage.GetStorageEnvironmentOptionsFromConfiguration(configuration, new IoChangesNotifications(),
                    new CatastrophicFailureNotification(exception => throw new InvalidOperationException($"Failed to compact database {_database}", exception))))
                {
                    src.ForceUsing32BitsPager = configuration.Storage.ForceUsing32BitsPager;
                    src.OnNonDurableFileSystemError += documentDatabase.HandleNonDurableFileSystemError;
                    src.OnRecoveryError += documentDatabase.HandleOnRecoveryError;
                    src.CompressTxAboveSizeInBytes = configuration.Storage.CompressTxAboveSize.GetValue(SizeUnit.Bytes);
                    src.TimeToSyncAfterFlashInSec = (int)configuration.Storage.TimeToSyncAfterFlash.AsTimeSpan.TotalSeconds;
                    src.NumOfConcurrentSyncsPerPhysDrive = configuration.Storage.NumberOfConcurrentSyncsPerPhysicalDrive;
                    Sodium.CloneKey(out src.MasterKey, documentDatabase.MasterKey);

                    var basePath = configuration.Core.DataDirectory.FullPath;
                    IOExtensions.DeleteDirectory(basePath + "-Compacting");
                    IOExtensions.DeleteDirectory(basePath + "-old");
                    try
                    {

                        configuration.Core.DataDirectory = new PathSetting(basePath + "-Compacting");
                        using (var dst = DocumentsStorage.GetStorageEnvironmentOptionsFromConfiguration(configuration, new IoChangesNotifications(),
                            new CatastrophicFailureNotification(exception => throw new InvalidOperationException($"Failed to compact database {_database}", exception))))
                        {
                            dst.OnNonDurableFileSystemError += documentDatabase.HandleNonDurableFileSystemError;
                            dst.OnRecoveryError += documentDatabase.HandleOnRecoveryError;
                            dst.CompressTxAboveSizeInBytes = configuration.Storage.CompressTxAboveSize.GetValue(SizeUnit.Bytes);
                            dst.ForceUsing32BitsPager = configuration.Storage.ForceUsing32BitsPager;
                            dst.TimeToSyncAfterFlashInSec = (int)configuration.Storage.TimeToSyncAfterFlash.AsTimeSpan.TotalSeconds;
                            dst.NumOfConcurrentSyncsPerPhysDrive = configuration.Storage.NumberOfConcurrentSyncsPerPhysicalDrive;
                            Sodium.CloneKey(out dst.MasterKey, documentDatabase.MasterKey);

                            _token.ThrowIfCancellationRequested();
                            StorageCompaction.Execute(src, (StorageEnvironmentOptions.DirectoryStorageEnvironmentOptions)dst, progressReport =>
                            {
                                progress.Processed = progressReport.GlobalProgress;
                                progress.Total = progressReport.GlobalTotal;
                                progress.TreeProgress = progressReport.TreeProgress;
                                progress.TreeTotal = progressReport.TreeTotal;
                                progress.TreeName = progressReport.TreeName;
                                progress.Message = progressReport.Message;
                                onProgress?.Invoke(progress);
                            }, _token);
                        }

                        _token.ThrowIfCancellationRequested();
                        IOExtensions.MoveDirectory(basePath, basePath + "-old");
                        IOExtensions.MoveDirectory(basePath + "-Compacting", basePath);

                        var oldIndexesPath = new PathSetting(basePath + "-old").Combine("Indexes");
                        var newIndexesPath = new PathSetting(basePath).Combine("Indexes");
                        IOExtensions.MoveDirectory(oldIndexesPath.FullPath, newIndexesPath.FullPath);

                        var oldConfigPath = new PathSetting(basePath + "-old").Combine("Configuration");
                        var newConfigPath = new PathSetting(basePath).Combine("Configuration");
                        IOExtensions.MoveDirectory(oldConfigPath.FullPath, newConfigPath.FullPath);

                    }
                    catch (Exception e)
                    {
                        throw new InvalidOperationException($"Failed to execute compaction for {_database}", e);
                    }
                    finally
                    {
                        IOExtensions.DeleteDirectory(basePath + "-Compacting");
                        IOExtensions.DeleteDirectory(basePath + "-old");
                        _isCompactionInProgress = false;
                    }
                }
            }
        }
    }
}
