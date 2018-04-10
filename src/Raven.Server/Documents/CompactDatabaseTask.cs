using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations;
using Raven.Server.Config;
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

        public async Task Execute(Action<IOperationProgress>  onProgress, CompactionResult result)
        {
            if (_isCompactionInProgress)
                throw new InvalidOperationException($"Database '{_database}' cannot be compacted because compaction is already in progress.");
            
            result.AddMessage($"Started database compaction for {_database}");
            onProgress?.Invoke(result);

            _isCompactionInProgress = true;           
            bool done = false; 
            string compactDirectory = null;
            string tmpDirectory = null;

            try
            {
                var documentDatabase = await _serverStore.DatabasesLandlord.TryGetOrCreateResourceStore(_database);
                var configuration = _serverStore.DatabasesLandlord.CreateDatabaseConfiguration(_database);

                using (await _serverStore.DatabasesLandlord.UnloadAndLockDatabase(_database, "it is being compacted"))
                using (var src = DocumentsStorage.GetStorageEnvironmentOptionsFromConfiguration(configuration, new IoChangesNotifications(),
                new CatastrophicFailureNotification((endId, exception) => throw new InvalidOperationException($"Failed to compact database {_database}", exception))))
                {
                    InitializeOptions(src, configuration, documentDatabase);

                    var basePath = configuration.Core.DataDirectory.FullPath;
                    compactDirectory = basePath + "-compacting";
                    tmpDirectory = basePath + "-old";

                    //Making sure we can read and write to both the tmp (backup) folder and compaction folder before we do any real work.
                    if (IOExtensions.EnsureReadWritePermissionForDirectory(compactDirectory) == false)
                    {
                        throw new UnauthorizedAccessException($"Couldn't gain read/write access to compact directory {compactDirectory}");
                    }

                    if (IOExtensions.EnsureReadWritePermissionForDirectory(tmpDirectory) == false)
                    {
                        throw new UnauthorizedAccessException($"Couldn't gain read/write access to tmp directory {tmpDirectory}");
                    }

                    IOExtensions.DeleteDirectory(compactDirectory);
                    IOExtensions.DeleteDirectory(tmpDirectory);
                    configuration.Core.DataDirectory = new PathSetting(compactDirectory);
                    using (var dst = DocumentsStorage.GetStorageEnvironmentOptionsFromConfiguration(configuration, new IoChangesNotifications(),
                        new CatastrophicFailureNotification((envId, exception) => throw new InvalidOperationException($"Failed to compact database {_database}", exception))))
                    {
                        InitializeOptions(dst, configuration, documentDatabase);

                        _token.ThrowIfCancellationRequested();
                        StorageCompaction.Execute(src, (StorageEnvironmentOptions.DirectoryStorageEnvironmentOptions)dst, progressReport =>
                        {
                            result.Progress.TreeProgress = progressReport.TreeProgress;
                            result.Progress.TreeTotal = progressReport.TreeTotal;
                            result.Progress.TreeName = progressReport.TreeName;
                            result.Progress.GlobalProgress = progressReport.GlobalProgress;
                            result.Progress.GlobalTotal = progressReport.GlobalTotal;
                            result.AddMessage(progressReport.Message);
                            onProgress?.Invoke(result);
                        }, _token);
                    }

                    result.TreeName = null;
                    
                    _token.ThrowIfCancellationRequested();

                    SwitchDatabaseDirectories(basePath, tmpDirectory, compactDirectory);
                    done = true;
                }                
                
            }
            catch (Exception e)
            {
                throw new InvalidOperationException($"Failed to execute compaction for {_database}", e);
            }
            finally
            {                
                IOExtensions.DeleteDirectory(compactDirectory);
                if (done)
                {
                    IOExtensions.DeleteDirectory(tmpDirectory);
                }
                _isCompactionInProgress = false;
            }
        }

        private static void InitializeOptions(StorageEnvironmentOptions options, RavenConfiguration configuration, DocumentDatabase documentDatabase)
        {
            options.ForceUsing32BitsPager = configuration.Storage.ForceUsing32BitsPager;
            options.OnNonDurableFileSystemError += documentDatabase.HandleNonDurableFileSystemError;
            options.OnRecoveryError += documentDatabase.HandleOnDatabaseRecoveryError;
            options.CompressTxAboveSizeInBytes = configuration.Storage.CompressTxAboveSize.GetValue(SizeUnit.Bytes);
            options.TimeToSyncAfterFlashInSec = (int)configuration.Storage.TimeToSyncAfterFlash.AsTimeSpan.TotalSeconds;
            options.NumOfConcurrentSyncsPerPhysDrive = configuration.Storage.NumberOfConcurrentSyncsPerPhysicalDrive;
            options.MasterKey = documentDatabase.MasterKey?.ToArray(); // clone 
            options.DoNotConsiderMemoryLockFailureAsCatastrophicError = documentDatabase.Configuration.Security.DoNotConsiderMemoryLockFailureAsCatastrophicError;
            if (configuration.Storage.MaxScratchBufferSize.HasValue)
                options.MaxScratchBufferSize = configuration.Storage.MaxScratchBufferSize.Value.GetValue(SizeUnit.Bytes);
        }

        private static void SwitchDatabaseDirectories(string basePath, string backupDirectory, string compactDirectory)
        {
            IOExtensions.MoveDirectory(basePath, backupDirectory);
            IOExtensions.MoveDirectory(compactDirectory, basePath);

            var oldIndexesPath = new PathSetting(backupDirectory).Combine("Indexes");
            var newIndexesPath = new PathSetting(basePath).Combine("Indexes");
            IOExtensions.MoveDirectory(oldIndexesPath.FullPath, newIndexesPath.FullPath);

            var oldConfigPath = new PathSetting(backupDirectory).Combine("Configuration");
            var newConfigPath = new PathSetting(basePath).Combine("Configuration");
            IOExtensions.MoveDirectory(oldConfigPath.FullPath, newConfigPath.FullPath);
        }
    }
}
