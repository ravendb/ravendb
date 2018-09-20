using System;
using System.Collections.Generic;
using System.IO;
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

        public async Task Execute(Action<IOperationProgress> onProgress, CompactionResult result)
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
                new CatastrophicFailureNotification((endId, path, exception) => throw new InvalidOperationException($"Failed to compact database {_database} ({path})", exception))))
                {
                    InitializeOptions(src, configuration, documentDatabase);

                    var basePath = configuration.Core.DataDirectory.FullPath;
                    compactDirectory = basePath + "-compacting";
                    tmpDirectory = basePath + "-old";

                    EnsureDirectoriesPermission(basePath, compactDirectory, tmpDirectory);

                    IOExtensions.DeleteDirectory(compactDirectory);
                    IOExtensions.DeleteDirectory(tmpDirectory);
                    configuration.Core.DataDirectory = new PathSetting(compactDirectory);
                    using (var dst = DocumentsStorage.GetStorageEnvironmentOptionsFromConfiguration(configuration, new IoChangesNotifications(),
                        new CatastrophicFailureNotification((envId, path, exception) => throw new InvalidOperationException($"Failed to compact database {_database} ({path})", exception))))
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

                    EnsureDirectoriesPermission(basePath, compactDirectory, tmpDirectory);
                    IOExtensions.DeleteDirectory(tmpDirectory);

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

        private static void EnsureDirectoriesPermission(params string[] directories)
        {
            var missingPermissions = new List<string>();

            foreach (var directory in directories)
            {
                if (IOExtensions.EnsureReadWritePermissionForDirectory(directory) == false)
                {
                    missingPermissions.Add(directory);
                }
            }

            if (missingPermissions.Count > 0)
            {
                throw new UnauthorizedAccessException(
                    $"Couldn't gain read/write access to the following directories:{Environment.NewLine}{string.Join(Environment.NewLine, missingPermissions)}");
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
            foreach (var moveDir in new(string Src, string Dst)[]
            {
                (basePath, backupDirectory),
                (compactDirectory, basePath),
                (new PathSetting(backupDirectory).Combine("Indexes").FullPath, new PathSetting(basePath).Combine("Indexes").FullPath),
                (new PathSetting(backupDirectory).Combine("Configuration").FullPath, new PathSetting(basePath).Combine("Configuration").FullPath)
            })
            {
                try
                {
                    IOExtensions.MoveDirectory(moveDir.Src, moveDir.Dst);
                }
                catch (Exception e)
                {
                    ThrowCantMoveDirectory(moveDir.Src, moveDir.Dst, e, basePath);
                }
            }
        }

        private static void ThrowCantMoveDirectory(string src, string dst, Exception e, string databasePath)
        {
            throw new IOException(
                $"Cannot move directory '{src}' to '{dst}'. Please verify the directory '{databasePath}' exists and contains the database's data before loading the database",
                e);
        }
    }
}
