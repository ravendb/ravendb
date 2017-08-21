using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations;
using Raven.Server.Config.Settings;
using Raven.Server.Documents.Indexes;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow;
using Voron;
using Voron.Data;
using Voron.Exceptions;
using Voron.Impl.Compaction;

namespace Raven.Server.Documents
{
    public class CompactDatabaseTask
    {
        private readonly ServerStore _serverStore;
        private readonly string _database;
        private CancellationToken _token;

        public CompactDatabaseTask(ServerStore serverStore, string database, CancellationToken token)
        {
            _serverStore = serverStore;
            _database = database;
            _token = token;
        }

        public async Task<IOperationResult> Execute(Action<IOperationProgress> onProgress)
        {
            var progress = new DatabaseCompactionProgress
            {
                Message = $"Started database compaction for {_database}"
            };
            onProgress?.Invoke(progress);

            using (await _serverStore.DatabasesLandlord.UnloadAndLockDatabase(_database))
            {
                var configuration = _serverStore.DatabasesLandlord.CreateDatabaseConfiguration(_database);

                using (var src = DocumentsStorage.GetStorageEnvironmentOptionsFromConfiguration(configuration, new IoChangesNotifications(),
                    new CatastrophicFailureNotification(exception => throw new InvalidOperationException($"Failed to compact database {_database}", exception))))
                {
                    var basePath = configuration.Core.DataDirectory.FullPath;
                    IOExtensions.DeleteDirectory(basePath + "-Compacting");
                    IOExtensions.DeleteDirectory(basePath + "-old");
                    try
                    {

                        configuration.Core.DataDirectory = new PathSetting(basePath + "-Compacting");
                        using (var dst = DocumentsStorage.GetStorageEnvironmentOptionsFromConfiguration(configuration, new IoChangesNotifications(),
                            new CatastrophicFailureNotification(exception => throw new InvalidOperationException($"Failed to compact database {_database}", exception))))
                        {
                            _token.ThrowIfCancellationRequested();
                            StorageCompaction.Execute(src, (StorageEnvironmentOptions.DirectoryStorageEnvironmentOptions)dst, progressReport =>
                            {
                                progress.ObjectType = progressReport.ObjectType.ToString();
                                progress.GlobalProgress = progressReport.GlobalProgress;
                                progress.GlobalTotal = progressReport.GlobalTotal;
                                progress.ObjectName = progressReport.ObjectName;
                                progress.ObjectProgress = progressReport.ObjectProgress;
                                progress.ObjectTotal = progressReport.ObjectTotal;
                                progress.Message = progressReport.Message;
                                onProgress?.Invoke(progress);
                            }, _token);
                        }

                        _token.ThrowIfCancellationRequested();
                        IOExtensions.MoveDirectory(basePath, basePath + "-old");
                        IOExtensions.MoveDirectory(basePath + "-Compacting", basePath);
                    }
                    catch (Exception e)
                    {
                        throw new InvalidOperationException($"Failed to execute compaction for {_database}", e);
                    }
                    finally
                    {
                        IOExtensions.DeleteDirectory(basePath + "-Compacting");
                        IOExtensions.DeleteDirectory(basePath + "-old");
                    }
                }
            }

            return DatabaseCompactionResult.Instance;
        }
    }
}
