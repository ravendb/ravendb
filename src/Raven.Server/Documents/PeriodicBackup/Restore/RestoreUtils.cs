using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Smuggler;
using Raven.Server.Config.Settings;
using Raven.Server.ServerWide;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Utils;

namespace Raven.Server.Documents.PeriodicBackup.Restore
{
    public static class RestoreUtils
    {
        public static RestoreBackupConfigurationBase GetRestoreConfigurationAndSource(ServerStore serverStore, BlittableJsonReaderObject restoreConfiguration, out IRestoreSource restoreSource)
        {
            RestoreBackupConfigurationBase configuration;
            RestoreType restoreType = default;
            if (restoreConfiguration.TryGet("Type", out string typeAsString))
            {
                if (Enum.TryParse(typeAsString, out restoreType) == false)
                    throw new ArgumentException($"{typeAsString} is unknown backup type.");
            }

            switch (restoreType)
            {
                case RestoreType.Local:
                    var localConfiguration = JsonDeserializationCluster.RestoreBackupConfiguration(restoreConfiguration);
                    configuration = localConfiguration; 
                    restoreSource = new RestoreFromLocal(localConfiguration);
                    break;

                case RestoreType.S3:
                    var s3Configuration = JsonDeserializationCluster.RestoreS3BackupConfiguration(restoreConfiguration);
                    configuration = s3Configuration; 
                    restoreSource = new RestoreFromS3(serverStore, s3Configuration);
                    break;

                case RestoreType.Azure:
                    var azureConfiguration = JsonDeserializationCluster.RestoreAzureBackupConfiguration(restoreConfiguration);
                    configuration = azureConfiguration;
                    restoreSource = new RestoreFromAzure(serverStore, azureConfiguration);
                    break;

                case RestoreType.GoogleCloud:
                    var googleCloudConfiguration = JsonDeserializationCluster.RestoreGoogleCloudBackupConfiguration(restoreConfiguration);
                    configuration = googleCloudConfiguration;
                    restoreSource = new RestoreFromGoogleCloud(serverStore, googleCloudConfiguration);
                    break;

                default:
                    throw new InvalidOperationException($"No matching backup type was found for {restoreType}");
            }

            return configuration;
        }

        public static async Task<AbstractRestoreBackupTask> CreateBackupTaskAsync(ServerStore serverStore, RestoreBackupConfigurationBase configuration, IRestoreSource restoreSource, OperationCancelToken token)
        {
            if (configuration.ShardRestoreSettings.Length > 0)
                return new ShardedRestoreOrchestrationTask(serverStore, configuration, token);
            
            var singleShardRestore = ShardHelper.IsShardedName(configuration.DatabaseName);

            var filesToRestore = await GetOrderedFilesToRestoreAsync(restoreSource, configuration);
            var firstFile = filesToRestore[0];
            var extension = Path.GetExtension(firstFile);

            if (extension is Constants.Documents.PeriodicBackup.SnapshotExtension or
                Constants.Documents.PeriodicBackup.EncryptedSnapshotExtension)
            {
                if (singleShardRestore)
                    throw new InvalidOperationException($"Cannot perform a snapshot restore on sharded database '{configuration.DatabaseName}'");

                return new RestoreSnapshotTask(serverStore, configuration, restoreSource, firstFile, extension, filesToRestore, token);
            }

            if (singleShardRestore)
                return new SingleShardRestoreBackupTask(serverStore, configuration, filesToRestore, restoreSource, token);

            return new RestoreBackupTask(serverStore, configuration, restoreSource, filesToRestore, token);
        }

        public static async Task<Stream> CopyRemoteStreamLocallyAsync(Stream stream, PathSetting tempPath)
        {
            if (stream.CanSeek)
                return stream;

            // This is meant to be used by ZipArchive, which will copy the data locally because is *must* be seekable.
            // To avoid reading everything to memory, we copy to a local file instead. Note that this also ensure that we
            // can process files > 2GB in size. https://github.com/dotnet/runtime/issues/59027
            var basePath = tempPath?.FullPath ?? Path.GetTempPath();
            var filePath = Path.Combine(basePath, $"{Guid.NewGuid()}.restore-local-file");
            var file = SafeFileStream.Create(filePath, FileMode.Create, FileAccess.ReadWrite, FileShare.Read,
                32 * 1024, FileOptions.DeleteOnClose);

            try
            {
                await stream.CopyToAsync(file);
                file.Seek(0, SeekOrigin.Begin);

                return file;
            }
            catch
            {
                try
                {
                    await file.DisposeAsync();
                }
                catch
                {
                    // nothing we can do
                }
                finally
                {
                    PosixFile.DeleteOnClose(filePath);
                }

                throw;
            }

        }

        private static async Task<List<string>> GetOrderedFilesToRestoreAsync(IRestoreSource restoreSource, RestoreBackupConfigurationBase configuration)
        {
            var files = await restoreSource.GetFilesForRestore();

            var orderedFiles = files
                .Where(RestorePointsBase.IsBackupOrSnapshot)
                .OrderBackups()
                .ToList();

            if (orderedFiles.Any() == false)
                throw new ArgumentException($"No files to restore from the backup location, path: {restoreSource.GetBackupLocation()}");

            if (string.IsNullOrWhiteSpace(configuration.LastFileNameToRestore))
                return orderedFiles;

            var filesToRestore = new List<string>();

            foreach (var file in orderedFiles)
            {
                filesToRestore.Add(file);
                if (file.Equals(configuration.LastFileNameToRestore, StringComparison.OrdinalIgnoreCase))
                    break;
            }

            return filesToRestore;
        }
    }
}
