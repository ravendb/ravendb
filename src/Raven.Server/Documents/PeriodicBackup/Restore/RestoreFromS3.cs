using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Operations.Backups;
using Raven.Server.Documents.PeriodicBackup.Aws;
using Raven.Server.ServerWide;
using Raven.Server.Utils;

namespace Raven.Server.Documents.PeriodicBackup.Restore
{
    public sealed class RestoreFromS3 : IRestoreSource
    {
        private readonly ServerStore _serverStore;
        private readonly CancellationToken _cancellationToken;
        private readonly RavenAwsS3Client _client;
        private readonly string _remoteFolderName;

        public RestoreFromS3([NotNull] ServerStore serverStore, RestoreFromS3Configuration restoreFromConfiguration, CancellationToken cancellationToken)
        {
            _serverStore = serverStore ?? throw new ArgumentNullException(nameof(serverStore));
            _cancellationToken = cancellationToken;
            _client = new RavenAwsS3Client(restoreFromConfiguration.Settings, serverStore.Configuration.Backup, cancellationToken: cancellationToken);
            _remoteFolderName = restoreFromConfiguration.Settings.RemoteFolderName;
        }

        public async Task<Stream> GetStream(string path)
        {
            var blob = await _client.GetObjectAsync(path);
            return blob.Data;
        }

        public async Task<ZipArchive> GetZipArchiveForSnapshot(string path, Action<string> onProgress)
        {
            var blob = await _client.GetObjectAsync(path);
            var file = await RestoreUtils.CopyRemoteStreamLocallyAsync(blob.Data, blob.Size, _serverStore.Configuration, onProgress, _cancellationToken);
            return new DeleteOnCloseZipArchive(file, ZipArchiveMode.Read);
        }

        public async Task<List<string>> GetFilesForRestore()
        {
            var prefix = string.IsNullOrEmpty(_remoteFolderName) ? "" : _remoteFolderName.TrimEnd('/') + "/";
            var allObjects = await _client.ListAllObjectsAsync(prefix, string.Empty, false);
            return allObjects.Select(x => x.FullPath).ToList();
        }

        public string GetBackupPath(string fileName)
        {
            return fileName;
        }

        public string GetBackupLocation()
        {
            return _remoteFolderName;
        }

        public async Task ValidateConfigurationsAsync()
        {
            var files = await GetFilesForRestore();
            foreach (var file in files)
            {
                var meta = await _client.GetMetaDataAsync(file);
                if (ArchiveClasses.Contains(meta.StorageClass) == false)
                    return;
                
                if (meta.RestoreInProgress.HasValue == false)
                    throw new InvalidOperationException(
                        $"Object '{file}' in bucket '{_client._bucketName}' is stored in the S3 '{meta.StorageClass?.Value}' storage class, but no restore job is in progress. Initiate a restore in S3 and wait until it completes.");

                if (meta.RestoreInProgress.Value)
                    throw new InvalidOperationException(
                        $"Object '{file}' in bucket '{_client._bucketName}' is stored in the S3 '{meta.StorageClass?.Value}' storage class, and its restore is still in progress. Wait until the restore completes before retrying.");
            }
        }

        private static readonly HashSet<string> ArchiveClasses = new(StringComparer.Ordinal)
        {
            "DEEP_ARCHIVE",
            "GLACIER",
        };

        public void Dispose()
        {
            _client?.Dispose();
        }
    }
}
