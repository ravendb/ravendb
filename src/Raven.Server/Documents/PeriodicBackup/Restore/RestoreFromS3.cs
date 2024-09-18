using System;
using System.Collections.Generic;
using System.IO.Compression;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Operations.Backups;
using Raven.Server.ServerWide;
using Raven.Server.Utils;

namespace Raven.Server.Documents.PeriodicBackup.Restore
{
    public sealed class RestoreFromS3 : DownloadFromS3
    {
        private readonly ServerStore _serverStore;
        private readonly CancellationToken _cancellationToken;
        private readonly string _remoteFolderName;

        public RestoreFromS3([NotNull] ServerStore serverStore, RestoreFromS3Configuration restoreFromConfiguration, CancellationToken cancellationToken) : base(restoreFromConfiguration, serverStore.Configuration.Backup, token: cancellationToken)
        {
            _serverStore = serverStore ?? throw new ArgumentNullException(nameof(serverStore));
            _cancellationToken = cancellationToken;
            _remoteFolderName = restoreFromConfiguration.Settings.RemoteFolderName;
        }

        public override async Task<ZipArchive> GetZipArchiveForSnapshot(string path, Action<string> onProgress)
        {
            var blob = await _client.GetObjectAsync(path);
            var file = await RestoreUtils.CopyRemoteStreamLocallyAsync(blob.Data, blob.Size, _serverStore.Configuration, onProgress, _cancellationToken);
            return new DeleteOnCloseZipArchive(file, ZipArchiveMode.Read);
        }

        public override async Task<List<string>> GetFilesForRestore()
        {
            var prefix = string.IsNullOrEmpty(_remoteFolderName) ? "" : _remoteFolderName.TrimEnd('/') + "/";
            var allObjects = await _client.ListAllObjectsAsync(prefix, string.Empty, false);
            return allObjects.Select(x => x.FullPath).ToList();
        }

        public override string GetBackupPath(string fileName)
        {
            return fileName;
        }

        public override string GetBackupLocation()
        {
            return _remoteFolderName;
        }
    }
}
