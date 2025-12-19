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
    public sealed class RestoreFromAzure : DownloadFromAzure, IRestoreSource
    {
        private readonly ServerStore _serverStore;
        private readonly CancellationToken _cancellationToken;
        public string _remoteFolderName { get; set; }

        public RestoreFromAzure([NotNull] ServerStore serverStore, RestoreFromAzureConfiguration restoreFromConfiguration, CancellationToken cancellationToken) : base(restoreFromConfiguration, serverStore.Configuration.Backup, token: cancellationToken)
        {
            _serverStore = serverStore ?? throw new ArgumentNullException(nameof(serverStore));
            _cancellationToken = cancellationToken;
            _remoteFolderName = restoreFromConfiguration.Settings.RemoteFolderName;
        }

        public async Task<ZipArchive> GetZipArchiveForSnapshot(string path, Action<string> onProgress)
        {
            var blob = await _client.GetBlobAsync(path);
            var file = await RestoreUtils.CopyRemoteStreamLocallyAsync(blob.Data, blob.Size, _serverStore.Configuration, onProgress, _cancellationToken);
            return new DeleteOnCloseZipArchive(file, ZipArchiveMode.Read);
        }

        public async Task<List<string>> GetFilesForRestore()
        {
            var prefix = string.IsNullOrEmpty(_remoteFolderName) ? "" : _remoteFolderName;
            var allObjects = await _client.ListBlobsAsync(prefix, string.Empty, false);
            return allObjects.List.Select(x => x.Name).ToList();
        }

        public Task ValidateConfigurationsAsync()
        {
            return Task.CompletedTask;
        }
    }
}
