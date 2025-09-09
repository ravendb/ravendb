using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations.Backups;
using Raven.Server.Documents.PeriodicBackup.Azure;

namespace Raven.Server.Documents.PeriodicBackup.Restore;

public class DownloadFromAzure : IDownloadSource
{
    protected readonly IRavenAzureClient _client;

    public DownloadFromAzure(RestoreFromAzureConfiguration restoreFromConfiguration, Config.Categories.BackupConfiguration backupConfiguration, CancellationToken token)
    {
        _client = RavenAzureClient.Create(restoreFromConfiguration.Settings, backupConfiguration, progress: null, token);
    }

    public async Task<Stream> GetStream(string path)
    {
        var blob = await _client.GetBlobAsync(path);
        return blob.Data;
    }

    public void Dispose()
    {
        _client?.Dispose();
    }
}
