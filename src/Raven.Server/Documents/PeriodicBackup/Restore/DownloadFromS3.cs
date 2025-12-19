using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations.Backups;
using Raven.Server.Documents.PeriodicBackup.Aws;

namespace Raven.Server.Documents.PeriodicBackup.Restore;

public class DownloadFromS3 : IDownloadSource
{
    protected readonly RavenAwsS3Client _client;

    public DownloadFromS3(RestoreFromS3Configuration restoreFromConfiguration, Config.Categories.BackupConfiguration backupConfiguration, CancellationToken token)
    {
        _client = new RavenAwsS3Client(restoreFromConfiguration.Settings, backupConfiguration, progress: null, token);
    }

    public async Task<Stream> GetStream(string path)
    {
        var blob = await _client.GetObjectAsync(path);
        return blob.Data;
    }

    public void Dispose()
    {
        _client?.Dispose();
    }
}
