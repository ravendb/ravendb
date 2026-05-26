using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Operations.Backups;
using Raven.Server.Documents.PeriodicBackup.Aws;
using Raven.Server.Documents.PeriodicBackup.Restore;

namespace Tests.Infrastructure;

public class S3TestsHelper
{
    internal const string CollectionName = "Orders";

    internal static async Task DeleteObjects(IS3Settings s3Settings, string additionalTable = null)
    {
        if (s3Settings == null)
            return;

        await DeleteObjects(s3Settings, prefix: $"{s3Settings.RemoteFolderName}/{CollectionName}", delimiter: string.Empty);

        if (additionalTable == null)
            return;

        await DeleteObjects(s3Settings, prefix: $"{s3Settings.RemoteFolderName}/{additionalTable}", delimiter: string.Empty);
    }

    internal static async Task DeleteObjects(IS3Settings s3Settings, string prefix, string delimiter, bool listFolder = false)
    {
        if (s3Settings == null)
            return;

        try
        {
            using (var cts = new CancellationTokenSource(TimeSpan.FromMinutes(5)))
            using (var s3Client = new RavenAwsS3Client(s3Settings, RavenTestBase.EtlTestBase.DefaultBackupConfiguration, cancellationToken: cts.Token))
            {
                var cloudObjects = await s3Client.ListObjectsAsync(prefix, delimiter, listFolder);
                if (cloudObjects.FileInfoDetails.Count == 0)
                    return;

                if (listFolder == false)
                {
                    var pathsToDelete = cloudObjects.FileInfoDetails.Select(x => x.FullPath).ToList();
                    s3Client.DeleteMultipleObjects(pathsToDelete);
                    return;
                }

                var filesToDelete = await ListAllFilesInFolders(s3Client, cloudObjects);
                s3Client.DeleteMultipleObjects(filesToDelete);
            }
        }
        catch (Exception)
        {
            // ignored
        }
    }

    internal static async Task<List<string>> ListAllFilesInFolders(RavenAwsS3Client s3Client, ListObjectsResult cloudObjects)
    {
        var files = new List<string>();
        foreach (var folder in cloudObjects.FileInfoDetails)
        {
            var objectsInFolder = await s3Client.ListObjectsAsync(prefix: folder.FullPath, delimiter: string.Empty, listFolders: false);
            files.AddRange(objectsInFolder.FileInfoDetails.Select(fi => fi.FullPath));
        }

        return files;
    }
}
