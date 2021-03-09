using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Server.Documents.Indexes.Static.Extensions;
using Raven.Server.Documents.PeriodicBackup.Azure;
using Raven.Server.Documents.PeriodicBackup.Restore;

namespace Raven.Server.Documents.PeriodicBackup.Retention
{
    public class AzureRetentionPolicyRunner : RetentionPolicyRunnerBase
    {
        private readonly RavenAzureClient _client;

        protected override string Name => "Azure";

        private const string Delimiter = "/";

        private string _nextMarker = null;

        public AzureRetentionPolicyRunner(RetentionPolicyBaseParameters parameters, RavenAzureClient client)
            : base(parameters)
        {
            _client = client;
        }

        protected override GetFoldersResult GetSortedFolders()
        {
            var prefix = string.IsNullOrWhiteSpace(_client.RemoteFolderName) ? string.Empty : $"{_client.RemoteFolderName}{Delimiter}";
            var result = _client.ListBlobs(prefix, Delimiter, listFolders: true, marker: _nextMarker);
            _nextMarker = result.NextMarker;

            return new GetFoldersResult
            {
                List = result.List.Select(x => x.Name).OrderBy(x => x).ToList(),
                HasMore = result.NextMarker != null
            };
        }

        protected override string GetFolderName(string folderPath)
        {
            return folderPath.Substring(0, folderPath.Length - 1);
        }

        protected override GetBackupFolderFilesResult GetBackupFilesInFolder(string folder, DateTime startDateOfRetentionRange)
        {
            var backupFiles = new GetBackupFolderFilesResult();
            string blobsNextMarker = null;
            bool firstFileSet = false;

            do
            {
                var blobs = _client.ListBlobs(folder, delimiter: null, listFolders: false, marker: blobsNextMarker);

                foreach (var blob in blobs.List)
                {
                    if (firstFileSet == false)
                    {
                        backupFiles.FirstFile = blob.Name;
                        firstFileSet = true;
                    }

                    if (RestorePointsBase.TryExtractDateFromFileName(blob.Name, out var lastModified) && lastModified > startDateOfRetentionRange)
                    {
                        backupFiles.LastFile = blob.Name;
                        return backupFiles;

                    }
                }

                blobsNextMarker = blobs.NextMarker;
                CancellationToken.ThrowIfCancellationRequested();
            } while (blobsNextMarker != null);

            return backupFiles;
        }

        protected override void DeleteFolders(List<string> folders)
        {
            const int numberOfObjectsInBatch = 256;
            var blobsToDelete = new List<string>();

            foreach (var folder in folders)
            {
                string blobsNextMarker = null;

                do
                {
                    var blobs = _client.ListBlobs(folder, delimiter: null, listFolders: false, marker: blobsNextMarker);

                    foreach (var blob in blobs.List)
                    {
                        if (blobsToDelete.Count == numberOfObjectsInBatch)
                        {
                            _client.DeleteBlobs(blobsToDelete);
                            blobsToDelete.Clear();
                        }

                        blobsToDelete.Add(blob.Name);
                    }

                    blobsNextMarker = blobs.NextMarker;

                    CancellationToken.ThrowIfCancellationRequested();

                } while (blobsNextMarker != null);
            }

            if (blobsToDelete.Count > 0)
                _client.DeleteBlobs(blobsToDelete);
        }
    }
}
