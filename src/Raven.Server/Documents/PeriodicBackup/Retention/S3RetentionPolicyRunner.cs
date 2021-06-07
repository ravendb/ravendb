using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using Raven.Server.Documents.PeriodicBackup.Aws;
using Constants = Raven.Client.Constants;

namespace Raven.Server.Documents.PeriodicBackup.Retention
{
    public class S3RetentionPolicyRunner : RetentionPolicyRunnerBase
    {
        private readonly RavenAwsS3Client _client;

        protected override string Name => "S3";

        private const string Delimiter = "/";

        private string _folderContinuationToken = null;

        public S3RetentionPolicyRunner(RetentionPolicyBaseParameters parameters, RavenAwsS3Client client)
            : base(parameters)
        {
            _client = client;
        }

        protected override GetFoldersResult GetSortedFolders()
        {
            var prefix = $"{_client.RemoteFolderName}{Delimiter}";
            var result = _client.ListObjects(prefix, Delimiter, true, continuationToken: _folderContinuationToken);
            _folderContinuationToken = result.ContinuationToken;

            return new GetFoldersResult
            {
                List = result.FileInfoDetails.Select(x => x.FullPath).ToList(),
                HasMore = result.ContinuationToken != null
            };
        }

        protected override string GetFolderName(string folderPath)
        {
            return folderPath.Substring(0, folderPath.Length - 1);
        }

        protected override GetBackupFolderFilesResult GetBackupFilesInFolder(string folder, DateTime startDateOfRetentionRange)
        {
            var backupFiles = new GetBackupFolderFilesResult();
            // backups are ordered in lexicographical order
            // if the "directory" was created manually it will appear before the first file
            var files = _client.ListObjects(folder, null, false, take: 2);
            backupFiles.FirstFile = files.FileInfoDetails?.Select(x => x.FullPath).FirstOrDefault();

            var startAfter = $"{folder}{startDateOfRetentionRange.ToString(BackupTask.DateTimeFormat, CultureInfo.InvariantCulture)}{Constants.Documents.PeriodicBackup.IncrementalBackupExtension}";
            files = _client.ListObjects(folder, null, false, take: 1, startAfter: startAfter);

            backupFiles.LastFile = files.FileInfoDetails?.Select(x => x.FullPath).LastOrDefault();

            return backupFiles;
        }

        protected override void DeleteFolders(List<string> folders)
        {
            // deleting multiple objects is limited to 1000 in a single batch
            const int numberOfObjectsInBatch = 1000;
            var objectsToDelete = new List<string>();

            foreach (var folder in folders)
            {
                string filesContinuationToken = null;

                do
                {
                    // delete all objects in that folder
                    var objects = _client.ListObjects(folder, null, false, continuationToken: filesContinuationToken);

                    foreach (var file in objects.FileInfoDetails)
                    {
                        if (objectsToDelete.Count == numberOfObjectsInBatch)
                        {
                            _client.DeleteMultipleObjects(objectsToDelete);
                            objectsToDelete.Clear();
                        }

                        objectsToDelete.Add(file.FullPath);
                    }

                    filesContinuationToken = objects.ContinuationToken;

                    CancellationToken.ThrowIfCancellationRequested();

                } while (filesContinuationToken != null);
            }

            if (objectsToDelete.Count > 0)
                _client.DeleteMultipleObjects(objectsToDelete);
        }
    }
}
