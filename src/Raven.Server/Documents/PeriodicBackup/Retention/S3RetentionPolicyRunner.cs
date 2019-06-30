using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations.Backups;
using Raven.Server.Documents.PeriodicBackup.Aws;

namespace Raven.Server.Documents.PeriodicBackup.Retention
{
    public class S3RetentionPolicyRunner : RetentionPolicyRunnerBase
    {
        private readonly RavenAwsS3Client _client;

        public override string Name => "S3";

        private const string Delimiter = "/";

        public S3RetentionPolicyRunner(RetentionPolicy retentionPolicy, string databaseName, RavenAwsS3Client client)
            : base(retentionPolicy, databaseName)
        {
            _client = client;
        }

        protected override async Task<List<string>> GetFolders()
        {
            var prefix = $"{_client.RemoteFolderName}{Delimiter}";
            var folders = await _client.ListObjects(prefix, Delimiter, true);
            return folders.Select(x => x.FullPath).ToList();
        }

        protected override (string BackupTimeAsString, string DatabaseName, string NodeTag) ParseFolderName(string folder)
        {
            // there are no folders in s3
            // the returned path ends with /
            folder = folder.Substring(0, folder.Length - 1);
            return base.ParseFolderName(folder);
        }

        protected override async Task<List<string>> GetFiles(string folder)
        {
            var files = await _client.ListObjects(folder, null, false);
            return files.Select(x => x.FullPath).ToList();
        }

        protected override async Task DeleteFolders(List<FolderDetails> folderDetails)
        {
            // deleting multiple objects is limited to 1000 in a single batch
            const int numberOfObjectsInBatch = 1000;

            var allObjects = folderDetails.SelectMany(x => x.Files);
            if (folderDetails.Sum(x => x.Files.Count) <= numberOfObjectsInBatch)
            {
                await _client.DeleteMultipleObjects(allObjects.ToList());
                return;
            }

            var objectsToDelete = new List<string>();
            foreach (var obj in allObjects)
            {
                
                if (objectsToDelete.Count == numberOfObjectsInBatch)
                {
                    await _client.DeleteMultipleObjects(objectsToDelete);
                    objectsToDelete.Clear();
                }

                objectsToDelete.Add(obj);
            }

            if (objectsToDelete.Count > 0)
                await _client.DeleteMultipleObjects(objectsToDelete);
        }
    }
}
