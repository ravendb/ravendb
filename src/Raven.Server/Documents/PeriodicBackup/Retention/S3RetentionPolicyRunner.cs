using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations.Backups;
using Raven.Server.Documents.PeriodicBackup.Aws;

namespace Raven.Server.Documents.PeriodicBackup.Retention
{
    public class S3RetentionPolicyRunner : RetentionPolicyRunnerBase
    {
        private readonly RavenAwsS3Client _client;

        public override string Name => "S3";

        public S3RetentionPolicyRunner(RetentionPolicy retentionPolicy, string databaseName, RavenAwsS3Client client)
            : base(retentionPolicy, databaseName)
        {
            _client = client;
        }

        public override Task<List<string>> GetFolders()
        {
            throw new NotSupportedException();
        }

        public override Task<List<string>> GetFiles(string folder)
        {
            throw new NotSupportedException();
        }

        public override async Task DeleteFolder(string folder)
        {
            await _client.DeleteObject(folder);
        }
    }
}
