using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations.Backups;
using Raven.Server.Documents.PeriodicBackup.Aws;

namespace Raven.Server.Documents.PeriodicBackup.Retention
{
    public class GlacierRetentionPolicyRunner : RetentionPolicyRunnerBase
    {
        private readonly RavenAwsGlacierClient _client;

        public override string Name => "Glacier";

        public GlacierRetentionPolicyRunner(RetentionPolicy retentionPolicy, string databaseName, RavenAwsGlacierClient client)
            : base(retentionPolicy, databaseName)
        {
            _client = client;
        }

        protected override Task<List<string>> GetFolders()
        {
            throw new NotSupportedException();
        }

        protected override string GetFolderName(string folderPath)
        {
            throw new NotSupportedException();
        }

        protected override Task<List<string>> GetFiles(string folder)
        {
            throw new NotSupportedException();
        }

        protected override Task DeleteFolders(List<FolderDetails> folderDetails)
        {
            throw new NotSupportedException();
        }
    }
}
