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

        public GlacierRetentionPolicyRunner(RetentionPolicy retentionPolicy, string databaseName, RavenAwsGlacierClient client)
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
            throw new NotSupportedException();
        }
    }
}
