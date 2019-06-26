using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations.Backups;
using Raven.Server.Documents.PeriodicBackup.Azure;

namespace Raven.Server.Documents.PeriodicBackup.Retention
{
    public class AzureRetentionPolicyRunner : RetentionPolicyRunnerBase
    {
        private readonly RavenAzureClient _client;

        public AzureRetentionPolicyRunner(AzureSettings settings)
        {
            _client = new RavenAzureClient(settings);
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

        public override void Dispose()
        {
            _client.Dispose();
        }
    }
}
