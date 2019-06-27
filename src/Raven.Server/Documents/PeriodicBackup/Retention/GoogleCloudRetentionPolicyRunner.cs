using System.Collections.Generic;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations.Backups;
using Raven.Server.Documents.PeriodicBackup.GoogleCloud;

namespace Raven.Server.Documents.PeriodicBackup.Retention
{
    public class GoogleCloudRetentionPolicyRunner : RetentionPolicyRunnerBase
    {
        private readonly RavenGoogleCloudClient _client;

        public GoogleCloudRetentionPolicyRunner(RetentionPolicy retentionPolicy, string databaseName, RavenGoogleCloudClient client)
            : base(retentionPolicy, databaseName)
        {
            _client = client;
        }

        public override Task<List<string>> GetFolders()
        {
            throw new System.NotImplementedException();
        }

        public override Task<List<string>> GetFiles(string folder)
        {
            throw new System.NotImplementedException();
        }

        public override async Task DeleteFolder(string folder)
        {
            throw new System.NotImplementedException();
        }
    }
}
