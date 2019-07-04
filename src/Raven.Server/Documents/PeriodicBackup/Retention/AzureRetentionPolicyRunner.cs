using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Raven.Server.Documents.PeriodicBackup.Azure;

namespace Raven.Server.Documents.PeriodicBackup.Retention
{
    public class AzureRetentionPolicyRunner : RetentionPolicyRunnerBase
    {
        private readonly RavenAzureClient _client;

        protected override string Name => "Azure";

        public AzureRetentionPolicyRunner(RetentionPolicyBaseParameters parameters, RavenAzureClient client)
            : base(parameters)
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
