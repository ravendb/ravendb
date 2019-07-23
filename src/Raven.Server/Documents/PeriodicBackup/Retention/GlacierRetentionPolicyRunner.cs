using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Raven.Server.Documents.PeriodicBackup.Aws;

namespace Raven.Server.Documents.PeriodicBackup.Retention
{
    public class GlacierRetentionPolicyRunner : RetentionPolicyRunnerBase
    {
        private readonly RavenAwsGlacierClient _client;

        protected override string Name => "Glacier";

        public GlacierRetentionPolicyRunner(RetentionPolicyBaseParameters parameters, RavenAwsGlacierClient client)
            : base(parameters)
        {
            _client = client;
        }

        protected override Task<GetFoldersResult> GetSortedFolders()
        {
            throw new NotSupportedException();
        }

        protected override string GetFolderName(string folderPath)
        {
            throw new NotSupportedException();
        }

        protected override Task<GetBackupFolderFilesResult> GetBackupFilesInFolder(string folder, DateTime startDateOfRetentionRange)
        {
            throw new NotSupportedException();
        }

        protected override Task DeleteFolders(List<string> folders)
        {
            throw new NotSupportedException();
        }
    }
}
