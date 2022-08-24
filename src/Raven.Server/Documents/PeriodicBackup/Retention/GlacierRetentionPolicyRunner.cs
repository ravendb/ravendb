using System;
using System.Collections.Generic;
using Raven.Server.Documents.PeriodicBackup.Aws;
using Sparrow.Logging;

namespace Raven.Server.Documents.PeriodicBackup.Retention
{
    public class GlacierRetentionPolicyRunner : RetentionPolicyRunnerBase
    {
        private readonly RavenAwsGlacierClient _client;

        protected override string Name => "Glacier";

        public GlacierRetentionPolicyRunner(RetentionPolicyBaseParameters parameters, RavenAwsGlacierClient client, Logger logger)
            : base(parameters, logger)
        {
            _client = client;
        }

        protected override GetFoldersResult GetSortedFolders()
        {
            throw new NotSupportedException();
        }

        protected override string GetFolderName(string folderPath)
        {
            throw new NotSupportedException();
        }

        protected override GetBackupFolderFilesResult GetBackupFilesInFolder(string folder, DateTime startDateOfRetentionRange)
        {
            throw new NotSupportedException();
        }

        protected override void DeleteFolders(List<string> folders)
        {
            throw new NotSupportedException();
        }
    }
}
