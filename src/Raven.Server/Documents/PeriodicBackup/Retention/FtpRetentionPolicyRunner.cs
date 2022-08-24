using System;
using System.Collections.Generic;
using Sparrow.Logging;

namespace Raven.Server.Documents.PeriodicBackup.Retention
{
    public class FtpRetentionPolicyRunner : RetentionPolicyRunnerBase
    {
        private readonly RavenFtpClient _client;

        protected override string Name => "Ftp";

        public FtpRetentionPolicyRunner(RetentionPolicyBaseParameters parameters, RavenFtpClient client, Logger logger)
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
