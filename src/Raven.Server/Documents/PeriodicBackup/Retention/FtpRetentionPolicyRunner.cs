using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Raven.Server.Documents.PeriodicBackup.Retention
{
    public class FtpRetentionPolicyRunner : RetentionPolicyRunnerBase
    {
        private readonly RavenFtpClient _client;

        protected override string Name => "Glacier";

        public FtpRetentionPolicyRunner(RetentionPolicyBaseParameters parameters, RavenFtpClient client)
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

        protected override Task<string> GetFirstFileInFolder(string folder)
        {
            throw new NotSupportedException();
        }

        protected override Task DeleteFolders(List<string> folders)
        {
            throw new NotSupportedException();
        }
    }
}
