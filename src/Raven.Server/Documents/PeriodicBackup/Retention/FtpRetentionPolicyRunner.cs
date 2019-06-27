using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations.Backups;

namespace Raven.Server.Documents.PeriodicBackup.Retention
{
    public class FtpRetentionPolicyRunner : RetentionPolicyRunnerBase
    {
        private readonly RavenFtpClient _client;

        public override string Name => "Glacier";

        public FtpRetentionPolicyRunner(RetentionPolicy retentionPolicy, string databaseName, RavenFtpClient client)
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
