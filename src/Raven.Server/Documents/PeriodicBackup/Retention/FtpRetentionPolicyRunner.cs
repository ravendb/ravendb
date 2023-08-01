using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Server.Documents.Indexes.Static.Extensions;

namespace Raven.Server.Documents.PeriodicBackup.Retention
{
    public sealed class FtpRetentionPolicyRunner : RetentionPolicyRunnerBase
    {
        private readonly RavenFtpClient _client;

        protected override string Name => "FTP";

        public FtpRetentionPolicyRunner(RetentionPolicyBaseParameters parameters, RavenFtpClient client)
            : base(parameters)
        {
            _client = client;
        }

        protected override GetFoldersResult GetSortedFolders()
        {
            var dirs = _client.GetFolders();
            return new GetFoldersResult { List = dirs.OrderBy(x => x).ToList(), HasMore = false };
        }

        protected override string GetFolderName(string folderPath)
        {
            return folderPath.Substring(0, folderPath.Length - 1);
        }

        protected override GetBackupFolderFilesResult GetBackupFilesInFolder(string folder, DateTime startDateOfRetentionRange)
        {
            var filesList = _client.GetFiles(folder);
            return new GetBackupFolderFilesResult { FirstFile = filesList.FirstOrDefault(), LastFile = filesList.LastOrDefault() };
        }

        protected override void DeleteFolders(List<string> folders)
        {
            foreach (var folder in folders)
            {
                _client.DeleteFolder(folder);

                CancellationToken.ThrowIfCancellationRequested();
            }
        }
    }
}
