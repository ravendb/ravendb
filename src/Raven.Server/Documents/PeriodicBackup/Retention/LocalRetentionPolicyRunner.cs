using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents.Smuggler;
using Raven.Server.Utils;

namespace Raven.Server.Documents.PeriodicBackup.Retention
{
    public class LocalRetentionPolicyRunner : RetentionPolicyRunnerBase
    {
        private readonly string _folderPath;

        protected override string Name => "Local";

        public LocalRetentionPolicyRunner(RetentionPolicyBaseParameters parameters, string folderPath)
            : base(parameters)
        {
            _folderPath = folderPath;
        }

        protected override Task<GetFoldersResult> GetSortedFolders()
        {
            var folders = Directory.GetDirectories(_folderPath).OrderBy(x => x).ToList();

            return Task.FromResult(new GetFoldersResult
            {
                List = folders,
                HasMore = false
            });
        }

        protected override string GetFolderName(string folderPath)
        {
            return Path.GetFileName(folderPath);
        }

        protected override Task<GetBackupFolderFilesResult> GetBackupFilesInFolder(string folder, DateTime startDateOfRetentionRange)
        {
            return GetBackupFilesInFolderInternal(folder);
        }

        private static Task<GetBackupFolderFilesResult> GetBackupFilesInFolderInternal(string folder)
        {
            try
            {
                var orderedBackups = Directory.GetFiles(folder).AsEnumerable().OrderBackups();
                var backupFiles = new GetBackupFolderFilesResult
                {
                    FirstFile = orderedBackups.FirstOrDefault(),
                    LastFile = orderedBackups.LastOrDefault()
                };
                return Task.FromResult(backupFiles);
            }
            catch (DirectoryNotFoundException)
            {
                return Task.FromResult((GetBackupFolderFilesResult)null);
            }
        }

        protected override Task DeleteFolders(List<string> folders)
        {
            foreach (var folder in folders)
            {
                IOExtensions.DeleteDirectory(folder);

                CancellationToken.ThrowIfCancellationRequested();
            }

            return Task.CompletedTask;
        }
    }
}
