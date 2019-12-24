using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
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
            _folderPath = Path.GetFullPath(folderPath);
        }

        protected override GetFoldersResult GetSortedFolders()
        {
            var folders = Directory.GetDirectories(_folderPath).OrderBy(x => x).ToList();

            return new GetFoldersResult
            {
                List = folders,
                HasMore = false
            };
        }

        protected override string GetFolderName(string folderPath)
        {
            return Path.GetFileName(folderPath);
        }

        protected override GetBackupFolderFilesResult GetBackupFilesInFolder(string folder, DateTime startDateOfRetentionRange)
        {
            return GetBackupFilesInFolderInternal(folder);
        }

        private static GetBackupFolderFilesResult GetBackupFilesInFolderInternal(string folder)
        {
            try
            {
                var orderedBackups = Directory.GetFiles(folder).AsEnumerable().OrderBackups();
                var backupFiles = new GetBackupFolderFilesResult
                {
                    FirstFile = orderedBackups.FirstOrDefault(),
                    LastFile = orderedBackups.LastOrDefault()
                };
                return backupFiles;
            }
            catch (DirectoryNotFoundException)
            {
                return (GetBackupFolderFilesResult)null;
            }
        }

        protected override void DeleteFolders(List<string> folders)
        {
            foreach (var folder in folders)
            {
                IOExtensions.DeleteDirectory(folder);

                CancellationToken.ThrowIfCancellationRequested();
            }
        }
    }
}
