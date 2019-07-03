using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations.Backups;
using Raven.Server.Utils;

namespace Raven.Server.Documents.PeriodicBackup.Retention
{
    public class LocalRetentionPolicyRunner : RetentionPolicyRunnerBase
    {
        private readonly string _folderPath;

        protected override string Name => "Local";

        public LocalRetentionPolicyRunner(RetentionPolicy retentionPolicy, string databaseName, Action<string> onProgress, string folderPath)
            : base(retentionPolicy, databaseName, onProgress)
        {
            _folderPath = folderPath;
        }

        protected override Task<List<string>> GetFolders()
        {
            var folders = Directory.GetDirectories(_folderPath);
            return Task.FromResult(folders.ToList());
        }

        protected override string GetFolderName(string folderPath)
        {
            return Path.GetFileName(folderPath);
        }

        protected override Task<List<string>> GetFiles(string folder)
        {
            try
            {
                return Task.FromResult(Directory.GetFiles(folder).ToList());
            }
            catch (DirectoryNotFoundException)
            {
                return Task.FromResult(new List<string>());
            }
        }

        protected override Task DeleteFolders(List<FolderDetails> folderDetails)
        {
            foreach (var folderDetail in folderDetails)
            {
                IOExtensions.DeleteDirectory(folderDetail.Name);
            }

            return Task.CompletedTask;
        }
    }
}
