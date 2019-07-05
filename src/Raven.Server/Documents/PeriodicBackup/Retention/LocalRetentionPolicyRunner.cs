using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Raven.Server.Utils;
using Raven.Client.Documents.Smuggler;

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

        protected override Task<string> GetFirstFileInFolder(string folder)
        {
            try
            {
                var firstFile = Directory.GetFiles(folder).AsEnumerable().OrderBackups().FirstOrDefault();
                return Task.FromResult(firstFile);
            }
            catch (DirectoryNotFoundException)
            {
                return Task.FromResult((string)null);
            }
        }

        protected override Task DeleteFolders(List<string> folders)
        {
            foreach (var folder in folders)
            {
                IOExtensions.DeleteDirectory(folder);
            }

            return Task.CompletedTask;
        }
    }
}
