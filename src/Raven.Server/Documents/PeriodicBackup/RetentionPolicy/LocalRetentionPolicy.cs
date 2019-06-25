using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations.Backups;
using Raven.Server.Utils;

namespace Raven.Server.Documents.PeriodicBackup
{
    public class LocalRetentionPolicy : RetentionPolicyBase
    {
        private readonly string _folderPath;

        public LocalRetentionPolicy(RetentionPolicy retentionPolicy, string databaseName, string folderPath) : base(retentionPolicy, databaseName)
        {
            _folderPath = folderPath;
        }

        public override Task<List<string>> GetFolders()
        {
            var folders = Directory.GetDirectories(_folderPath);
            return Task.FromResult(folders.ToList());
        }

        public override Task<List<string>> GetFiles(string folder)
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

        public override Task DeleteFolder(string folder)
        {
            IOExtensions.DeleteDirectory(folder);
            return Task.CompletedTask;
        }
    }
}
