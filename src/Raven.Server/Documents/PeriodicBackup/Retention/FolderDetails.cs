using System.Collections.Generic;

namespace Raven.Server.Documents.PeriodicBackup.Retention
{
    public class FolderDetails
    {
        public string Name { get; set; }

        public List<string> Files { get; set; }
    }
}