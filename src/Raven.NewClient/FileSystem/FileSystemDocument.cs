using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Raven.NewClient.Abstractions.FileSystem
{
    public class FileSystemDocument
    {
        /// <summary>
        /// The ID can be either the file system name ("FilesystemName") or the full document name ("Raven/FileSystems/FilesystemName").
        /// </summary>
        public string Id { get; set; }
        public Dictionary<string, string> Settings { get; set; }
        public Dictionary<string, string> SecuredSettings { get; set; }
        public bool Disabled { get; set; }

        public FileSystemDocument()
        {
            Settings = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
            SecuredSettings = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        }
    }
}
