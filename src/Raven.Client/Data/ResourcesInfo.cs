
using System.Collections.Generic;

namespace Raven.Client.Data
{
    public class ResourcesInfo
    {
        public List<DatabaseInfo> Databases { get; set; }

        public List<FileSystemInfo> Filesystems { get; set; }

        //TODO: ts, cs

    }

    public class ResourceInfo
    {
        public string Name { get; set; }
        public bool Disabled { get; set; }
    }

    public class DatabaseInfo : ResourceInfo
    {
        public bool RejectClientsEnabled { get; set; }

        public bool IndexingDisabled { get; set; }
    }

    public class FileSystemInfo : ResourceInfo
    {
        //TODO: fill with fs specific properties
    }
}