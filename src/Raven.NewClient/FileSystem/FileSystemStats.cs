using System.Collections.Generic;

namespace Raven.NewClient.Abstractions.FileSystem
{
    public class FileSystemStats
    {
        public string Name { get; set; }

        public long FileCount { get; set; }

        public FileSystemMetrics Metrics { get; set; }

        public IList<SynchronizationDetails> ActiveSyncs { get; set; }

        public IList<SynchronizationDetails> PendingSyncs { get; set; } 
    }
}
