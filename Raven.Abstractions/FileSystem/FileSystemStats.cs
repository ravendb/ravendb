using System;
using System.Collections.Generic;

namespace Raven.Abstractions.FileSystem
{
    public class FileSystemStats
    {
        public string Name { get; set; }

        public Guid FileSystemId { get; set; }

        public string ServerUrl { get; set; }

        public long FileCount { get; set; }

        public FileSystemMetrics Metrics { get; set; }

        public IList<SynchronizationDetails> ActiveSyncs { get; set; }

        public IList<SynchronizationDetails> PendingSyncs { get; set; }
    }
}
