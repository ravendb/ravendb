using System;
using System.Collections.Generic;

namespace Raven.NewClient.Abstractions.FileSystem
{
    public class DestinationSyncResult
    {
        public string DestinationServer { get; set; }

        public string DestinationFileSystem { get; set; }

        public IEnumerable<SynchronizationReport> Reports { get; set; }

        public Exception Exception { get; set; }
    }
}
