using System;
using System.Collections.Generic;
using System.IO;
using Redbus.Events;

namespace Tests.ResourceSnapshotAggregator
{
    public class CsvUploadNotification : EventBase, IDisposable
    {
        public CsvUploadNotification(List<Stream> uploaded) => Uploaded = uploaded;

        public string JobName { get; set; }
        public string BuildNumber { get; set; }
        public List<Stream> Uploaded { get; }

        public void Dispose() => Uploaded?.ForEach(s => s.Dispose());
    }
}
