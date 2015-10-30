using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using Raven.Monitor.IO.Data;

namespace Raven.Monitor.IO
{
    internal class DiskQueuePerformanceMonitor : IMonitor
    {
        private readonly PerformanceCounter ReadQueueCounter;
        private readonly PerformanceCounter WriteQueueCounter;
        private DiskQueueReportDoc reportDoc;

        public DiskQueuePerformanceMonitor(MonitorOptions option)
        {
            string codeBase = Assembly.GetExecutingAssembly().CodeBase;
            UriBuilder uri = new UriBuilder(codeBase);
            string path = Uri.UnescapeDataString(uri.Path);
            var rootDir = Path.GetPathRoot(path);
            // 'c:\\' => 'c:'
            rootDir = rootDir.Substring(0, rootDir.Length - 1);
            ReadQueueCounter = new PerformanceCounter("LogicalDisk", "Avg. Disk Read Queue Length", rootDir);
            ReadQueueCounter.NextValue();
            WriteQueueCounter = new PerformanceCounter("LogicalDisk", "Avg. Disk Write Queue Length", rootDir);
            WriteQueueCounter.NextValue();
        }
        public void Dispose()
        {
            ReadQueueCounter.Dispose();
            WriteQueueCounter.Dispose();
        }

        public void Start()
        {
            reportDoc = new DiskQueueReportDoc() { StartTime = DateTime.Now, Sampling = new List<DiskQueueSample>() };
        }

        public void Stop()
        {
            reportDoc.EndTime = DateTime.UtcNow;
            using (var session = RavenDocumentStore.DocumentStore.OpenSession())
            {
                session.Store(reportDoc);
                session.SaveChanges();
            }
        }

        public void OnTimerTick()
        {
            var read = ReadQueueCounter.NextValue();
            var write = WriteQueueCounter.NextValue();
            reportDoc.Sampling.Add(new DiskQueueSample() { Time = DateTime.UtcNow, ReadQueueSize = read ,WriteQueueSize = write});
        }
    }
}
