using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Raven.Monitor.IO.Data;

namespace Raven.Monitor.CPU
{
    internal class CpuPerformanceMonitoring : IMonitor
    {
        public CpuPerformanceMonitoring(MonitorOptions options)
        {
            _pid = options.ProcessId;
            var processName = Process.GetProcessById(_pid).ProcessName;
            cpuCounter = new PerformanceCounter("Process",
                "% Processor Time", processName);
            cpuCounter.NextValue();
            logicalCores = Environment.ProcessorCount;
        }
        public void Dispose()
        {
            cpuCounter.Dispose();
        }

        private CpuUssageReportDoc reportDoc;
        private readonly PerformanceCounter cpuCounter;
        private int _pid;
        private int logicalCores;

        public void Start()
        {			
            reportDoc = new CpuUssageReportDoc(){StartTime = DateTime.Now,Sampling = new List<CpuUssageSample>()};
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
            var cpu = cpuCounter.NextValue();
            reportDoc.Sampling.Add(new CpuUssageSample() { Time = DateTime.UtcNow, UsedCpu = cpu / logicalCores });
        }
    }
}
