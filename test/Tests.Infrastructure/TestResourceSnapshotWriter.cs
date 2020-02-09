using System;
using System.IO;
using System.Runtime.Serialization;
using CsvHelper;
using Raven.Server.Utils;
using Raven.Server.Utils.Cpu;
using Sparrow;
using Sparrow.LowMemory;

namespace Tests.Infrastructure
{
    public class TestResourceSnapshotWriter : IDisposable
    {
        private readonly ICpuUsageCalculator _cpuUsageCalculator;
        private readonly TestResourcesAnalyzerMetricCacher _metricCacher;
        private readonly CsvWriter _csvWriter;
        private readonly object _syncObject = new object();
        
        public TestResourceSnapshotWriter(string filename = null)
        {
            lock (_syncObject)
            {
                _cpuUsageCalculator = CpuHelper.GetOSCpuUsageCalculator();
                _metricCacher = new TestResourcesAnalyzerMetricCacher(_cpuUsageCalculator);

                FileStream file;
                filename ??= $"TestResources_{DateTime.Now:dd_MM_yyyy_HH_mm_ss}.csv";
                try
                {
                    file = File.OpenWrite(filename ?? filename);
                }
                catch (UnauthorizedAccessException) //just in case, don't think it will ever be needed
                {
                    throw new InvalidOperationException($"Tried to open '{filename}' for write, but failed with {nameof(UnauthorizedAccessException)}. This is weird, and is probably a bug.");
                }

                file.Position = 0;
                file.SetLength(0);

                _csvWriter = new CsvWriter(new StreamWriter(file));
                _csvWriter.WriteHeader(typeof(TestResourceSnapshot));
            }
        }

        public void WriteResourceSnapshot(TestStage testStage, string comment = "")
        {
            lock (_syncObject)
            {
                _csvWriter.NextRecord();
                _csvWriter.WriteRecord(new TestResourceSnapshot(this, testStage, comment));
            }
        }

        public class TestResourceSnapshot
        {
            public TestStage TestStage { get; set; }

            public string Comment { get; set; }

            public DateTime InfluxTimestamp => DateTime.Parse(TimeStamp);

            public string TimeStamp { get; set; }
            
            public long MachineCpuUsage { get; set; }
            
            public long ProcessCpuUsage { get; set; }
            
            public long ProcessMemoryUsageInMb { get; set; }
            
            public long TotalMemoryInMb { get; set; }
            
            public long AvailableMemoryInMb { get; set; }
            
            public long TotalCommittableMemoryInMb { get; set; }
            
            public long CurrentCommitChargeInMb { get; set; }
            
            public long SharedCleanMemoryInMb { get; set; }
            
            public long TotalScratchDirtyMemory { get; set; }
            
            public long TotalScratchAllocatedMemory { get; set; }
            
            public long TotalDirtyMemory { get; set; }
            
            public bool IsHighDirty { get; set; }

            [Obsolete("Needed for serialization, should not be used directly", true)]
            public TestResourceSnapshot()
            {
                
            }

            public static TestResourceSnapshot GetEmpty() => (TestResourceSnapshot)FormatterServices.GetUninitializedObject(typeof(TestResourceSnapshot));

            internal TestResourceSnapshot(TestResourceSnapshotWriter parent, TestStage testStage, string comment)
            {
                var timeStamp = DateTime.Now;
                var cpuUsage = parent._metricCacher.GetValue(
                    MetricCacher.Keys.Server.CpuUsage, 
                    parent._cpuUsageCalculator.Calculate);
                
                var memoryInfo = parent._metricCacher.GetValue<MemoryInfoResult>(MetricCacher.Keys.Server.MemoryInfoExtended);

                TotalScratchAllocatedMemory = new Size(MemoryInformation.GetTotalScratchAllocatedMemory(), SizeUnit.Bytes).GetValue(SizeUnit.Megabytes);
                TotalDirtyMemory = new Size(MemoryInformation.GetDirtyMemoryState().TotalDirtyInBytes, SizeUnit.Bytes).GetValue(SizeUnit.Megabytes);
                IsHighDirty = MemoryInformation.GetDirtyMemoryState().IsHighDirty;
                TestStage = testStage;
                TimeStamp = timeStamp.ToString("o");
                Comment = comment;
                MachineCpuUsage = (long)cpuUsage.MachineCpuUsage;
                ProcessCpuUsage = (long)cpuUsage.ProcessCpuUsage;
                ProcessMemoryUsageInMb = memoryInfo.WorkingSet.GetValue(SizeUnit.Megabytes);
                TotalMemoryInMb = memoryInfo.TotalPhysicalMemory.GetValue(SizeUnit.Megabytes);
                TotalCommittableMemoryInMb = memoryInfo.TotalCommittableMemory.GetValue(SizeUnit.Megabytes);
                AvailableMemoryInMb = memoryInfo.AvailableMemory.GetValue(SizeUnit.Megabytes);
                CurrentCommitChargeInMb = memoryInfo.CurrentCommitCharge.GetValue(SizeUnit.Megabytes);
                SharedCleanMemoryInMb = memoryInfo.SharedCleanMemory.GetValue(SizeUnit.Megabytes);
                TotalScratchDirtyMemory = memoryInfo.TotalScratchDirtyMemory.GetValue(SizeUnit.Megabytes);
            }
        }

        public void Dispose()
        {
            _csvWriter.Flush();
            _csvWriter.Dispose();
        }
    }

    public enum TestStage
    {
        TestAssemblyStarted,
        TestAssemblyEnded,
        TestClassStarted,
        TestClassEnded,
        TestStarted,
        TestEndedBeforeGc,
        TestEndedAfterGc,
        Delta
    }
}
