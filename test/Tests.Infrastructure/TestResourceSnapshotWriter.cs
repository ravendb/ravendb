using System;
using System.Globalization;
using System.IO;
using CsvHelper;
using Raven.Server.Utils;
using Raven.Server.Utils.Cpu;
using Sparrow;
using Sparrow.LowMemory;
using Xunit.Abstractions;

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

                filename ??= $"TestResources_{DateTime.UtcNow:dd_MM_yyyy_HH_mm_ss}.csv";
                
                var file = File.OpenWrite(filename);

                file.Position = 0;
                file.SetLength(0);

                _csvWriter = new CsvWriter(new StreamWriter(file), CultureInfo.InvariantCulture);
                _csvWriter.WriteHeader(typeof(TestResourceSnapshot));
            }
        }
        
        public void WriteResourceSnapshot(TestStage testStage, ITestAssembly testAssembly) => WriteResourceSnapshot(testStage, testAssembly.Assembly.Name);
        
        public void WriteResourceSnapshot(TestStage testStage, ITestClass testClass) => WriteResourceSnapshot(testStage, testClass.Class.Name);

        public void WriteResourceSnapshot(TestStage testStage, ITestMethod testMethod, TestResult? testResult = null)
        {
            var displayName = $"{testMethod.TestClass.Class.Name}::{testMethod.Method.Name}()";
            WriteResourceSnapshot(testStage, displayName, testResult);
        }

        private void WriteResourceSnapshot(TestStage testStage, string comment, TestResult? testResult = null)
        {
            lock (_syncObject)
            {
                _csvWriter.NextRecord();

                var snapshot = GetTestResourceSnapshot(testStage, comment, testResult);
                _csvWriter.WriteRecord(snapshot);
            }
        }

        private TestResourceSnapshot GetTestResourceSnapshot(TestStage testStage, string comment, TestResult? testResult)
        {
            var timeStamp = DateTime.UtcNow;
            var cpuUsage = _metricCacher.GetValue(MetricCacher.Keys.Server.CpuUsage, _cpuUsageCalculator.Calculate);

            var memoryInfo = _metricCacher.GetValue<MemoryInfoResult>(MetricCacher.Keys.Server.MemoryInfoExtended);

            var snapshot = new TestResourceSnapshot
            {
                TotalScratchAllocatedMemory = new Size(MemoryInformation.GetTotalScratchAllocatedMemory(), SizeUnit.Bytes).GetValue(SizeUnit.Megabytes),
                TotalDirtyMemory = new Size(MemoryInformation.GetDirtyMemoryState().TotalDirtyInBytes, SizeUnit.Bytes).GetValue(SizeUnit.Megabytes),
                IsHighDirty = MemoryInformation.GetDirtyMemoryState().IsHighDirty,
                TestStage = testStage,
                Timestamp = timeStamp.ToString("o"),
                TestResult = testResult,
                Comment = comment,
                MachineCpuUsage = (long)cpuUsage.MachineCpuUsage,
                ProcessCpuUsage = (long)cpuUsage.ProcessCpuUsage,
                ProcessMemoryUsageInMb = memoryInfo.WorkingSet.GetValue(SizeUnit.Megabytes),
                TotalMemoryInMb = memoryInfo.TotalPhysicalMemory.GetValue(SizeUnit.Megabytes),
                TotalCommittableMemoryInMb = memoryInfo.TotalCommittableMemory.GetValue(SizeUnit.Megabytes),
                AvailableMemoryInMb = memoryInfo.AvailableMemory.GetValue(SizeUnit.Megabytes),
                CurrentCommitChargeInMb = memoryInfo.CurrentCommitCharge.GetValue(SizeUnit.Megabytes),
                SharedCleanMemoryInMb = memoryInfo.SharedCleanMemory.GetValue(SizeUnit.Megabytes),
                TotalScratchDirtyMemory = memoryInfo.TotalScratchDirtyMemory.GetValue(SizeUnit.Megabytes)
            };

            return snapshot;
        }

        public class TestResourceSnapshot
        {
            public TestStage TestStage { get; set; }

            public string Comment { get; set; }

            public TestResult? TestResult { get; set; }
            
            public string Timestamp { get; set; }
            
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

    public enum TestResult
    {
        Success,
        Fail,
        Skipped
    }
}
