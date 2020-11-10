using System;
using System.Globalization;
using System.IO;
using System.Reflection;
using CsvHelper;
using Raven.Server.Utils.Cpu;
using Sparrow;
using Sparrow.LowMemory;
using Xunit.Abstractions;

namespace Tests.Infrastructure.TestMetrics
{
    public class TestResourceSnapshotWriter : IDisposable
    {
        private readonly TestResourcesAnalyzerMetricCacher _metricCacher;
        private readonly CsvWriter _csvWriter;
        private readonly object _syncObject = new object();
        
        public TestResourceSnapshotWriter(string filename = null)
        {
            lock (_syncObject)
            {
                var cpuUsageCalculator = CpuHelper.GetOSCpuUsageCalculator();
                _metricCacher = new TestResourcesAnalyzerMetricCacher(cpuUsageCalculator);

                filename ??= $"TestResources_{DateTime.UtcNow:dd_MM_yyyy_HH_mm_ss}.csv";
                
                var file = File.OpenWrite(filename);

                file.Position = 0;
                file.SetLength(0);

                _csvWriter = new CsvWriter(new StreamWriter(file), CultureInfo.InvariantCulture);
                _csvWriter.WriteHeader(typeof(TestResourceSnapshot));
            }
        }

        public void WriteResourceSnapshot(TestStage testStage, ITestAssembly testAssembly)
        {
            var snapshot = GetTestResourceSnapshot(testStage, testAssembly.Assembly);
            Write(snapshot);
        }

        public void WriteResourceSnapshot(TestStage testStage, ITestClass testClass)
        {
            var snapshot = GetTestResourceSnapshot(testStage, testClass.Class.Assembly);
            snapshot.ClassName = testClass.Class.Name;
            
            Write(snapshot);
        }

        public void WriteResourceSnapshot(TestStage testStage, ITestMethod testMethod, TestResult? testResult = null)
        {
            var testClass = testMethod.TestClass.Class;

            var snapshot = GetTestResourceSnapshot(testStage, testClass.Assembly);
            
            snapshot.ClassName = testClass.Name;
            snapshot.MethodName = testMethod.Method.Name;
            snapshot.TestResult = testResult;
            
            Write(snapshot);
        }

        private void Write(TestResourceSnapshot snapshot)
        {
            lock (_syncObject)
            {
                _csvWriter.NextRecord();
                _csvWriter.WriteRecord(snapshot);
            }
        }

        private TestResourceSnapshot GetTestResourceSnapshot(TestStage testStage, IAssemblyInfo assemblyInfo)
        {
            var timeStamp = DateTime.UtcNow;
            var assemblyName = GetAssemblyShortName(assemblyInfo);
            
            var cpuUsage = _metricCacher.GetCpuUsage();
            var memoryInfo = _metricCacher.GetMemoryInfoExtended();
            var tcpConnections = TcpStatisticsProvider.GetConnections();

            var snapshot = new TestResourceSnapshot
            {
                TotalScratchAllocatedMemory = new Size(MemoryInformation.GetTotalScratchAllocatedMemory(), SizeUnit.Bytes).GetValue(SizeUnit.Megabytes),
                TotalDirtyMemory = new Size(MemoryInformation.GetDirtyMemoryState().TotalDirtyInBytes, SizeUnit.Bytes).GetValue(SizeUnit.Megabytes),
                IsHighDirty = MemoryInformation.GetDirtyMemoryState().IsHighDirty,
                TestStage = testStage,
                Timestamp = timeStamp.ToString("o"),
                AssemblyName = assemblyName,
                MachineCpuUsage = (long)cpuUsage.MachineCpuUsage,
                ProcessCpuUsage = (long)cpuUsage.ProcessCpuUsage,
                ProcessMemoryUsageInMb = memoryInfo.WorkingSet.GetValue(SizeUnit.Megabytes),
                TotalMemoryInMb = memoryInfo.TotalPhysicalMemory.GetValue(SizeUnit.Megabytes),
                TotalCommittableMemoryInMb = memoryInfo.TotalCommittableMemory.GetValue(SizeUnit.Megabytes),
                AvailableMemoryInMb = memoryInfo.AvailableMemory.GetValue(SizeUnit.Megabytes),
                CurrentCommitChargeInMb = memoryInfo.CurrentCommitCharge.GetValue(SizeUnit.Megabytes),
                SharedCleanMemoryInMb = memoryInfo.SharedCleanMemory.GetValue(SizeUnit.Megabytes),
                TotalScratchDirtyMemory = memoryInfo.TotalScratchDirtyMemory.GetValue(SizeUnit.Megabytes),
                CurrentIpv4Connections = tcpConnections.CurrentIpv4,
                CurrentIpv6Connections = tcpConnections.CurrentIpv6
            };

            return snapshot;
        }

        private static string GetAssemblyShortName(IAssemblyInfo assemblyInfo)
        {
            var assemblyName = new AssemblyName(assemblyInfo.Name);
            return assemblyName.Name;
        }

        public class TestResourceSnapshot
        {
            public TestStage TestStage { get; set; }
            
            public string AssemblyName { get; set; }
            
            public string ClassName { get; set; }
            
            public string MethodName { get; set; }

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
            
            public long CurrentIpv4Connections { get; set; }
            
            public long CurrentIpv6Connections { get; set; }
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
        TestExecution
    }

    public enum TestResult
    {
        Success,
        Fail,
        Skipped
    }
}
