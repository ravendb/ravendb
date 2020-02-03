using System;
using System.Collections.Concurrent;
using System.IO;
using System.Threading;
using CsvHelper;
using Microsoft.CodeAnalysis.Operations;
using Raven.Server.Utils;
using Raven.Server.Utils.Cpu;
using Sparrow;
using Sparrow.LowMemory;
using Sparrow.Platform;
using Sparrow.Platform.Posix;
using Xunit.Abstractions;
using XunitLogger;

namespace Tests.Infrastructure
{
    public static class TestResourcesAnalyzer
    {
        private static readonly ConcurrentQueue<TestResources> _resources = new ConcurrentQueue<TestResources>();
        private static readonly Timer _timer;
        private static readonly bool _enabled;
        private static readonly ICpuUsageCalculator _cpuUsageCalculator;
        private static readonly TestResourcesAnalyzerMetricCacher _metricCacher;
        private static CsvWriter _csvWriter;

        static TestResourcesAnalyzer()
        {
            _enabled = bool.TryParse(Environment.GetEnvironmentVariable("TEST_RESOURCE_ANALYZER_ENABLE"), out var value) && value;
            if (_enabled == false)
                return;

            _cpuUsageCalculator = CpuHelper.GetOSCpuUsageCalculator();
            _metricCacher = new TestResourcesAnalyzerMetricCacher(_cpuUsageCalculator);
            _timer = new Timer(ProcessQueue, null, TimeSpan.FromSeconds(15), TimeSpan.FromSeconds(15));
        }

        private static string GetDisplayName(ITestMethod testMethod)
            => $"{testMethod.TestClass.Class.Name}::{testMethod.Method.Name}()";

        internal static void Start(ITestMethod testMethod)
        {
            if (_enabled)
                _resources.Enqueue(TestResources.Create(GetDisplayName(testMethod), nameof(Start)));
        }

        internal static void End(ITestMethod testMethod)
        {
            if (_enabled)
                _resources.Enqueue(TestResources.Create(GetDisplayName(testMethod), nameof(End)));
        }

        internal static void Start(Context context)
        {
            if (_enabled)
                _resources.Enqueue(TestResources.Create(context, nameof(Start)));
        }

        internal static void End(Context context)
        {
            if (_enabled)
                _resources.Enqueue(TestResources.Create(context, nameof(End)));
        }

        internal static void Complete()
        {
            if (_enabled == false)
                return;

            try
            {
                _timer?.Dispose();
            }
            catch
            {
            }

            try
            {
                using (_csvWriter)
                    ProcessQueue(null);
            }
            catch
            {
            }
        }

        private static void ProcessQueue(object state)
        {
            lock (_cpuUsageCalculator)
            {
                if (_csvWriter == null)
                {
                    var file = File.OpenWrite($"TestResources_{DateTime.Now.ToString("dd_MM_yyyy_HH_mm_ss")}.csv");
                    file.Position = 0;
                    file.SetLength(0);

                    _csvWriter = new CsvWriter(new StreamWriter(file));
                    _csvWriter.WriteHeader(typeof(TestResources));
                }

                var anyWork = false;
                while (_resources.TryDequeue(out var resource))
                {
                    anyWork = true;
                    _csvWriter.NextRecord();
                    _csvWriter.WriteRecord(resource);
                }

                if (anyWork)
                    _csvWriter.Flush();
            }
        }

        private class TestResources
        {
            public string ResourceName { get; private set; }
            public string Comment { get; private set; }
            public string TimeStamp { get; private set; }
            public long MachineCpuUsage { get; private set; }
            public long ProcessCpuUsage { get; private set; }
            public long ProcessMemoryUsageInMb { get; private set; }
            public long TotalMemoryInMb { get; private set; }
            public long AvailableMemoryInMb { get; private set; }
            public long TotalCommittableMemoryInMb { get; private set; }
            public long CurrentCommitChargeInMb { get; private set; }
            public long SharedCleanMemoryInMb { get; private set; }

            public static TestResources Create(Context context, string comment) => 
                Create(context.UniqueTestName, comment);

            public static TestResources Create(string resourceName, string comment, bool isFailed = false)
            {
                var timeStamp = DateTime.Now;
                var cpuUsage = _metricCacher.GetValue(MetricCacher.Keys.Server.CpuUsage, _cpuUsageCalculator.Calculate);
                var memoryInfo = _metricCacher.GetValue<MemoryInfoResult>(MetricCacher.Keys.Server.MemoryInfoExtended);

                return new TestResources
                {
                    ResourceName = resourceName,
                    TimeStamp = timeStamp.ToString("o"),
                    Comment = comment,
                    MachineCpuUsage = (long)cpuUsage.MachineCpuUsage,
                    ProcessCpuUsage = (long)cpuUsage.ProcessCpuUsage,
                    ProcessMemoryUsageInMb = memoryInfo.WorkingSet.GetValue(SizeUnit.Megabytes),
                    TotalMemoryInMb = memoryInfo.TotalPhysicalMemory.GetValue(SizeUnit.Megabytes),
                    TotalCommittableMemoryInMb = memoryInfo.TotalCommittableMemory.GetValue(SizeUnit.Megabytes),
                    AvailableMemoryInMb = memoryInfo.AvailableMemory.GetValue(SizeUnit.Megabytes),
                    CurrentCommitChargeInMb = memoryInfo.CurrentCommitCharge.GetValue(SizeUnit.Megabytes),
                    SharedCleanMemoryInMb = memoryInfo.SharedCleanMemory.GetValue(SizeUnit.Megabytes)
                };
            }
        }

        private class TestResourcesAnalyzerMetricCacher : MetricCacher
        {
            private readonly SmapsReader _smapsReader;

            public TestResourcesAnalyzerMetricCacher(ICpuUsageCalculator cpuUsageCalculator)
            {
                if (PlatformDetails.RunningOnLinux)
                    _smapsReader = new SmapsReader(new[] { new byte[SmapsReader.BufferSize], new byte[SmapsReader.BufferSize] });

                Register(MetricCacher.Keys.Server.CpuUsage, TimeSpan.FromSeconds(1), cpuUsageCalculator.Calculate);
                Register(MetricCacher.Keys.Server.MemoryInfo, TimeSpan.FromSeconds(1), CalculateMemoryInfo);
                Register(MetricCacher.Keys.Server.MemoryInfoExtended, TimeSpan.FromSeconds(1), CalculateMemoryInfoExtended);
            }

            private object CalculateMemoryInfo()
            {
                return MemoryInformation.GetMemoryInfo();
            }

            private object CalculateMemoryInfoExtended()
            {
                return MemoryInformation.GetMemoryInfo(_smapsReader, extended: true);
            }
        }
    }
}
