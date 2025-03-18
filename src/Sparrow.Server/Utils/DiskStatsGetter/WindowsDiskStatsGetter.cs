using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Runtime.Versioning;
using Sparrow.Logging;
using Sparrow.Server.Logging;

namespace Sparrow.Server.Utils.DiskStatsGetter;

[SupportedOSPlatform("windows")]
internal class WindowsDiskStatsGetter : DiskStatsGetter<WindowsDiskStatsRawResult>
{
    private static readonly RavenLogger Logger = RavenLogManager.Instance.GetLoggerForSparrowServer(typeof(WindowsDiskStatsGetter));

    private const string DiskCategory = "LogicalDisk";

    private readonly CountersPerDisk _countersPerDisk = new CountersPerDisk();

    public WindowsDiskStatsGetter(TimeSpan minInterval) : base(minInterval)
    {
    }

    protected override DiskStatsResult CalculateStats(WindowsDiskStatsRawResult currentInfo, State state)
    {
        return new DiskStatsResult
        {
            IoReadOperations = ComputeCounterValue(state.Result.RawSampling.IoReadOperations, currentInfo.IoReadOperations),
            IoWriteOperations = ComputeCounterValue(state.Result.RawSampling.IoWriteOperations, currentInfo.IoWriteOperations),
            ReadThroughput = new Size((long)ComputeCounterValue(state.Result.RawSampling.ReadThroughput, currentInfo.ReadThroughput), SizeUnit.Bytes),
            WriteThroughput = new Size((long)ComputeCounterValue(state.Result.RawSampling.WriteThroughput, currentInfo.WriteThroughput), SizeUnit.Bytes),
            QueueLength = currentInfo.QueueLength.RawValue
        };
    }

    //It should return the equivalent result like CounterSampleCalculator.ComputeCounterValue
    private static double ComputeCounterValue(CounterSample oldSample, CounterSample newSample)
    {
        var diffTime = newSample.TimeStamp - oldSample.TimeStamp;
        var diffValue = newSample.RawValue - oldSample.RawValue;
        return diffTime != 0
            ? diffValue / (diffTime / 10000000.0)
            : diffValue > 0
                ? double.PositiveInfinity
                : double.NegativeInfinity;
    }

    protected override WindowsDiskStatsRawResult GetDiskInfo(string path)
    {
        try
        {
            var counters = _countersPerDisk.Get(path);
            if (counters == null)
                return null;

            return new WindowsDiskStatsRawResult
            {
                IoReadOperations = counters.ReadIOCounter.NextSample(),
                IoWriteOperations = counters.WriteIOCounter.NextSample(),
                ReadThroughput = counters.ReadThroughput.NextSample(),
                WriteThroughput = counters.WriteThroughput.NextSample(),
                QueueLength = counters.DiskQueue.NextSample(),
                Time = DateTime.UtcNow
            };
        }
        catch (Exception e)
        {
            if (Logger.IsWarnEnabled)
                Logger.Warn($"Could not get GetDiskInfo for {path}", e);
            return null;
        }
    }

    public override void Dispose() => _countersPerDisk.Dispose();

    private class DiskCounters : IDisposable
    {
        public DiskCounters(string drive)
        {
            ReadIOCounter = new PerformanceCounter(DiskCategory, "Disk Reads/sec", drive);
            WriteIOCounter = new PerformanceCounter(DiskCategory, "Disk Writes/sec", drive);
            ReadThroughput = new PerformanceCounter(DiskCategory, "Disk Read Bytes/sec", drive);
            WriteThroughput = new PerformanceCounter(DiskCategory, "Disk Write Bytes/sec", drive);
            DiskQueue = new PerformanceCounter(DiskCategory, "Current Disk Queue Length", drive);
        }

        public PerformanceCounter ReadIOCounter { get; }
        public PerformanceCounter WriteIOCounter { get; }
        public PerformanceCounter ReadThroughput { get; }
        public PerformanceCounter WriteThroughput { get; }
        public PerformanceCounter DiskQueue { get; }

        public void Dispose()
        {
            ReadIOCounter?.Dispose();
            WriteIOCounter?.Dispose();
            ReadThroughput?.Dispose();
            WriteThroughput?.Dispose();
            DiskQueue?.Dispose();
        }
    }

    private class CountersPerDisk : IDisposable
    {
        private readonly PerformanceCounterCategory _category = new PerformanceCounterCategory(DiskCategory);
        private readonly ConcurrentDictionary<string, DiskCounters> _countersPerDisk = new ConcurrentDictionary<string, DiskCounters>();

        public DiskCounters Get(string path)
        {
            var drive = DiskUtils.WindowsGetDriveName(path, out _);
            if (_countersPerDisk.TryGetValue(drive, out var counter) == false)
            {
                foreach (string name in _category.GetInstanceNames())
                {
                    //The return value from GetInstanceNames for example can be "C:" while the return value from WindowsGetDriveName is "C:\"
                    if (drive.StartsWith(name, StringComparison.OrdinalIgnoreCase) == false)
                        continue;

                    if (Logger.IsDebugEnabled)
                        Logger.Debug($"{nameof(DiskCounters)} was created for \"{drive}\" (requested for path \"{path}\").");

                    counter = _countersPerDisk[path] = new DiskCounters(name);
                    break;
                }

                if (counter == null)
                {
                    if (Logger.IsWarnEnabled)
                        Logger.Warn($"Couldn't find instance in {DiskCategory} for \"{drive}\" (requested for path \"{path}\").");

                    _countersPerDisk[path] = null;
                }
            }

            return counter;
        }

        public void Dispose()
        {
            foreach (var (_, value) in _countersPerDisk)
            {
                value.Dispose();
            }
        }
    }
}
