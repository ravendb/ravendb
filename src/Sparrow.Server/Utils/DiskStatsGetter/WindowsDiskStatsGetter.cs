using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Runtime.Versioning;
using Sparrow.Logging;

namespace Sparrow.Server.Utils.DiskStatsGetter;

[SupportedOSPlatform("windows")]
internal class WindowsDiskStatsGetter : DiskStatsGetter<WindowsDiskStatsRawResult>
{
    private static readonly Logger Logger = LoggingSource.Instance.GetLogger<WindowsDiskStatsGetter>("Server");
    
    private readonly CountersPerDisk _countersPerDisk = new CountersPerDisk();
        
    public WindowsDiskStatsGetter(TimeSpan minInterval) : base(minInterval)
    {
    }
        
    protected override DiskStatsResult CalculateStats(WindowsDiskStatsRawResult currentInfo, State state)
    {
        return new DiskStatsResult
        {
            IoReadOperations = ComputeCounterValue(state.Prev.Raw.IoReadOperations, currentInfo.IoReadOperations), 
            IoWriteOperations = ComputeCounterValue(state.Prev.Raw.IoWriteOperations, currentInfo.IoWriteOperations), 
            ReadThroughput = new Size((long)ComputeCounterValue(state.Prev.Raw.ReadThroughput, currentInfo.ReadThroughput), SizeUnit.Bytes), 
            WriteThroughput = new Size((long)ComputeCounterValue(state.Prev.Raw.WriteThroughput, currentInfo.WriteThroughput), SizeUnit.Bytes), 
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
            : diffValue > 0 ? double.PositiveInfinity : double.NegativeInfinity;
    }
        

    protected override WindowsDiskStatsRawResult GetDiskInfo(string drive)
    {
        try
        {
            var counters = _countersPerDisk.Get(drive);
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
            if(Logger.IsInfoEnabled)
                Logger.Info($"Could not get GetDiskInfo for {drive}", e);
            return null;
        }
    }
        
    private class DiskCounters
    {
        public DiskCounters(string drive)
        {
            ReadIOCounter = new PerformanceCounter("PhysicalDisk", "Disk Reads/sec", drive);
            WriteIOCounter = new PerformanceCounter("PhysicalDisk", "Disk Writes/sec", drive);
            ReadThroughput = new PerformanceCounter("PhysicalDisk", "Disk Read Bytes/sec", drive);
            WriteThroughput = new PerformanceCounter("PhysicalDisk", "Disk Write Bytes/sec", drive);
            DiskQueue = new PerformanceCounter("PhysicalDisk", "Current Disk Queue Length", drive);
        }

        public PerformanceCounter ReadIOCounter { get; }
        public PerformanceCounter WriteIOCounter { get; }
        public PerformanceCounter ReadThroughput { get; }
        public PerformanceCounter WriteThroughput { get; }
        public PerformanceCounter DiskQueue { get; }
    }
        
    private class CountersPerDisk
    {
        private readonly PerformanceCounterCategory _category = new PerformanceCounterCategory("PhysicalDisk");
        private readonly ConcurrentDictionary<string, DiskCounters> _countersPerDisk = new ConcurrentDictionary<string, DiskCounters>();
        public DiskCounters Get(string path)
        {
            var drive = DiskUtils.WindowsGetDriveName(path, out _);
            if (_countersPerDisk.TryGetValue(drive, out var counter) == false)
            {
                foreach (string name in _category.GetInstanceNames())
                {
                    if (name.AsSpan().EndsWith(drive.AsSpan(0, drive.Length - 1), StringComparison.OrdinalIgnoreCase) == false)
                        continue;

                    counter = _countersPerDisk[path] = new DiskCounters(name);
                    break;
                }
            }

            return counter;
        }
    }
}
