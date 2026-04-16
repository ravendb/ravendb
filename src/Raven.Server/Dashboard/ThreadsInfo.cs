using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using Raven.Client.Util;
using Sparrow.Json.Parsing;

namespace Raven.Server.Dashboard
{
    public sealed class ThreadsInfo : IDynamicJson
    {
        private readonly int? _take;

        public DateTime Date => SystemTime.UtcNow;

        public ISet<ThreadInfo> List { get; set; }

        public double CpuUsage { get; set; }
        
        public double ProcessCpuUsage { get; set; }

        public long ActiveCores { get; set; }

        public long ThreadsCount => List.Count;

        public int DedicatedThreadsCount { get; set; }

        public ThreadsInfo(int? take)
        {
            _take = take;
            List = new SortedSet<ThreadInfo>(new ThreadsInfoComparer());
        }

        public ThreadsInfo()
        {
            // for deserialization purposes
        }
        
        private sealed class ThreadsInfoComparer : IComparer<ThreadInfo>
        {
            public int Compare(ThreadInfo x, ThreadInfo y)
            {
                Debug.Assert(x != null && y != null);

                var compareByCpu = y.CpuUsage.CompareTo(x.CpuUsage);
                if (compareByCpu != 0)
                    return compareByCpu;

                int compareTo = y.TotalProcessorTime.CompareTo(x.TotalProcessorTime);
                if (compareTo != 0)
                    return compareTo;
                return y.Id.CompareTo(x.Id);
            }
        }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(Date)] = Date,
                [nameof(CpuUsage)] = CpuUsage,
                [nameof(ProcessCpuUsage)] = ProcessCpuUsage,
                [nameof(ActiveCores)] = ActiveCores,
                [nameof(ThreadsCount)] = ThreadsCount,
                [nameof(DedicatedThreadsCount)] = DedicatedThreadsCount,
                [nameof(List)] = new DynamicJsonArray(List.Take(_take ?? int.MaxValue).Select(x => x.ToJson()))
            };
        }
    }

    public sealed class IoStats : IDynamicJson
    {
        // Per-thread IO metrics (Linux only, populated when available)
        // Last measured I/O operations per second (syscr + syscw delta / interval)
        public double? IoSyscallsPerSecLast { get; set; }
        // Last measured throughput in KB/s based on read_bytes+write_bytes delta
        public double? ThroughputKbPerSecLast { get; set; }

        // Split read/write metrics
        public double? ReadIoSyscallsPerSecLast { get; set; }
        public double? WriteIoSyscallsPerSecLast { get; set; }
        public double? ReadThroughputKbPerSecLast { get; set; }
        public double? WriteThroughputKbPerSecLast { get; set; }

        // Raw cumulative values from /proc/self/task/{tid}/io
        // These are monotonically increasing counters for the lifetime of each thread.
        // The client uses these to compute totals as (current - initial_snapshot).
        public long? Syscr { get; set; }
        public long? Syscw { get; set; }
        public long? ReadBytes { get; set; }
        public long? WriteBytes { get; set; }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(IoSyscallsPerSecLast)] = IoSyscallsPerSecLast,
                [nameof(ThroughputKbPerSecLast)] = ThroughputKbPerSecLast,
                [nameof(ReadIoSyscallsPerSecLast)] = ReadIoSyscallsPerSecLast,
                [nameof(WriteIoSyscallsPerSecLast)] = WriteIoSyscallsPerSecLast,
                [nameof(ReadThroughputKbPerSecLast)] = ReadThroughputKbPerSecLast,
                [nameof(WriteThroughputKbPerSecLast)] = WriteThroughputKbPerSecLast,
                [nameof(Syscr)] = Syscr,
                [nameof(Syscw)] = Syscw,
                [nameof(ReadBytes)] = ReadBytes,
                [nameof(WriteBytes)] = WriteBytes
            };
        }
    }

    public sealed class ThreadInfo : IDynamicJson
    {
        public int Id { get; set; }

        public double CpuUsage { get; set; }

        public string Name { get; set; }

        public int? ManagedThreadId { get; set; }

        public long? UnmanagedAllocationsInBytes { get; set; }

        public DateTime? StartingTime { get; set; }

        public double Duration { get; set; }

        public TimeSpan TotalProcessorTime { get; set; }

        public TimeSpan PrivilegedProcessorTime { get; set; }

        public TimeSpan UserProcessorTime { get; set; }

        public ThreadState? State { get; set; }

        public ThreadPriorityLevel? Priority { get; set; }

        public ThreadWaitReason? WaitReason { get; set; }

        public IoStats IoStats { get; set; }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(Id)] = Id,
                [nameof(CpuUsage)] = CpuUsage,
                [nameof(Name)] = Name,
                [nameof(ManagedThreadId)] = ManagedThreadId,
                [nameof(UnmanagedAllocationsInBytes)] = UnmanagedAllocationsInBytes,
                [nameof(StartingTime)] = StartingTime,
                [nameof(Duration)] = Duration,
                [nameof(TotalProcessorTime)] = TotalProcessorTime,
                [nameof(PrivilegedProcessorTime)] = PrivilegedProcessorTime,
                [nameof(UserProcessorTime)] = UserProcessorTime,
                [nameof(State)] = State,
                [nameof(Priority)] = Priority,
                [nameof(WaitReason)] = WaitReason,
                [nameof(IoStats)] = IoStats?.ToJson()
            };
        }
    }
}
