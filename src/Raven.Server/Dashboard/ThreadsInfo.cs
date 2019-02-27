using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using Raven.Client.Util;
using Sparrow.Json.Parsing;

namespace Raven.Server.Dashboard
{
    public class ThreadsInfo : IDynamicJson
    {
        public DateTime Date => SystemTime.UtcNow;

        public List<ThreadInfo> List { get; }

        public double CpuUsage { get; set; }

        public long ActiveCores { get; set; }

        public ThreadsInfo()
        {
            List = new List<ThreadInfo>();
        }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(Date)] = Date,
                [nameof(List)] = new DynamicJsonArray(List.Select(x => x.ToJson())),
                [nameof(CpuUsage)] = CpuUsage,
                [nameof(ActiveCores)] = ActiveCores
            };
        }
    }

    public class ThreadInfo : IDynamicJson, IComparable<ThreadInfo>
    {
        public int Id { get; set; }

        public double CpuUsage { get; set; }

        public string Name { get; set; }

        public int? ManagedThreadId { get; set; }

        public DateTime? StartingTime { get; set; }

        public double Duration { get; set; }

        public TimeSpan TotalProcessorTime { get; set; }

        public TimeSpan PrivilegedProcessorTime { get; set; }

        public TimeSpan UserProcessorTime { get; set; }

        public ThreadState? State { get; set; }

        public ThreadPriorityLevel? Priority { get; set; }

        public ThreadWaitReason? WaitReason { get; set; }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(Id)] = Id,
                [nameof(CpuUsage)] = CpuUsage,
                [nameof(Name)] = Name,
                [nameof(ManagedThreadId)] = ManagedThreadId,
                [nameof(StartingTime)] = StartingTime,
                [nameof(Duration)] = Duration,
                [nameof(TotalProcessorTime)] = TotalProcessorTime,
                [nameof(PrivilegedProcessorTime)] = PrivilegedProcessorTime,
                [nameof(UserProcessorTime)] = UserProcessorTime,
                [nameof(State)] = State,
                [nameof(Priority)] = Priority,
                [nameof(WaitReason)] = WaitReason
            };
        }

        public int CompareTo(ThreadInfo other)
        {
            var compareByCpu = other.CpuUsage.CompareTo(CpuUsage);
            if (compareByCpu != 0)
                return compareByCpu;

            return other.TotalProcessorTime.CompareTo(TotalProcessorTime);
        }
    }
}
