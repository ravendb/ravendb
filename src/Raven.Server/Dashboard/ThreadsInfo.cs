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

        public SortedSet<ThreadInfo> List { get; }

        public double CpuUsage { get; set; }

        public long ActiveCores { get; set; }

        public long ThreadsCount => List.Count;

        public ThreadsInfo()
        {
            List = new SortedSet<ThreadInfo>(new ThreadsInfoComparer());
        }

        private class ThreadsInfoComparer : IComparer<ThreadInfo>
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
                [nameof(ActiveCores)] = ActiveCores,
                [nameof(ThreadsCount)] = ThreadsCount,
                [nameof(List)] = new DynamicJsonArray(List.Select(x => x.ToJson()))
            };
        }
    }

    public class ThreadInfo : IDynamicJson
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
    }
}
