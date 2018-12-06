using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using Sparrow.Json.Parsing;

namespace Raven.Server.Dashboard
{
    public class ThreadsInfo : AbstractDashboardNotification
    {
        public override DashboardNotificationType Type => DashboardNotificationType.ThreadsInfo;

        public List<ThreadInfo> List { get; }

        public double CpuUsage { get; set; }

        public long ActiveCores { get; set; }

        public ThreadsInfo()
        {
            List = new List<ThreadInfo>();
        }

        public override DynamicJsonValue ToJson()
        {
            var json = base.ToJson();
            json[nameof(List)] = new DynamicJsonArray(List.Select(x => x.ToJson()));
            json[nameof(CpuUsage)] = CpuUsage;
            json[nameof(ActiveCores)] = ActiveCores;
            return json;
        }
    }

    public class ThreadInfo : IDynamicJson, IComparable<ThreadInfo>
    {
        public int Id { get; set; }

        public double CpuUsage { get; set; }

        public string Name { get; set; }

        public int? ManagedThreadId { get; set; }

        public DateTime? StartingTime { get; set; }

        public ThreadState? State { get; set; }

        public ThreadPriorityLevel? Priority { get; set; }

        public ThreadWaitReason? ThreadWaitReason { get; set; }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(Id)] = Id,
                [nameof(CpuUsage)] = CpuUsage,
                [nameof(Name)] = Name,
                [nameof(ManagedThreadId)] = ManagedThreadId,
                [nameof(StartingTime)] = StartingTime,
                [nameof(State)] = State,
                [nameof(Priority)] = Priority,
                [nameof(ThreadWaitReason)] = ThreadWaitReason
            };
        }

        public int CompareTo(ThreadInfo other)
        {
            return other.CpuUsage.CompareTo(CpuUsage);
        }
    }
}
