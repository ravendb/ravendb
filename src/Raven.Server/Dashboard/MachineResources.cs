using System;
using Sparrow.Json.Parsing;
using Sparrow.LowMemory;

namespace Raven.Server.Dashboard
{
    public class MachineResources : AbstractDashboardNotification
    {
        public override DashboardNotificationType Type => DashboardNotificationType.MachineResources;

        public double MachineCpuUsage { get; set; } // 0 - 100

        public double ProcessCpuUsage { get; set; } // 0 - 100

        public long TotalMemory { get; set; } // in bytes

        public long AvailableMemory { get; set; } // in bytes

        public long AvailableMemoryForProcessing { get; set; } // in bytes

        public long SystemCommitLimit { get; set; } // in bytes

        public long CommittedMemory { get; set; } // in bytes

        public long ProcessMemoryUsage { get; set; } // in bytes

        public bool IsWindows { get; set; }

        public LowMemorySeverity LowMemorySeverity { get; set; }

        public long LowMemoryThreshold { get; set; } // in bytes

        public long CommitChargeThreshold { get; set; } // in bytes

        public override DynamicJsonValue ToJson()
        {
            var json = base.ToJson();

            json[nameof(MachineCpuUsage)] = MachineCpuUsage;
            json[nameof(ProcessCpuUsage)] = ProcessCpuUsage;
            json[nameof(TotalMemory)] = TotalMemory;
            json[nameof(AvailableMemory)] = AvailableMemory;
            json[nameof(AvailableMemoryForProcessing)] = AvailableMemoryForProcessing;
            json[nameof(SystemCommitLimit)] = SystemCommitLimit;
            json[nameof(CommittedMemory)] = CommittedMemory;
            json[nameof(ProcessMemoryUsage)] = ProcessMemoryUsage;
            json[nameof(LowMemorySeverity)] = LowMemorySeverity;
            json[nameof(IsWindows)] = IsWindows;
            json[nameof(LowMemoryThreshold)] = LowMemoryThreshold;
            json[nameof(CommitChargeThreshold)] = CommitChargeThreshold;

            return json;
        }

        public override DynamicJsonValue ToJsonWithFilter(CanAccessDatabase filter)
        {
            // nothing to filter
            return ToJson();
        }
    }
}
