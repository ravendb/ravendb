using Sparrow.LowMemory;

namespace Raven.Server.Dashboard
{
    public class MachineResources : AbstractDashboardNotification
    {
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
    }
}
