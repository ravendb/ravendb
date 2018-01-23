using Sparrow.Json.Parsing;

namespace Raven.Server.Dashboard
{
    public class MachineResources : AbstractDashboardNotification
    {
        public override DashboardNotificationType Type => DashboardNotificationType.MachineResources;
        
        public double MachineCpuUsage { get; set; } // 0 - 100

        public double ProcessCpuUsage { get; set; } // 0 - 100

        public long TotalMemory { get; set; } // in bytes

        public long MachineMemoryUsage { get; set; } // in bytes

        public long ProcessMemoryUsage { get; set; } // in bytes

        public long ProcessMemoryExcludingSharedUsage { get; set; } // in bytes

        public override DynamicJsonValue ToJson()
        {
            var json = base.ToJson();

            json[nameof(MachineCpuUsage)] = MachineCpuUsage;
            json[nameof(ProcessCpuUsage)] = ProcessCpuUsage;
            json[nameof(TotalMemory)] = TotalMemory;
            json[nameof(MachineMemoryUsage)] = MachineMemoryUsage;
            json[nameof(ProcessMemoryUsage)] = ProcessMemoryUsage;
            json[nameof(ProcessMemoryExcludingSharedUsage)] = ProcessMemoryExcludingSharedUsage;

            return json;
        }
    }
    
}
