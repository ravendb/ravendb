using System.Collections.Generic;
using System.Linq;
using Sparrow.Json.Parsing;

namespace Raven.Server.Dashboard
{
    public class MachineResources : AbstractDashboardNotification
    {
        public override DashboardNotificationType Type => DashboardNotificationType.MachineResources;
        
        public double CpuUsage { get; set; } // 0 - 100
        
        public long MemoryUsage { get; set; } // in bytes
        
        public long TotalMemory { get; set; } // in bytes
        
        public override DynamicJsonValue ToJson()
        {
            var json = base.ToJson();
            json[nameof(CpuUsage)] = CpuUsage;
            json[nameof(MemoryUsage)] = MemoryUsage;
            json[nameof(TotalMemory)] = TotalMemory;
            return json;
        }
    }
    
}
