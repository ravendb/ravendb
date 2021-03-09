// -----------------------------------------------------------------------
//  <copyright file="MemoryBasicUsagePayload.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using Sparrow.Json.Parsing;
using Sparrow.LowMemory;

namespace Raven.Server.ClusterDashboard.Widgets
{
    public class MemoryBasicUsagePayload : IDynamicJson
    {
        public LowMemorySeverity LowMemorySeverity { get; set; }
        public long WorkingSet { get; set; }
        public long AvailableMemory { get; set; }
        public long PhysicalMemory { get; set; }
      
        public MemoryUsageType Type => MemoryUsageType.Basic;

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(LowMemorySeverity)] = LowMemorySeverity,
                [nameof(PhysicalMemory)] = PhysicalMemory,
                [nameof(WorkingSet)] = WorkingSet,
                [nameof(AvailableMemory)] = AvailableMemory,
                [nameof(Type)] = Type
            };
        }
    }
}
