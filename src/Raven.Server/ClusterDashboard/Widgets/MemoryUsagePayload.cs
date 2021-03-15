// -----------------------------------------------------------------------
//  <copyright file="MemoryUsagePayload.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using Sparrow.Json.Parsing;
using Sparrow.LowMemory;

namespace Raven.Server.ClusterDashboard.Widgets
{
    public class MemoryUsagePayload : IDynamicJson
    {
        public LowMemorySeverity LowMemorySeverity { get; set; }
        public long PhysicalMemory { get; set; }
        public long WorkingSet { get; set; }
        public long ManagedAllocations { get; set; }
        public long UnmanagedAllocations { get; set; }
        public long SystemCommitLimit { get; set; }
        public long EncryptionBuffersInUse { get; set; }
        public long EncryptionBuffersPool { get; set; }
        public long MemoryMapped { get; set; }
        public long DirtyMemory { get; set; }
        public long AvailableMemory { get; set; }
        public long AvailableMemoryForProcessing { get; set; }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(LowMemorySeverity)] = LowMemorySeverity,
                [nameof(PhysicalMemory)] = PhysicalMemory,
                [nameof(WorkingSet)] = WorkingSet,
                [nameof(ManagedAllocations)] = ManagedAllocations,
                [nameof(UnmanagedAllocations)] = UnmanagedAllocations,
                [nameof(SystemCommitLimit)] = SystemCommitLimit,
                [nameof(EncryptionBuffersInUse)] = EncryptionBuffersInUse,
                [nameof(EncryptionBuffersPool)] = EncryptionBuffersPool,
                [nameof(MemoryMapped)] = MemoryMapped,
                [nameof(DirtyMemory)] = DirtyMemory,
                [nameof(AvailableMemory)] = AvailableMemory,
                [nameof(AvailableMemoryForProcessing)] = AvailableMemoryForProcessing,
            };
        }
    }
}
