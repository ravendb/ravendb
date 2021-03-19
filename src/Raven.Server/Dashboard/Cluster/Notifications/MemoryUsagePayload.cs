// -----------------------------------------------------------------------
//  <copyright file="MemoryUsagePayload.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using Sparrow.Json.Parsing;
using Sparrow.LowMemory;

namespace Raven.Server.Dashboard.Cluster.Notifications
{
    public class MemoryUsagePayload : AbstractClusterDashboardNotification
    {
        public DateTime Time { get; set; }
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

        public override ClusterDashboardNotificationType Type => ClusterDashboardNotificationType.MemoryUsage;

        public override DynamicJsonValue ToJson()
        {
            var json = base.ToJson();

            json[nameof(Time)] = Time;
            json[nameof(LowMemorySeverity)] = LowMemorySeverity;
            json[nameof(PhysicalMemory)] = PhysicalMemory;
            json[nameof(WorkingSet)] = WorkingSet;
            json[nameof(ManagedAllocations)] = ManagedAllocations;
            json[nameof(UnmanagedAllocations)] = UnmanagedAllocations;
            json[nameof(SystemCommitLimit)] = SystemCommitLimit;
            json[nameof(EncryptionBuffersInUse)] = EncryptionBuffersInUse;
            json[nameof(EncryptionBuffersPool)] = EncryptionBuffersPool;
            json[nameof(MemoryMapped)] = MemoryMapped;
            json[nameof(DirtyMemory)] = DirtyMemory;
            json[nameof(AvailableMemory)] = AvailableMemory;
            json[nameof(AvailableMemoryForProcessing)] = AvailableMemoryForProcessing;

            return json;
        }

        public override DynamicJsonValue ToJsonWithFilter(CanAccessDatabase filter)
        {
            throw new NotImplementedException();
        }
    }
}
