// -----------------------------------------------------------------------
//  <copyright file="StoragePayload.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Collections.Generic;
using System.Linq;
using Sparrow.Json.Parsing;

namespace Raven.Server.ClusterDashboard.Widgets
{
    public class StoragePayload : IDynamicJson
    {
        public List<StorageMountPointPayload> Items { get; set; } = new();

        public DynamicJsonValue ToJson()
        {
            return new()
            {
                [nameof(Items)] = new DynamicJsonArray(Items.Select(x => x.ToJson()))
            };
        }
    }

    public class StorageMountPointPayload : IDynamicJson
    {
        public string MountPoint { get; set; }

        public string VolumeLabel { get; set; }

        public long TotalCapacity { get; set; }

        public long FreeSpace { get; set; }

        public bool IsLowSpace { get; set; }
        
        public long RavenSize { get; set; }

        public DynamicJsonValue ToJson()
        {
            return new()
            {
                [nameof(MountPoint)] = MountPoint,
                [nameof(VolumeLabel)] = VolumeLabel,
                [nameof(TotalCapacity)] = TotalCapacity,
                [nameof(FreeSpace)] = FreeSpace,
                [nameof(IsLowSpace)] = IsLowSpace,
                [nameof(RavenSize)] = RavenSize
            };
        }
    }
}
