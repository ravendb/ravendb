using System.Collections.Generic;
using System.Linq;
using Sparrow.Json.Parsing;

namespace Raven.Server.Dashboard
{
    public class DrivesUsage : AbstractDashboardNotification
    {
        public override DashboardNotificationType Type => DashboardNotificationType.DriveUsage;
        
        public List<MountPointUsage> Items { get; set; }

        public DrivesUsage()
        {
            Items = new List<MountPointUsage>();
        }

        public override DynamicJsonValue ToJson()
        {
            var json = base.ToJson();
            json[nameof(Items)] = new DynamicJsonArray(Items.Select(x => x.ToJson()));
            return json;
        }
    }
    
    public class MountPointUsage : IDynamicJson
    {
        public string MountPoint { get; set; }

        public string VolumeLabel { get; set; }

        public long TotalCapacity { get; set; }
        
        public long FreeSpace { get; set; }
        
        public bool IsLowSpace { get; set; }
        
        public List<DatabaseDiskUsage> Items { get; set; }
        
        public MountPointUsage()
        {
            Items = new List<DatabaseDiskUsage>();
        }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(MountPoint)] = MountPoint,
                [nameof(VolumeLabel)] = VolumeLabel,
                [nameof(TotalCapacity)] = TotalCapacity,
                [nameof(FreeSpace)] = FreeSpace,
                [nameof(IsLowSpace)] = IsLowSpace,
                [nameof(Items)] = new DynamicJsonArray(Items.Select(x => x.ToJson()))
            };
        }
    }
    
    public class DatabaseDiskUsage : IDynamicJson
    {
        public string Database { get; set; }
        
        public long Size { get; set; }
        
        public long TempBuffersSize { get; set; }
        
        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue()
            {
                [nameof(Database)] = Database,
                [nameof(Size)] = Size,
                [nameof(TempBuffersSize)] = TempBuffersSize
            };
        }
    }
}
