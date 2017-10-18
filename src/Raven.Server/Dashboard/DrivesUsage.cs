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
        
        public long TotalCapacity { get; set; }
        
        public long FreeSpace { get; set; }
        
        public FreeSpaceLevel FreeSpaceLevel { get; set; }
        
        public List<DatabaseDiskUsage> Items { get; set; }
        
        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(MountPoint)] = MountPoint,
                [nameof(TotalCapacity)] = TotalCapacity,
                [nameof(FreeSpace)] = FreeSpace,
                [nameof(FreeSpaceLevel)] = FreeSpaceLevel,
                [nameof(Items)] = new DynamicJsonArray(Items.Select(x => x.ToJson()))
            };
        }
    }
    
    /**
     * High - we have a lot of disk space
     * Medium - warn user about free space
     * Low - we almost run out of empty space
     */
    public enum FreeSpaceLevel
    {
        High, 
        Medium, 
        Low 
    }
    
    public class DatabaseDiskUsage : IDynamicJson
    {
        public string Database { get; set; }
        
        public long Size { get; set; }
        
        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue()
            {
                [nameof(Database)] = Database,
                [nameof(Size)] = Size,
            };
        }
    }
}
