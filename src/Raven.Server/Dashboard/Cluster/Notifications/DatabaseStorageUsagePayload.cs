using System.Collections.Generic;
using System.Linq;
using Sparrow.Json.Parsing;

namespace Raven.Server.Dashboard.Cluster.Notifications
{
    public class DatabaseStorageUsagePayload : AbstractClusterDashboardNotification
    {
        public List<DatabaseDiskUsage> Items { get; set; }

        public override ClusterDashboardNotificationType Type => ClusterDashboardNotificationType.DatabaseStorageUsage;

        public override DynamicJsonValue ToJson()
        {
            var json = base.ToJson();

            json[nameof(Items)] = new DynamicJsonArray(Items.Select(x => x.ToJson()));

            return json;
        }

        public override DynamicJsonValue ToJsonWithFilter(CanAccessDatabase filter)
        {
            var items = new DynamicJsonArray();

            foreach (var databaseDiskUsage in Items)
            {
                if (filter(databaseDiskUsage.Database, requiresWrite: false))
                {
                    items.Add(databaseDiskUsage.ToJson());
                }
            }

            if (items.Count == 0)
                return null;

            var json = base.ToJson();
            json[nameof(Items)] = items;

            return json;
        }
    }
}
