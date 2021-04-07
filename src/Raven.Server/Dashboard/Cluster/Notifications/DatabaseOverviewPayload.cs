using System.Collections.Generic;
using System.Linq;
using Sparrow.Json.Parsing;

namespace Raven.Server.Dashboard.Cluster.Notifications
{
    public class DatabaseOverviewPayload : AbstractClusterDashboardNotification
    {
        public override ClusterDashboardNotificationType Type => ClusterDashboardNotificationType.DatabaseOverview;

        public List<DatabaseInfoItem> Items { get; set; }

        public DatabaseOverviewPayload()
        {
            Items = new List<DatabaseInfoItem>();
        }

        public override DynamicJsonValue ToJson()
        {
            var json = base.ToJson();
            json[nameof(Items)] = new DynamicJsonArray(Items.Select(x => x.ToJson()));
            return json;
        }

        public override DynamicJsonValue ToJsonWithFilter(CanAccessDatabase filter)
        {
            var items = new DynamicJsonArray();
            foreach (var databaseInfoItem in Items)
            {
                if (filter(databaseInfoItem.Database, requiresWrite: false))
                {
                    items.Add(databaseInfoItem.ToJson());
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
