using System.Collections.Generic;
using System.Linq;
using Sparrow.Json.Parsing;

namespace Raven.Server.Dashboard.Cluster.Notifications
{
    public class DatabaseTrafficWatchPayload : AbstractClusterDashboardNotification
    {
        public override ClusterDashboardNotificationType Type => ClusterDashboardNotificationType.DatabaseTraffic;

        public List<TrafficWatchItem> Items { get; set; }

        public override DynamicJsonValue ToJson()
        {
            var json = base.ToJson();
            json[nameof(Items)] = new DynamicJsonArray(Items.Select(x => x.ToJson()));
            return json;
        }

        public override DynamicJsonValue ToJsonWithFilter(CanAccessDatabase filter)
        {
            var items = new DynamicJsonArray();

            foreach (var trafficWatchItem in Items)
            {
                if (filter(trafficWatchItem.Database, requiresWrite: false))
                {
                    items.Add(trafficWatchItem.ToJson());
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
