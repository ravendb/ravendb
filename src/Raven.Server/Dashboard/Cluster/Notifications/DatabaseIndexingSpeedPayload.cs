using System.Collections.Generic;
using System.Linq;
using Sparrow.Json.Parsing;

namespace Raven.Server.Dashboard.Cluster.Notifications
{
    public class DatabaseIndexingSpeedPayload : AbstractClusterDashboardNotification
    {
        public override ClusterDashboardNotificationType Type => ClusterDashboardNotificationType.DatabaseIndexing;

        public List<IndexingSpeedItem> Items { get; set; }

        public override DynamicJsonValue ToJson()
        {
            var json = base.ToJson();
            json[nameof(Items)] = new DynamicJsonArray(Items.Select(x => x.ToJson()));
            return json;
        }

        public override DynamicJsonValue ToJsonWithFilter(CanAccessDatabase filter)
        {
            var items = new DynamicJsonArray();
            foreach (var indexingSpeedItem in Items)
            {
                if (filter(indexingSpeedItem.Database, requiresWrite: false))
                {
                    items.Add(indexingSpeedItem.ToJson());
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
