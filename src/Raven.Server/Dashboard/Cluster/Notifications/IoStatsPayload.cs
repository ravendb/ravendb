using System.Collections.Generic;
using System.Linq;
using Raven.Client.ServerWide.Operations;
using Sparrow.Json.Parsing;

namespace Raven.Server.Dashboard.Cluster.Notifications
{
    public sealed class IoStatsPayload : AbstractClusterDashboardNotification
    {
        public List<IoStatsResult> Items { get; set; }

        public override ClusterDashboardNotificationType Type => ClusterDashboardNotificationType.IoStats;

        public override DynamicJsonValue ToJson()
        {
            var json = base.ToJson();

            json[nameof(Items)] = new DynamicJsonArray(Items.Select(x => x.ToJson()));

            return json;
        }

        public override DynamicJsonValue ToJsonWithFilter(CanAccessDatabase filter)
        {
            return ToJson();
        }
    }
}