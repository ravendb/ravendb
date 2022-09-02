// -----------------------------------------------------------------------
//  <copyright file="StoragePayload.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Collections.Generic;
using System.Linq;
using Raven.Client.ServerWide.Operations;
using Sparrow.Json.Parsing;

namespace Raven.Server.Dashboard.Cluster.Notifications
{
    public class IoStatsPayload : AbstractClusterDashboardNotification
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
