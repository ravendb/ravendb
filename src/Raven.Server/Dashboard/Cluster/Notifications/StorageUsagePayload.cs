// -----------------------------------------------------------------------
//  <copyright file="StoragePayload.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Collections.Generic;
using System.Linq;
using Sparrow.Json.Parsing;

namespace Raven.Server.Dashboard.Cluster.Notifications
{
    public class StorageUsagePayload : AbstractClusterDashboardNotification
    {
        public List<MountPointUsage> Items { get; set; }

        public override ClusterDashboardNotificationType Type => ClusterDashboardNotificationType.StorageUsage;

        public override DynamicJsonValue ToJson()
        {
            var json = base.ToJson();

            json[nameof(Items)] = new DynamicJsonArray(Items.Select(x =>
            {
                var usageAsJson = x.ToJson();

                usageAsJson.RemoveInMemoryPropertyByName(nameof(MountPointUsage.Items));

                return usageAsJson;
            }));

            return json;
        }

        public override DynamicJsonValue ToJsonWithFilter(CanAccessDatabase filter)
        {
            var items = new DynamicJsonArray();

            foreach (var mountPointUsage in Items)
            {
                var usageAsJson = mountPointUsage.ToJsonWithFilter(filter);
                if (usageAsJson != null)
                {
                    usageAsJson.RemoveInMemoryPropertyByName(nameof(MountPointUsage.Items));

                    items.Add(usageAsJson);
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
