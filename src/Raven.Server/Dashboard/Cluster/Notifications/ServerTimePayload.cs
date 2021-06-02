// -----------------------------------------------------------------------
//  <copyright file="ServerTimePayload.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using Sparrow.Json.Parsing;

namespace Raven.Server.Dashboard.Cluster.Notifications
{
    public class ServerTimePayload : AbstractClusterDashboardNotification
    {
        public override ClusterDashboardNotificationType Type => ClusterDashboardNotificationType.ServerTime;
        public override DynamicJsonValue ToJsonWithFilter(CanAccessDatabase filter)
        {
            return ToJson();
        }
    }
}
