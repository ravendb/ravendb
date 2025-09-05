using Sparrow.Json.Parsing;

namespace Raven.Server.Dashboard.Cluster.Notifications
{
    public sealed class ServerTimePayload : AbstractClusterDashboardNotification
    {
        public override ClusterDashboardNotificationType Type => ClusterDashboardNotificationType.ServerTime;
        public override DynamicJsonValue ToJsonWithFilter(CanAccessDatabase filter)
        {
            return ToJson();
        }
    }
}