using Sparrow.Json.Parsing;

namespace Raven.Server.Dashboard.Cluster
{
    public abstract class AbstractClusterDashboardNotification : IDynamicJson
    {
        public abstract ClusterDashboardNotificationType Type { get; }

        public virtual DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue { [nameof(Type)] = Type };
        }

        /// <summary>
        /// Convert object to DynamicJsonValue but using provided filter
        /// Return null when filter matches nothing - such message will be skipped
        /// </summary>
        public abstract DynamicJsonValue ToJsonWithFilter(CanAccessDatabase filter);
    }
}
