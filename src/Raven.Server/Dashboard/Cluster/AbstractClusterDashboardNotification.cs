using System;
using Raven.Client.Util;
using Sparrow.Json.Parsing;

namespace Raven.Server.Dashboard.Cluster
{
    public abstract class AbstractClusterDashboardNotification : IDynamicJson
    {
        public abstract ClusterDashboardNotificationType Type { get; }

        public DateTime Date { get; } = SystemTime.UtcNow;

        public virtual DynamicJsonValue ToJson()
        {
            return new()
            {
                [nameof(Type)] = Type,
                [nameof(Date)] = Date
            };
            
        }

        /// <summary>
        /// Convert object to DynamicJsonValue but using provided filter
        /// Return null when filter matches nothing - such message will be skipped
        /// </summary>
        public abstract DynamicJsonValue ToJsonWithFilter(CanAccessDatabase filter);
    }
}
