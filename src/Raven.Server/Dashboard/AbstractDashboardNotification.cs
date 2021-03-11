using System;
using Raven.Client.Util;
using Sparrow.Json.Parsing;

namespace Raven.Server.Dashboard
{
    public delegate bool CanAccessDatabase(string databaseName, bool requiresWrite);

    public abstract class AbstractDashboardNotification : IDynamicJson
    {
        public abstract DashboardNotificationType Type { get; }

        public DateTime Date => SystemTime.UtcNow;

        public virtual DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
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
