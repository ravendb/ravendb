using System;
using Raven.Client.Util;
using Sparrow.Json.Parsing;

namespace Raven.Server.Dashboard
{
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
    }
}
