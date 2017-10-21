using System;
using Sparrow.Json.Parsing;

namespace Raven.Server.Dashboard
{
    public class ServerInfo : AbstractDashboardNotification
    {
        public override DashboardNotificationType Type => DashboardNotificationType.ServerInfo;

        public DateTime StartUpTime { get; set; }

        public override DynamicJsonValue ToJson()
        {
            var json = base.ToJson();
            json[nameof(StartUpTime)] = StartUpTime;
            return json;
        }
    }
}
