using System;
using System.Collections.Generic;
using System.Linq;
using Sparrow.Json.Parsing;

namespace Raven.Server.Dashboard
{
    public class TrafficWatch : AbstractDashboardNotification
    {
        public override DashboardNotificationType Type => DashboardNotificationType.TrafficWatch;
        
        public List<TrafficWatchItem> Items { get; set; }

        public TrafficWatch()
        {
            Items = new List<TrafficWatchItem>();
        }

        public override DynamicJsonValue ToJson()
        {
            var json = base.ToJson();
            json[nameof(Items)] = new DynamicJsonArray(Items.Select(x => x.ToJson()));
            return json;
        }
    }
    
    public class TrafficWatchItem : IDynamicJson
    {
        public string Database { get; set; }
        
        public int RequestsPerSecond { get; set; }

        public int WritesPerSecond { get; set; }

        public double WriteBytesPerSecond { get; set; }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(Database)] = Database,
                [nameof(RequestsPerSecond)] = RequestsPerSecond,
                [nameof(WritesPerSecond)] = WritesPerSecond,
                [nameof(WriteBytesPerSecond)] = WriteBytesPerSecond
            };
        }
    }
}
