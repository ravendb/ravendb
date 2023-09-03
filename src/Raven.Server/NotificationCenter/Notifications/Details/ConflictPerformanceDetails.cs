using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Sparrow.Json.Parsing;
using static Raven.Server.NotificationCenter.Revisions;

namespace Raven.Server.NotificationCenter.Notifications.Details
{
    public class ConflictPerformanceDetails : INotificationDetails
    {
        public ConflictPerformanceDetails()
        {
            Details = new Dictionary<string, ActionDetails>(StringComparer.OrdinalIgnoreCase);
        }

        public Dictionary<string, ActionDetails> Details { get; set; }

        public DynamicJsonValue ToJson()
        {
            var djv = new DynamicJsonValue();
            foreach (var (key, details) in Details)
            {
                djv[key] = new DynamicJsonValue
                {
                    [nameof(ActionDetails.Reason)] = details.Reason,
                    [nameof(ActionDetails.Deleted)] = details.Deleted,
                    [nameof(ActionDetails.Time)] = details.Time
                };
            }

            return new DynamicJsonValue(GetType())
            {
                [nameof(Details)] = djv
            };
        }

        public void Update(ConflictInfo info)
        {
            Details[info.Id] = new ActionDetails { Reason = info.Reason.ToString(), Deleted = info.Deleted, Time = info.Time };
        }

        public class ActionDetails
        {
            public string Reason { get; set; }
            public long Deleted { get; set; }
            public DateTime Time { get; set; }
        }
    }
}
