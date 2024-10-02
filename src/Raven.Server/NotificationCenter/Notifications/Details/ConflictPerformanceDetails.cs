using System;
using System.Collections.Generic;
using Sparrow.Json.Parsing;
using static Raven.Server.NotificationCenter.ConflictRevisionsExceeded;

namespace Raven.Server.NotificationCenter.Notifications.Details
{
    public class ConflictPerformanceDetails : INotificationDetails
    {
        public ConflictPerformanceDetails()
        {
            Details = new Queue<ActionDetails>();
        }

        public Queue<ActionDetails> Details { get; set; }

        public DynamicJsonValue ToJson()
        {
            var dja = new DynamicJsonArray();
            foreach (var details in Details)
            {
                var djv = new DynamicJsonValue
                {
                    [nameof(ActionDetails.Id)] = details.Id,
                    [nameof(ActionDetails.Reason)] = details.Reason,
                    [nameof(ActionDetails.Deleted)] = details.Deleted,
                    [nameof(ActionDetails.Time)] = details.Time
                };
                dja.Add(djv);
            }

            return new DynamicJsonValue(GetType())
            {
                [nameof(Details)] = dja
            };
        }

        public void Update(ConflictInfo info)
        {
            Details.Enqueue(new ActionDetails { Id = info.GetId(), Reason = info.Reason.ToString(), Deleted = info.Deleted, Time = info.Time });

            while (Details.Count > QueueMaxSize)
                Details.TryDequeue(out _);
        }

        public class ActionDetails
        {
            public string Id { get; set; }
            public string Reason { get; set; }
            public long Deleted { get; set; }
            public DateTime Time { get; set; }
        }
    }
}
