using System;
using System.Collections.Generic;
using Sparrow.Json.Parsing;

namespace Raven.Server.NotificationCenter.Notifications.Details
{
    internal class PagingPerformanceDetails : INotificationDetails
    {
        public PagingPerformanceDetails()
        {
            Actions = new Dictionary<string, Queue<ActionDetails>>(StringComparer.OrdinalIgnoreCase);
        }

        public Dictionary<string, Queue<ActionDetails>> Actions { get; set; }

        public DynamicJsonValue ToJson()
        {
            var djv = new DynamicJsonValue();
            foreach (var key in Actions.Keys)
            {
                var queue = Actions[key];
                if (queue == null)
                    continue;

                var list = new DynamicJsonArray();
                foreach (var details in queue)
                {
                    list.Add(new DynamicJsonValue
                    {
                        [nameof(ActionDetails.NumberOfResults)] = details.NumberOfResults,
                        [nameof(ActionDetails.PageSize)] = details.PageSize,
                        [nameof(ActionDetails.Occurrence)] = details.Occurrence,
                        [nameof(ActionDetails.Duration)] = details.Duration,
                        [nameof(ActionDetails.QueryString)] = details.QueryString
                    });
                }

                djv[key] = list;
            }

            return new DynamicJsonValue(GetType())
            {
                [nameof(Actions)] = djv
            };
        }

        public void Update(string action, string queryString, int numberOfResults, int pageSize, TimeSpan duration, DateTime occurrence)
        {
            if (Actions.TryGetValue(action, out Queue<ActionDetails> details) == false)
                Actions[action] = details = new Queue<ActionDetails>();

            details.Enqueue(new ActionDetails { Duration = duration, Occurrence = occurrence, NumberOfResults = numberOfResults, PageSize = pageSize, QueryString = queryString });

            while (details.Count > 10)
                details.Dequeue();
        }

        internal class ActionDetails
        {
            public string QueryString { get; set; }
            public TimeSpan Duration { get; set; }
            public DateTime Occurrence { get; set; }
            public int NumberOfResults { get; set; }
            public int PageSize { get; set; }
        }
    }
}