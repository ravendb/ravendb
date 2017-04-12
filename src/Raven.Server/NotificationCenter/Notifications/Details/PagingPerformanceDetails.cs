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
                        [nameof(ActionDetails.Occurence)] = details.Occurence
                    });
                }

                djv[key] = list;
            }

            return new DynamicJsonValue(GetType())
            {
                [nameof(Actions)] = djv
            };
        }

        public void Update(string action, int numberOfResults, int pageSize, DateTime occurence)
        {
            Queue<ActionDetails> details;
            if (Actions.TryGetValue(action, out details) == false)
                Actions[action] = details = new Queue<ActionDetails>();

            details.Enqueue(new ActionDetails { Occurence = occurence, NumberOfResults = numberOfResults, PageSize = pageSize });

            while (details.Count > 10)
                details.Dequeue();
        }

        internal class ActionDetails
        {
            public DateTime Occurence { get; set; }
            public int NumberOfResults { get; set; }
            public int PageSize { get; set; }
        }
    }
}