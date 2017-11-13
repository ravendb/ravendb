using System;
using System.Collections.Generic;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.NotificationCenter.Notifications.Details
{
    public class RequestLatencyDetail : INotificationDetails
    {
        public Dictionary<string, Queue<RequestLatencyInfo>> RequestLatencies { get; set; }

        public RequestLatencyDetail()
        {
            RequestLatencies = new Dictionary<string, Queue<RequestLatencyInfo>>();
        }
        
        public void Update(string queryString, long duration, string action)
        {
            if (RequestLatencies.TryGetValue(action, out var hintQueue) == false)
            {
                var queue = new Queue<RequestLatencyInfo>();
                queue.Enqueue(new RequestLatencyInfo(queryString, duration, action));
                RequestLatencies.Add(action, queue);
            }
            else
            {
                hintQueue.Enqueue(new RequestLatencyInfo(queryString, duration, action));
            }
        }

        public DynamicJsonValue ToJson()
        {
            var djv = new DynamicJsonValue();
            
            var dict = new DynamicJsonValue();
            djv[nameof(RequestLatencies)] = dict;
            
            foreach (var key in RequestLatencies.Keys)
            {
                var queue = RequestLatencies[key];
                if (queue == null)
                    continue;

                var list = new DynamicJsonArray();
                foreach (var details in queue)
                {
                    list.Add(details.ToJson());
                }

                dict[key] = list;
            }

            return djv;
        }
    }

    public struct RequestLatencyInfo : IDynamicJsonValueConvertible
    {
        public readonly string QueryString;
        public readonly long Duration;
        public readonly DateTime Date;
        public readonly string Action;

        public RequestLatencyInfo(string queryString, long duration, string action)
        {
            QueryString = queryString;
            Duration = duration;
            Action = action;
            Date = DateTime.UtcNow;
        }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(QueryString)] = QueryString,
                [nameof(Duration)] = Duration,
                [nameof(Date)] = Date,
                [nameof(Action)] = Action
            };
        }
    }
}
