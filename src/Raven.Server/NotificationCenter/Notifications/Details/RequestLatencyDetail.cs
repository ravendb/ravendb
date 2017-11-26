using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.AspNetCore.Http;
using Raven.Client.Extensions;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.NotificationCenter.Notifications.Details
{
    public class RequestLatencyDetail : INotificationDetails
    {
        private const int RequestLatencyDetailLimit = 50;
        public Dictionary<string, Queue<RequestLatencyInfo>> RequestLatencies { get; set; }

        public RequestLatencyDetail()
        {
            RequestLatencies = new Dictionary<string, Queue<RequestLatencyInfo>>();
        }
        
        public void Update(string queryString,  IQueryCollection requestQuery, long duration, string action)
        {
            if (RequestLatencies.TryGetValue(action, out var hintQueue) == false)
            {
                var queue = new Queue<RequestLatencyInfo>();
                queue.Enqueue(new RequestLatencyInfo(queryString, requestQuery.ToDictionary(x => x.Key, x => x.Value.FirstOrDefault()), duration, action));
                RequestLatencies.Add(action, queue);
            }
            else
            {
                EnforceLimitOfQueueLength(hintQueue);
                hintQueue.Enqueue(new RequestLatencyInfo(queryString, requestQuery.ToDictionary(x => x.Key, x=> x.Value.FirstOrDefault()), duration, action));
            }
        }

        private static void EnforceLimitOfQueueLength(Queue<RequestLatencyInfo> hintQueue)
        {
            while (hintQueue.Count > RequestLatencyDetailLimit)
            {
                hintQueue.Dequeue();
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
        public string QueryString;
        public Dictionary<string, string> Parameters;
        public long Duration;
        public DateTime Date;
        public string Action;

        public RequestLatencyInfo(string queryString, Dictionary<string, string> parameters, long duration, string action)
        {
            QueryString = queryString;
            Parameters = parameters;
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
                [nameof(Parameters)] = Parameters.ToJson(),
                [nameof(Date)] = Date,
                [nameof(Action)] = Action
            };
        }
    }
}
