using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.AspNetCore.Http;
using Raven.Client.Extensions;
using Sparrow.Collections.LockFree;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Concurrent = System.Collections.Concurrent;
namespace Raven.Server.NotificationCenter.Notifications.Details
{
    public class RequestLatencyDetail : INotificationDetails
    {
        private const int RequestLatencyDetailLimit = 50;
        public ConcurrentDictionary<string, Concurrent.ConcurrentQueue<RequestLatencyInfo>> RequestLatencies { get; set; }

        public RequestLatencyDetail()
        {
            RequestLatencies = new ConcurrentDictionary<string, Concurrent.ConcurrentQueue<RequestLatencyInfo>>();
        }
        
        public void Update(string queryString,  IQueryCollection requestQuery, long duration, string action)
        {
            if (RequestLatencies.TryGetValue(action, out var hintQueue) == false)
            {
                var queue = new Concurrent.ConcurrentQueue<RequestLatencyInfo>();
                queue.Enqueue(new RequestLatencyInfo(queryString, requestQuery.ToDictionary(x => x.Key, x => x.Value.FirstOrDefault()), duration, action));
                RequestLatencies.Add(action, queue);
            }
            else
            {
                EnforceLimitOfQueueLength(hintQueue);
                hintQueue.Enqueue(new RequestLatencyInfo(queryString, requestQuery.ToDictionary(x => x.Key, x => x.Value.FirstOrDefault()), duration, action));
            }
        }

        private static void EnforceLimitOfQueueLength(Concurrent.ConcurrentQueue<RequestLatencyInfo> hintQueue)
        {
            while (hintQueue.Count > RequestLatencyDetailLimit)
            {
                hintQueue.TryDequeue(out _);
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
