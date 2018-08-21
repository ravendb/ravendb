using System;
using System.Collections.Concurrent;
using Sparrow.Json;
using Sparrow.Json.Parsing;
namespace Raven.Server.NotificationCenter.Notifications.Details
{
    public class RequestLatencyDetail : INotificationDetails
    {
        private const int RequestLatencyDetailLimit = 50;
        public ConcurrentDictionary<string, ConcurrentQueue<RequestLatencyInfo>> RequestLatencies { get; set; }

        public RequestLatencyDetail()
        {
            RequestLatencies = new ConcurrentDictionary<string, ConcurrentQueue<RequestLatencyInfo>>();
        }
        
        public void Update(long duration, string action, string query)
        {
            if (RequestLatencies.TryGetValue(action, out var hintQueue) == false)
            {
                var queue = new ConcurrentQueue<RequestLatencyInfo>();
                queue.Enqueue(new RequestLatencyInfo(duration, action, query));
                RequestLatencies.TryAdd(action, queue);
            }
            else
            {
                EnforceLimitOfQueueLength(hintQueue);
                hintQueue.Enqueue(new RequestLatencyInfo(duration, action, query));
            }
        }

        private static void EnforceLimitOfQueueLength(ConcurrentQueue<RequestLatencyInfo> hintQueue)
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
        public long Duration;
        public DateTime Date;
        public string Action;
        public string Query;

        public RequestLatencyInfo(long duration, string action, string query)
        {
            Duration = duration;
            Action = action;
            Query = query;
            Date = DateTime.UtcNow;
        }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(Duration)] = Duration,
                [nameof(Date)] = Date,
                [nameof(Action)] = Action,
                [nameof(Query)] = Query
            };
        }
    }
}
