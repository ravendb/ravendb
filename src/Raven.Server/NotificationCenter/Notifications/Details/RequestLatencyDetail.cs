using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Lucene.Net.Search;
using Microsoft.AspNetCore.Http;
using Newtonsoft.Json;
using Raven.Client;
using Raven.Client.Extensions;
using Raven.Client.Json;
using Raven.Server.Json;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.NotificationCenter.Notifications.Details
{
    public class RequestLatencyDetail : INotificationDetails
    {
        private const int RequestLatencyDetailLimit = 50;
        public Dictionary<string, List<RequestLatencyInfo>> RequestLatencies = new Dictionary<string, List<RequestLatencyInfo>>(StringComparer.OrdinalIgnoreCase);
     
        public void Update(string path, IQueryCollection requestQuery, long duration, string database, long requestContentLength)
        {
            if (RequestLatencies.TryGetValue(database, out var hintQueue) == false)
            {
                var queue = new List<RequestLatencyInfo>
                {
                    new RequestLatencyInfo(path, duration, database, new Parameters(requestQuery.ToDictionary(x => x.Key, x => (object)x.Value.FirstOrDefault())),
                        requestContentLength)
                };
                RequestLatencies.Add(database, queue);
            }
            else
            {
                if (hintQueue.Count > RequestLatencyDetailLimit)
                {
                    while (hintQueue.Count > RequestLatencyDetailLimit)
                    {
                        hintQueue.RemoveAt(hintQueue.Count - 1);
                    }
                }

                hintQueue.Add(new RequestLatencyInfo(path, duration, database, new Parameters(requestQuery.ToDictionary(x => x.Key, x => (object)x.Value.FirstOrDefault())), requestContentLength));
            }
        }

        public DynamicJsonValue ToJson()
        {
            var latencies = new DynamicJsonValue();

            foreach (var key in RequestLatencies.Keys)
            {
                var queue = RequestLatencies[key];
                var list = new DynamicJsonArray();
                if (queue != null)
                {
                    foreach (var details in queue)
                    {
                        list.Add(details.ToJson());
                    }
                }

                latencies[key] = list;
            }

            return new DynamicJsonValue
            {
                [nameof(RequestLatencies)] = latencies,
                [Constants.Documents.Metadata.Key] = new DynamicJsonValue
                {
                    [Constants.Documents.Metadata.RavenClrType] = typeof(RequestLatencyDetail).AssemblyQualifiedName
                }
            };
        }
    }

    public class RequestLatencyInfo 
    {
        public string Path;
        public long Duration;
        public DateTime Date;
        public string Database;
        public Dictionary<string,string> Parameters;
        public long RequestContentLength;

        //for deserialization
        protected RequestLatencyInfo()
        {
            
        }
        
        public RequestLatencyInfo(string path, long duration, string database, Parameters parameters, long requestContentLength)
        {
            Path = path;
            Duration = duration;
            Database = database;
            Parameters = parameters.ToDictionary(kvp => kvp.Key, kvp => Convert.ToString(kvp.Value));
            RequestContentLength = requestContentLength;
            Date = DateTime.UtcNow;
        }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(Path)] = Path,
                [nameof(Duration)] = Duration,
                [nameof(Date)] = Date,
                [nameof(Database)] = Database,
                [nameof(Parameters)] = Parameters.ToJson(),
                [nameof(RequestContentLength)] = RequestContentLength,
                [Constants.Documents.Metadata.Key] = new DynamicJsonValue
                {
                    [Constants.Documents.Metadata.RavenClrType] = typeof(RequestLatencyInfo).FullName
                }
            };
        }
    }
}
