using System;
using System.Collections.Generic;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.NotificationCenter.Notifications.Details;

public class ServerLimitsDetails : INotificationDetails
{
    public const int MaxNumberOfLimits = 128;

    public LinkedList<ServerLimitInfo> Limits { get; set; } = new();

    public DynamicJsonValue ToJson()
    {
        var djv = new DynamicJsonValue();

        var dict = new DynamicJsonArray();
        djv[nameof(Limits)] = dict;

        foreach (var limit in Limits)
        {
            dict.Add(limit.ToJson());
        }
        return djv;
    }

    public class ServerLimitInfo : IDynamicJsonValueConvertible
    {
        public long Current { get; set; }
        public long Max { get; set; }
        public DateTime Date { get; set; }
        public string Limit { get; set; }
        public string Name { get; set; }

        public ServerLimitInfo()
        {
            /* Used for deserialization */
        }

        public ServerLimitInfo(string name, string limit, long current, long max, DateTime now)
        {
            Name = name;
            Limit = limit;
            Date = now;
            Current = current;
            Max = max;
        }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(Current)] = Current,
                [nameof(Max)] = Max,
                [nameof(Date)] = Date,
                [nameof(Name)] = Name,
                [nameof(Limit)] = Limit,
            };
        }
    }
}
