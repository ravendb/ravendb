using System;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.NotificationCenter.Notifications.Details
{
    public class SlowSqlStatementInfo : IDynamicJsonValueConvertible
    {
        public long Duration { get; set; }
        public DateTime Date { get; set; }
        public string Statement { get; set; }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(Duration)] = Duration,
                [nameof(Date)] = Date,
                [nameof(Statement)] = Statement
            };
        }
    }
}
