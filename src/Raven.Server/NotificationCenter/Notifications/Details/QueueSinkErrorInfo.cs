using System;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.NotificationCenter.Notifications.Details
{
    public class QueueSinkErrorInfo : IDynamicJsonValueConvertible
    {
        public QueueSinkErrorInfo(string error)
        {
            Date = DateTime.UtcNow;
            Error = error;
        }
        
        public DateTime Date { get; set; }
        public string Error { get; set; }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(Date)] = Date,
                [nameof(Error)] = Error
            };
        }
    }
}
