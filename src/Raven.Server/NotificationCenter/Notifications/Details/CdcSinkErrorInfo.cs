using System;
using Raven.Client.Util;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.NotificationCenter.Notifications.Details
{
    public class CdcSinkErrorInfo : IDynamicJson
    {
        public CdcSinkErrorInfo(string error)
        {
            Date = SystemTime.UtcNow;
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
