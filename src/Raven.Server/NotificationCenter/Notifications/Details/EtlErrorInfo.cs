using System;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.NotificationCenter.Notifications.Details
{
    public class EtlErrorInfo : IDynamicJsonValueConvertible
    {
        public string DocumentId { get; set; }
        public DateTime Date { get; set; }
        public string Error { get; set; }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(DocumentId)] = DocumentId,
                [nameof(Date)] = Date,
                [nameof(Error)] = Error
            };
        }
    }
}
