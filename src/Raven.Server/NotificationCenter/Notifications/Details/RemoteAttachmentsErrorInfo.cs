using System;
using System.Collections.Generic;
using Sparrow.Json.Parsing;

namespace Raven.Server.NotificationCenter.Notifications.Details
{
    public class RemoteAttachmentsErrorInfo : IDynamicJson
    {
        public RemoteAttachmentsErrorInfo(string error, string identifier, string hash, List<string> ids)
        {
            Date = DateTime.UtcNow;
            Error = error;
            Identifier = identifier;
            Hash = hash;
            Ids = ids;
        }

        public DateTime Date { get; set; }
        public string Error { get; set; }
        public string Identifier { get; set; }
        public string Hash { get; set; }
        public List<string> Ids { get; set; }

        public DynamicJsonValue ToJson()
        {
            var djv = new DynamicJsonValue
            {
                [nameof(Date)] = Date,
                [nameof(Error)] = Error,
                [nameof(Identifier)] = Identifier,
                [nameof(Hash)] = Hash
            };

            if (Ids is { Count: > 0 })
            {
                djv[nameof(Ids)] = new DynamicJsonArray(Ids);
            }

            return djv;
        }
    }
}
