using System;
using System.Collections.Generic;
using Raven.Server.Utils;
using Sparrow.Json.Parsing;

namespace Raven.Server.Commercial
{
    public class LicenseStatus
    {
        public LicenseStatus()
        {
            Message = "No installed license";
        }

        public bool Error { get; set; }

        public Dictionary<string, object> Attributes { get; set; }

        public string Message { get; set; }

        public string Status => Attributes == null ? "AGPL - Open Source" : "Commercial";

        public string LicenseType
        {
            get
            {
                if (Error)
                    return "Invalid";

                if (Attributes == null)
                    return "None";

                object type;
                if (Attributes != null && Attributes.TryGetValue("type", out type))
                    return (string)type;

                return "Unknown";
            }
        }

        public DateTime FirstServerStartDate { get; set; }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(FirstServerStartDate)] = FirstServerStartDate,
                [nameof(Error)] = Error,
                [nameof(Message)] = Message,
                [nameof(Status)] = Status,
                [nameof(LicenseType)] = LicenseType,
                [nameof(Attributes)] = TypeConverter.ToBlittableSupportedType(Attributes)
            };
        }
    }
}