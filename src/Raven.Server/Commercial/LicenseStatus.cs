using System;
using System.Collections.Generic;
using System.Globalization;
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

        public string ShortDescription
        {
            get
            {
                if (Attributes == null)
                    return null;

                int? cores = null;
                if (Attributes.TryGetValue("cores", out object coresObject) &&
                    coresObject is int)
                {
                    cores = (int)coresObject;
                }

                int? ram = null;
                if (Attributes.TryGetValue("RAM", out object memoryObject) &&
                    memoryObject is int)
                {
                    ram = (int)memoryObject;
                }

                var list = new List<string>();
                if (cores != null)
                    list.Add($"{cores} CPUs");
                if (ram != null)
                    list.Add($"{(ram.Value == 0 ? "Unlimited" : $"{ram.Value}GB")} RAM");

                return string.Join(", ", list);
            }
        }

        public string FormattedExpiration
        {
            get
            {
                if (Attributes == null)
                    return null;

                if (Attributes.TryGetValue("expiration", out object expirationObject) &&
                    expirationObject is DateTime)
                {
                    var date = (DateTime)expirationObject;
                    return date.ToString("d", CultureInfo.CurrentCulture);
                }

                return null;
            }
        }

        public string Type
        {
            get
            {
                if (Error)
                    return "Invalid";

                if (Attributes == null)
                    return "None";

                if (Attributes != null &&
                    Attributes.TryGetValue("type", out object type) &&
                    type is int)
                {
                    var typeAsInt = (int)type;
                    if (Enum.IsDefined(typeof(LicenseType), typeAsInt))
                        return ((LicenseType)typeAsInt).ToString();
                }

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
                [nameof(ShortDescription)] = ShortDescription,
                [nameof(FormattedExpiration)] = FormattedExpiration,
                [nameof(Type)] = Type,
                [nameof(Attributes)] = TypeConverter.ToBlittableSupportedType(Attributes)
            };
        }
    }
}