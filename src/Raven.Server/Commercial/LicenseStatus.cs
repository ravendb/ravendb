using System.Collections.Generic;
using Raven.Imports.Newtonsoft.Json;
using Raven.Imports.Newtonsoft.Json.Converters;

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

        [JsonConverter(typeof(StringEnumConverter))] //TODO: delete this and use blittable for serialization! - temporary fix
        public LicenseType LicenseType
        {
            get
            {
                if (Error)
                    return LicenseType.Invalid;

                object type;
                if (Attributes != null && Attributes.TryGetValue("type", out type))
                    return (LicenseType) type;

                return LicenseType.None;
            }
        }
    }
}