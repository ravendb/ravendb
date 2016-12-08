using System.Collections.Generic;

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