using System.Collections.Generic;

namespace Raven.Server.ServerWide.Licensing
{
    public class LicenseStatus
    {
        public LicenseStatus()
        {
            Message = "No installed license";
            Status = "AGPL - Open Source";
        }

        public string Status { get; set; }

        public bool Error { get; set; }

        public Dictionary<string, object> Attributes { get; set; }

        public string Message { get; set; }

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