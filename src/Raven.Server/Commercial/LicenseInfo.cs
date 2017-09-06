using System.Collections.Generic;
using Sparrow.Json.Parsing;

namespace Raven.Server.Commercial
{
    public class LicenseLimits
    {
        public Dictionary<string, DetailsPerNode> NodeLicenseDetails { get; set; }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(NodeLicenseDetails)] = DynamicJsonValue.Convert(NodeLicenseDetails)
            };
        }
    }
}
