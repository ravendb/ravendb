using System.Collections.Generic;
using System.Linq;
using Sparrow.Json.Parsing;

namespace Raven.Server.Commercial
{
    public class LicenseLimits
    {
        public LicenseLimits()
        {
            NodeLicenseDetails = new Dictionary<string, DetailsPerNode>();
        }

        public Dictionary<string, DetailsPerNode> NodeLicenseDetails { get; set; }

        public int TotalUtilizedCores => NodeLicenseDetails.Sum(x => x.Value.UtilizedCores);

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(NodeLicenseDetails)] = DynamicJsonValue.Convert(NodeLicenseDetails)
            };
        }
    }
}
