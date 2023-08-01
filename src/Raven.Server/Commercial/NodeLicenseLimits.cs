using System.Collections.Generic;
using Sparrow.Json.Parsing;

namespace Raven.Server.Commercial
{
    public sealed class NodeLicenseLimits : IDynamicJson
    {
        public string NodeTag { get; set; }

        public DetailsPerNode DetailsPerNode { get; set; }

        public int LicensedCores { get; set; }

        public List<string> AllNodes { get; set; }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(NodeTag)] = NodeTag,
                [nameof(DetailsPerNode)] = DetailsPerNode.ToJson(),
                [nameof(LicensedCores)] = LicensedCores,
                [nameof(AllNodes)] = AllNodes
            };
        }
    }
}
