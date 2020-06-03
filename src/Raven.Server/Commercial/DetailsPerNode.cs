using Raven.Client.ServerWide.Commands;
using Raven.Client.ServerWide.Operations;
using Sparrow.Json.Parsing;

namespace Raven.Server.Commercial
{
    public class DetailsPerNode: IDynamicJson
    {
        public int UtilizedCores;

        public int NumberOfCores;

        public double InstalledMemoryInGb;

        public double UsableMemoryInGb;

        public BuildNumber BuildInfo;

        public OsInfo OsInfo;

        public bool Modified;

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(UtilizedCores)] = UtilizedCores,
                [nameof(NumberOfCores)] = NumberOfCores,
                [nameof(InstalledMemoryInGb)] = InstalledMemoryInGb,
                [nameof(UsableMemoryInGb)] = UsableMemoryInGb,
                [nameof(BuildInfo)] = BuildInfo,
                [nameof(OsInfo)] = OsInfo,
                [nameof(Modified)] = Modified
            };
        }

        public static DetailsPerNode FromNodeInfo(NodeInfo nodeInfo)
        {
            return new DetailsPerNode
            {
                UtilizedCores = 0, // don't care
                NumberOfCores = nodeInfo.NumberOfCores,
                InstalledMemoryInGb = nodeInfo.InstalledMemoryInGb,
                UsableMemoryInGb = nodeInfo.UsableMemoryInGb,
                BuildInfo = nodeInfo.BuildInfo,
                OsInfo = nodeInfo.OsInfo,
                Modified = false
            };
        }
    }
}
