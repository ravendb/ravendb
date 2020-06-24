using Raven.Client.ServerWide.Commands;
using Raven.Client.ServerWide.Operations;
using Sparrow.Json.Parsing;

namespace Raven.Server.Commercial
{
    public class DetailsPerNode: IDynamicJson
    {
        public int UtilizedCores;

        public bool CustomUtilizedCores;

        public int NumberOfCores;

        public double InstalledMemoryInGb;

        public double UsableMemoryInGb;

        public BuildNumber BuildInfo;

        public OsInfo OsInfo;

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(UtilizedCores)] = UtilizedCores,
                [nameof(CustomUtilizedCores)] = CustomUtilizedCores,
                [nameof(NumberOfCores)] = NumberOfCores,
                [nameof(InstalledMemoryInGb)] = InstalledMemoryInGb,
                [nameof(UsableMemoryInGb)] = UsableMemoryInGb,
                [nameof(BuildInfo)] = BuildInfo,
                [nameof(OsInfo)] = OsInfo
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
                OsInfo = nodeInfo.OsInfo
            };
        }
    }
}
