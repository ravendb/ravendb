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

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(UtilizedCores)] = UtilizedCores,
                [nameof(NumberOfCores)] = NumberOfCores,
                [nameof(InstalledMemoryInGb)] = InstalledMemoryInGb,
                [nameof(UsableMemoryInGb)] = UsableMemoryInGb,
                [nameof(BuildInfo)] = BuildInfo
            };
        }
    }
}
