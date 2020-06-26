using System;
using Raven.Client.ServerWide.Operations;
using Sparrow.Json.Parsing;

namespace Raven.Server.Commercial
{
    public class DetailsPerNode: IDynamicJson
    {
        public int UtilizedCores;

        public int? MaxUtilizedCores;

        public int NumberOfCores;

        public double InstalledMemoryInGb;

        public double UsableMemoryInGb;

        public BuildNumber BuildInfo;

        public OsInfo OsInfo;

        public int AvailableCoresToAssignForNode
        {
            get
            {
                var availableCoresToAssign = NumberOfCores - UtilizedCores;
                if (MaxUtilizedCores == null)
                    return availableCoresToAssign;

                if (UtilizedCores < MaxUtilizedCores.Value)
                    return Math.Min(availableCoresToAssign, MaxUtilizedCores.Value - UtilizedCores);

                return 0;
            }
        }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(UtilizedCores)] = UtilizedCores,
                [nameof(MaxUtilizedCores)] = MaxUtilizedCores,
                [nameof(NumberOfCores)] = NumberOfCores,
                [nameof(InstalledMemoryInGb)] = InstalledMemoryInGb,
                [nameof(UsableMemoryInGb)] = UsableMemoryInGb,
                [nameof(BuildInfo)] = BuildInfo,
                [nameof(OsInfo)] = OsInfo
            };
        }

        public int GetMaxCoresToUtilize(int requestedCores)
        {
            var coresToUtilize = Math.Min(requestedCores, NumberOfCores);
            if (MaxUtilizedCores != null)
                coresToUtilize = Math.Min(coresToUtilize, MaxUtilizedCores.Value);

            return coresToUtilize;
        }
    }
}
