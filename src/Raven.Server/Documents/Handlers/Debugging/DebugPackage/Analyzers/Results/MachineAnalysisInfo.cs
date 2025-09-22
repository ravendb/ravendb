using Raven.Client.ServerWide.Operations;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Handlers.Debugging.DebugPackage.Analyzers.Results;

public class MachineAnalysisInfo : IDynamicJson
{
    public int? NumberOfCores;

    public int? UtilizedCores;

    public double? InstalledMemoryInGb;

    public double? UsableMemoryInGb;
    
    public OsInfo OsInfo;

    public DynamicJsonValue ToJson()
    {
        return new DynamicJsonValue
        {
            [nameof(NumberOfCores)] = NumberOfCores,
            [nameof(UtilizedCores)] = UtilizedCores,
            [nameof(InstalledMemoryInGb)] = InstalledMemoryInGb,
            [nameof(UsableMemoryInGb)] = UsableMemoryInGb,
            [nameof(OsInfo)] = OsInfo?.ToJson()
        };
    }
}
