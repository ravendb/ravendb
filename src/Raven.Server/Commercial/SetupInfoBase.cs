using System.Collections.Generic;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations.Configuration;
using Sparrow.Json.Parsing;

namespace Raven.Server.Commercial;

public abstract class SetupInfoBase
{
    public bool EnableExperimentalFeatures { get; set; }
    public string DataDirectory { get; set; }
    public string SetupCertificatePath { get; set; }
    public string LogsPath { get; set; }
    public License License { get; set; }
    public string AutoIndexingEngineType { get; set; }
    public string StaticIndexingEngineType { get; set; }
    public StudioConfiguration.StudioEnvironment Environment { get; set; }
    public Dictionary<string, NodeInfo> NodeSetupInfos { get; set; }
    public string LocalNodeTag { get; set; }
    public bool ZipOnly { get; set; }
    public bool StartAsPassive { get; set; }

    public abstract Task<byte[]> GenerateZipFile(CreateSetupPackageParameters parameters);
    public abstract void ValidateInfo(CreateSetupPackageParameters parameters);

    public virtual DynamicJsonValue ToJson()
    {
        return new DynamicJsonValue
        {
            [nameof(EnableExperimentalFeatures)] = EnableExperimentalFeatures,
            [nameof(Environment)] = Environment,
            [nameof(NodeSetupInfos)] = DynamicJsonValue.Convert(NodeSetupInfos),
            [nameof(LocalNodeTag)] = LocalNodeTag,
            [nameof(License)] = License?.ToJson(),
            [nameof(StartAsPassive)] = StartAsPassive,
        };
    }
}
