using System.Collections.Generic;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations.Configuration;
using Sparrow.Json.Parsing;

namespace Raven.Server.Commercial;

public abstract class SetupInfoBase
{
    public bool EnableExperimentalFeatures { get; set; }
    public StudioConfiguration.StudioEnvironment Environment { get; set; }
    public Dictionary<string, NodeInfo> NodeSetupInfos { get; set; }
    public string LocalNodeTag { get; set; }
    public bool ZipOnly { get; set; }
    
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
        };
    }
}
