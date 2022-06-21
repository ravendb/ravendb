using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client;
using Raven.Client.Documents.Operations.Configuration;
using Raven.Server.Commercial.SetupWizard;
using Sparrow.Json.Parsing;

namespace Raven.Server.Commercial;

public abstract class SetupInfoBase
{
    public bool EnableExperimentalFeatures { get; set; }
    public StudioConfiguration.StudioEnvironment Environment { get; set; }
    public Dictionary<string, NodeInfo> NodeSetupInfos { get; set; }
    
    public abstract Task<byte[]> GenerateZipFile(CreateSetupPackageParameters parameters);
    public abstract void InfoValidation(CreateSetupPackageParameters parameters);
    
    public virtual DynamicJsonValue ToJson()
    {
        return new DynamicJsonValue
        {
            [nameof(EnableExperimentalFeatures)] = EnableExperimentalFeatures,
            [nameof(Environment)] = Environment,
            [nameof(NodeSetupInfos)] = DynamicJsonValue.Convert(NodeSetupInfos)
        };
    }
}
