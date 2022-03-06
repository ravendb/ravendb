using System;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Configuration;

namespace Raven.Server.Commercial;

public class GetSetupZipFileParameters
{
    public CompleteClusterConfigurationResult CompleteClusterConfigurationResult; 
    public SetupProgressAndResult Progress;
    public bool ModifyLocalServer;
    public Action<IOperationProgress> OnProgress;
    public Func<string> OnSettingsPath;
    public SetupInfo SetupInfo;
    public SetupMode SetupMode;
    public Func<string, string> OnGetCertificatePath;
    public CancellationToken Token;
    public Func<StudioConfiguration.StudioEnvironment, Task> OnPutServerWideStudioConfigurationValues;
    public Action<string> OnWriteSettingsJsonLocally;
}
