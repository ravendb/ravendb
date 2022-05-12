using System;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Configuration;
using Raven.Client.ServerWide.Operations.Certificates;

namespace Raven.Server.Commercial.LetsEncrypt;

public class CompleteClusterConfigurationParameters
{
    public SetupProgressAndResult Progress;
    public Action<IOperationProgress> OnProgress;
    public Func<string> OnSettingsPath;
    public SetupInfo SetupInfo;
    public Func<string, string, Task> OnBeforeAddingNodesToCluster;
    public Func<string, Task> AddNodeToCluster;
    public Action<Action<IOperationProgress>, SetupProgressAndResult, X509Certificate2> RegisterClientCertInOs;
    public Func<StudioConfiguration.StudioEnvironment, Task> OnPutServerWideStudioConfigurationValues;
    public Action<string> OnWriteSettingsJsonLocally;
    public Func<string, string> OnGetCertificatePath;
    public Func<X509Certificate2,CertificateDefinition, Task> PutCertificateInCluster;
    public SetupMode SetupMode;
    public bool CertificateValidationKeyUsages;
    public LicenseType LicenseType;
    public CancellationToken Token = CancellationToken.None;
}
