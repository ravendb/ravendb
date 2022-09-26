using System.Collections.Generic;
using System.Threading;
using McMaster.Extensions.CommandLineUtils;

namespace Raven.Server.Commercial;

public class CreateSetupPackageParameters
{
    public string SetupJsonPath;
    public string PackageOutputPath;
    public CommandLineApplication Command;
    public SetupInfo SetupInfo;
    public UnsecuredSetupInfo UnsecuredSetupInfo;
    public string Mode;
    public string CertificatePath;
    public string CertPassword;
    public string HelmValuesOutputPath;
    public SetupProgressAndResult Progress;
    public bool RegisterTcpDnsRecords;
    public CancellationToken CancellationToken;
}
