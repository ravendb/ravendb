using System.ComponentModel;
using System.Threading;
using McMaster.Extensions.CommandLineUtils;
using Raven.Server.Commercial;

namespace rvn.Parameters;

internal class CreateSetupPackageParameters
{
    public string SetupJsonPath;
    public string PackageOutputPath;
    public CommandLineApplication Command;
    public string Mode;
    public string CertificatePath;
    public string CertPassword;
    public SetupProgressAndResult Progress;
    public CancellationToken CancellationToken;
}

