using System.Threading;
using McMaster.Extensions.CommandLineUtils;
using Raven.Server.Commercial;

namespace rvn.Parameters;

internal class CreateSetupPackageParameters
{
    public string SetupInfoPath;
    public string PackageOutputPath;
    public CommandLineApplication Command;
    public string SetupMode;
    public string CertificatePath;
    public (string passFromCmd, string PassFromEnv) CertPassword;
    public SetupProgressAndResult Progress;
    public CancellationToken CancellationToken;
}
