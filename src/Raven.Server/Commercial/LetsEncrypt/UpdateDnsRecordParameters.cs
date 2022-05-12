using System;
using System.Threading;
using Raven.Client.Documents.Operations;

namespace Raven.Server.Commercial.LetsEncrypt;

public class UpdateDnsRecordParameters
{
    public Action<IOperationProgress> OnProgress;
    public SetupProgressAndResult Progress;
    public string Challenge;
    public SetupInfo SetupInfo;
    public CancellationToken Token;
}
