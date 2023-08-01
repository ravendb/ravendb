using System;
using System.Threading;
using Raven.Client.Documents.Operations;

namespace Raven.Server.Commercial.LetsEncrypt;

public sealed class UpdateDnsRecordParameters
{
    public Action<IOperationProgress> OnProgress;
    public SetupProgressAndResult Progress;
    public string Challenge;
    public SetupInfo SetupInfo;
    public bool RegisterTcpDnsRecords;
    public CancellationToken Token;
}
