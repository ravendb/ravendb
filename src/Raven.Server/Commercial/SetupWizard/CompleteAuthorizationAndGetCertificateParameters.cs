using System;
using System.Security.Cryptography;
using System.Threading;

namespace Raven.Server.Commercial.SetupWizard;

public sealed class CompleteAuthorizationAndGetCertificateParameters
{
    public Action OnValidationSuccessful;
    public SetupInfo SetupInfo;
    public LetsEncryptClient Client;
    public (string Challange, LetsEncryptClient.CachedCertificateResult Cache) ChallengeResult;
    public CancellationToken Token;
    public RSA ExistingPrivateKey;
}
