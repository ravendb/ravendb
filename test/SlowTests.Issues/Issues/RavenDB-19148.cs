using System;
using System.Collections.Generic;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.ServerWide.Operations;
using Raven.Server.Utils;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues;

public class RavenDB_19148 : DisableParallelTestBase
{
    public RavenDB_19148(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.Certificates | RavenTestCategory.Cluster)]
    public async Task CanAuthUsingWellKnownIssuer()
    {
        var suffix = Guid.NewGuid().ToString().Split('-')[0];
        var caCommonNameValue = $"auth-{suffix}";
        var ca = CertificateUtils.CreateCertificateAuthorityCertificate(caCommonNameValue, out var caName, generateNewKeyPair: true);
        CertificateUtils.CreateSelfSignedCertificateBasedOnPrivateKey($"admin-{suffix}", caName, (ca.GetExportableRsaPrivateKey(), ca.GetRSAPublicKey()), true, false,
            DateTime.UtcNow.Date.AddMonths(3), out var certBytes);
        CertificateUtils.RemoveOldTestCertificatesFromOsStore(caCommonNameValue);

        byte[] caBytes = ca.Export(X509ContentType.Cert);
        var result = await CreateRaftClusterWithSsl(1, true, customSettings: new Dictionary<string, string>
        {
            ["Security.WellKnownIssuers.Admin"] = Convert.ToBase64String(caBytes)
        });

        using (var store = new DocumentStore
        {
            Urls = new[] { result.Leader.WebUrl },
#pragma warning disable SYSLIB0057
            Certificate = new X509Certificate2(certBytes),
#pragma warning restore SYSLIB0057
            Conventions =
            {
                DisposeCertificate = false
            }
        })
        {
            store.Initialize();
            await store.Maintenance.Server.SendAsync(new GetBuildNumberOperation());
        }
    }
}
