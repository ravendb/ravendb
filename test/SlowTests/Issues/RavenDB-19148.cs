using System;
using System.Collections.Generic;
using System.Security.Cryptography.X509Certificates;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.ServerWide.Operations;
using Raven.Client.Util;
using Raven.Server.Utils;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_19148 : ClusterTestBase
{
    public RavenDB_19148(ITestOutputHelper output) : base(output)
    {
    }

    [Fact]
    public async Task CanAuthUsingWellKnownIssuer()
    {
        var ca = CertificateUtils.CreateCertificateAuthorityCertificate("auth", out var caKey, out var caName);
        CertificateUtils.CreateSelfSignedCertificateBasedOnPrivateKey("admin", caName, caKey, true, false,
            DateTime.UtcNow.Date.AddMonths(3), out var certBytes);

        byte[] caBytes = ca.Export(X509ContentType.Cert);
        var result = await CreateRaftClusterWithSsl(1, true, customSettings: new Dictionary<string, string>
        {
            ["Security.WellKnownIssuers.Admin"] = Convert.ToBase64String(caBytes)
        });

        using (var store = new DocumentStore
        {
            Urls = new[] { result.Leader.WebUrl },
            Certificate = CertificateLoaderUtil.CreateCertificateFromAny(certBytes),
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
