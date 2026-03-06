using System;
using System.Security.Cryptography.X509Certificates;
using FastTests;
using Raven.Client;
using Raven.Client.Util;
using Raven.Server.Utils;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues;

public class RavenDB_25327 : RavenTestBase
{
    public RavenDB_25327(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.Certificates)]
    public void CanCreateCertificateForCommunication_UsingServerCertWith1EKU_AndMultipleSans_WithoutSubject()
    {
        const string firstDomain = "a.test-domain.com";
        var suffix = Guid.NewGuid().ToString().Split('-')[0];
        var ca = CertificateUtils.CreateCertificateAuthorityCertificate($"ca-{suffix}", out var caSubjectName);
        CertificateUtils.CreateSelfSignedCertificateBasedOnPrivateKey(null,
            caSubjectName,
            (ca.GetExportableRsaPrivateKey(), ca.GetRSAPublicKey()),
            false,
            false,
            DateTime.UtcNow.AddDays(7),
            out var serverCertBytes,
            sans: [firstDomain, "b.test-domain.com", "c.test-domain.com"],
            with2Eku: false);

        var serverCert = CertificateLoaderUtil.CreateCertificate(serverCertBytes, flags: CertificateLoaderUtil.FlagsForExport);
        Assert.Contains(firstDomain, serverCert.GetDisplayName());
        var clientCert = CertificateUtils.CreateClientCertificateFromServerCertificate(serverCert, out _);

        Assert.NotNull(clientCert);
        Assert.Equal("CN=client-cert-for-cluster-communication", clientCert.Subject);
    }
}
