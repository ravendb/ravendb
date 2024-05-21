using System.Collections.Generic;
using System.Security.Cryptography.X509Certificates;
using FastTests;
using Raven.Server.Config.Categories;
using Raven.Server.Utils;
using Tests.Infrastructure;
using Tests.Infrastructure.Utils;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_22210 : RavenTestBase
{
    public RavenDB_22210(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.Certificates)]
    public void RenewedWithDifferentIntermediate_CanAccess()
    {
        var certificates = GenerateAndRenewWithDifferentIntermediate();
        PopulateCaStore(certificates.ca, certificates.intermediate, certificates.intermediate2);

        try
        {
            var explanationsList = new List<string>();
            var result = CertificateUtils.CertHasKnownIssuer(certificates.clientRenewed, certificates.client, new SecurityConfiguration(), explanationsList);
            Assert.True(result, string.Join('\n', explanationsList));
        }
        finally
        {
            CleanupCaStore(certificates.ca, certificates.intermediate, certificates.intermediate2);
        }
    }

    [RavenFact(RavenTestCategory.Certificates)]
    public void RenewedWithTheSameIntermediate_CanAccess()
    {
        var certificates = GenerateAndRenewWithTheSameIntermediate();
        PopulateCaStore(certificates.ca, certificates.intermediate);

        try
        {
            var explanationsList = new List<string>();
            var result = CertificateUtils.CertHasKnownIssuer(certificates.clientRenewed, certificates.client, new SecurityConfiguration(), explanationsList);
            Assert.True(result, string.Join('\n', explanationsList));
        }
        finally
        {
            CleanupCaStore(certificates.ca, certificates.intermediate);
        }
    }

    [RavenFact(RavenTestCategory.Certificates)]
    public void SelfSigned_Renewed_CanAccess()
    {
        var certificates = GenerateAndRenewSelfSigned();

        var explanationsList = new List<string>();
        var result = CertificateUtils.CertHasKnownIssuer(certificates.clientRenewed, certificates.client, new SecurityConfiguration(), explanationsList);
        Assert.True(result, string.Join('\n', explanationsList));
    }

    [RavenFact(RavenTestCategory.Certificates)]
    public void RenewedWithDifferentChain_CannotAccess()
    {
        var certificates = GenerateAndRenewWithDifferentChain();
        PopulateCaStore(certificates.ca, certificates.intermediate, certificates.intermediate2);
        try
        {
            var explanationsList = new List<string>();
            var result = CertificateUtils.CertHasKnownIssuer(certificates.clientRenewed, certificates.client, new SecurityConfiguration(), explanationsList);
            Assert.False(result, string.Join('\n', explanationsList));
        }
        finally
        {
            CleanupCaStore(certificates.ca, certificates.intermediate, certificates.intermediate2);
        }
    }

    private static (X509Certificate2 ca, X509Certificate2 intermediate, X509Certificate2 intermediate2, X509Certificate2 client, X509Certificate2 clientRenewed)
        GenerateAndRenewWithDifferentIntermediate()
    {
        var caKp = CertificateGenerator.GenerateRSAKeyPair();
        var ca = CertificateGenerator.GenerateRootCACertificate(CaName, 5, caKp);

        var intermediateKp = CertificateGenerator.GenerateRSAKeyPair();
        var intermediate = CertificateGenerator.GenerateIntermediateCACertificate(ca, caKp, IntermediateName, 2, intermediateKp);

        var intermediate2Kp = CertificateGenerator.GenerateRSAKeyPair();
        var intermediate2 = CertificateGenerator.GenerateIntermediateCACertificate(ca, caKp, $"{IntermediateName}-2", 2, intermediate2Kp);

        var clientKp = CertificateGenerator.GenerateRSAKeyPair();
        var client = CertificateGenerator.GenerateSignedClientCertificate(intermediate, intermediateKp, ClientName, 1, clientKp);

        var client2 = CertificateGenerator.GenerateSignedClientCertificate(intermediate2, intermediate2Kp, ClientRenewedName, 1, clientKp);

        return (ca, intermediate, intermediate2, client, client2);
    }

    private static (X509Certificate2 ca, X509Certificate2 intermediate, X509Certificate2 intermediate2, X509Certificate2 client, X509Certificate2 clientRenewed)
        GenerateAndRenewWithDifferentChain()
    {
        var caKp = CertificateGenerator.GenerateRSAKeyPair();
        var ca = CertificateGenerator.GenerateRootCACertificate(CaName, 5, caKp);

        var ca2Kp = CertificateGenerator.GenerateRSAKeyPair();
        var ca2 = CertificateGenerator.GenerateRootCACertificate($"{CaName}-2", 5, caKp);

        var intermediateKp = CertificateGenerator.GenerateRSAKeyPair();
        var intermediate = CertificateGenerator.GenerateIntermediateCACertificate(ca, caKp, IntermediateName, 2, intermediateKp);

        var intermediate2Kp = CertificateGenerator.GenerateRSAKeyPair();
        var intermediate2 = CertificateGenerator.GenerateIntermediateCACertificate(ca2, ca2Kp, $"{IntermediateName}-2", 2, intermediate2Kp);

        var clientKp = CertificateGenerator.GenerateRSAKeyPair();
        var client = CertificateGenerator.GenerateSignedClientCertificate(intermediate, intermediateKp, ClientName, 1, clientKp);

        var client2 = CertificateGenerator.GenerateSignedClientCertificate(intermediate2, intermediate2Kp, ClientRenewedName, 1, clientKp);

        return (ca, intermediate, intermediate2, client, client2);
    }

    private static (X509Certificate2 ca, X509Certificate2 intermediate, X509Certificate2 client, X509Certificate2 clientRenewed) GenerateAndRenewWithTheSameIntermediate()
    {
        var caKp = CertificateGenerator.GenerateRSAKeyPair();
        var ca = CertificateGenerator.GenerateRootCACertificate(CaName, 5, caKp);

        var intermediateKp = CertificateGenerator.GenerateRSAKeyPair();
        var intermediate = CertificateGenerator.GenerateIntermediateCACertificate(ca, caKp, IntermediateName, 2, intermediateKp);

        var clientKp = CertificateGenerator.GenerateRSAKeyPair();
        var client = CertificateGenerator.GenerateSignedClientCertificate(intermediate, intermediateKp, ClientName, 1, clientKp);
        var client2 = CertificateGenerator.GenerateSignedClientCertificate(intermediate, intermediateKp, ClientRenewedName, 1, clientKp);

        return (ca, intermediate, client, client2);
    }

    private static (X509Certificate2 client, X509Certificate2 clientRenewed) GenerateAndRenewSelfSigned()
    {
        var clientKp = CertificateGenerator.GenerateRSAKeyPair();
        var client = CertificateGenerator.GenerateSelfSignedClientCertificate(ClientName, 1, clientKp);
        var client2 = CertificateGenerator.GenerateSelfSignedClientCertificate(ClientRenewedName, 1, clientKp);

        return (client, client2);
    }

    private static void PopulateCaStore(params X509Certificate2[] certificates)
    {
        using (var store = new X509Store(StoreName.CertificateAuthority, StoreLocation.CurrentUser))
        {
            store.Open(OpenFlags.ReadWrite);
            store.AddRange(new X509Certificate2Collection(certificates));
        }
    }

    private static void CleanupCaStore(params X509Certificate2[] certificates)
    {
        using (var store = new X509Store(StoreName.CertificateAuthority, StoreLocation.CurrentUser))
        {
            store.Open(OpenFlags.ReadWrite);
            store.RemoveRange(new X509Certificate2Collection(certificates));
        }
    }

    private const string CaName = "raven-test-ca";
    private const string IntermediateName = "raven-test-intermediate";
    private const string ClientName = "raven-test-client";
    private const string ClientRenewedName = "raven-test-client-renewed";
}
