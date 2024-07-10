using System;
using System.Collections.Generic;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Threading;
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
        var explanationsList = new List<string>();

        try
        {
            var result = CertificateUtils.CertHasKnownIssuer(certificates.clientRenewed, certificates.client, new SecurityConfiguration(), explanationsList);
            Assert.True(result, string.Join('\n', explanationsList));
        }
        catch
        {
            //make sure CA certs are in store
            Assert.True(IsCACertificateInStore(certificates.ca), $"Certificate {certificates.ca.SubjectName} is not in store. {string.Join('\n', explanationsList)}");
            Assert.True(IsCACertificateInStore(certificates.intermediate),
                $"Certificate {certificates.intermediate.SubjectName} is not in store.  {string.Join('\n', explanationsList)}");
            Assert.True(IsCACertificateInStore(certificates.intermediate2),
                $"Certificate {certificates.intermediate2.SubjectName} is not in store.  {string.Join('\n', explanationsList)}");

            throw;
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
        var explanationsList = new List<string>();

        try
        {
            var result = CertificateUtils.CertHasKnownIssuer(certificates.clientRenewed, certificates.client, new SecurityConfiguration(), explanationsList);
            Assert.True(result, string.Join('\n', explanationsList));
        }
        catch
        {
            //make sure CA certs are in store
            Assert.True(IsCACertificateInStore(certificates.ca), $"Certificate {certificates.ca.SubjectName} is not in store. {string.Join('\n', explanationsList)}");
            Assert.True(IsCACertificateInStore(certificates.intermediate),
                $"Certificate {certificates.intermediate.SubjectName} is not in store. {string.Join('\n', explanationsList)}");

            throw;
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
        PopulateCaStore(certificates.ca, certificates.ca2, certificates.intermediate, certificates.intermediate2);
        var explanationsList = new List<string>();

        try
        {
            var result = CertificateUtils.CertHasKnownIssuer(certificates.clientRenewed, certificates.client, new SecurityConfiguration(), explanationsList);
            Assert.False(result, string.Join('\n', explanationsList));
        }
        catch
        {
            //make sure CA certs are in store
            Assert.True(IsCACertificateInStore(certificates.ca), $"Certificate {certificates.ca.SubjectName} is not in store. {string.Join('\n', explanationsList)}");
            Assert.True(IsCACertificateInStore(certificates.ca2), $"Certificate {certificates.ca2.SubjectName} is not in store. {string.Join('\n', explanationsList)}");
            Assert.True(IsCACertificateInStore(certificates.intermediate),
                $"Certificate {certificates.intermediate.SubjectName} is not in store. {string.Join('\n', explanationsList)}");
            Assert.True(IsCACertificateInStore(certificates.intermediate2),
                $"Certificate {certificates.intermediate2.SubjectName} is not in store. {string.Join('\n', explanationsList)}");

            throw;
        }
        finally
        {
            CleanupCaStore(certificates.ca, certificates.ca2, certificates.intermediate, certificates.intermediate2);
        }
    }

    private static (X509Certificate2 ca, X509Certificate2 intermediate, X509Certificate2 intermediate2, X509Certificate2 client, X509Certificate2 clientRenewed)
        GenerateAndRenewWithDifferentIntermediate()
    {
        var suffix = GenerateSuffix();
        var caKp = CertificateGenerator.GenerateRSAKeyPair();
        var ca = CertificateGenerator.GenerateRootCACertificate($"CaName-{suffix}", 5, caKp);

        var intermediateKp = CertificateGenerator.GenerateRSAKeyPair();
        var intermediate = CertificateGenerator.GenerateIntermediateCACertificate(ca, caKp, $"{IntermediateName}-{suffix}", 2, intermediateKp);

        var intermediate2Kp = CertificateGenerator.GenerateRSAKeyPair();
        var intermediate2 = CertificateGenerator.GenerateIntermediateCACertificate(ca, caKp, $"{IntermediateName}-{suffix}-2", 2, intermediate2Kp);

        var clientKp = CertificateGenerator.GenerateRSAKeyPair();
        var client = CertificateGenerator.GenerateSignedClientCertificate(intermediate, intermediateKp, $"{ClientName}-{suffix}", 1, clientKp);

        var client2 = CertificateGenerator.GenerateSignedClientCertificate(intermediate2, intermediate2Kp, $"{ClientRenewedName}-{suffix}", 1, clientKp);

        return (ca, intermediate, intermediate2, client, client2);
    }

    private static (X509Certificate2 ca, X509Certificate2 ca2, X509Certificate2 intermediate, X509Certificate2 intermediate2, X509Certificate2 client, X509Certificate2
        clientRenewed)
        GenerateAndRenewWithDifferentChain()
    {
        var suffix = GenerateSuffix();
        var caKp = CertificateGenerator.GenerateRSAKeyPair();
        var ca = CertificateGenerator.GenerateRootCACertificate($"{CaName}-{suffix}", 5, caKp);

        var ca2Kp = CertificateGenerator.GenerateRSAKeyPair();
        var ca2 = CertificateGenerator.GenerateRootCACertificate($"{CaName}-{suffix}-2", 5, ca2Kp);

        var intermediateKp = CertificateGenerator.GenerateRSAKeyPair();
        var intermediate = CertificateGenerator.GenerateIntermediateCACertificate(ca, caKp, $"{IntermediateName}-{suffix}", 2, intermediateKp);

        var intermediate2Kp = CertificateGenerator.GenerateRSAKeyPair();
        var intermediate2 = CertificateGenerator.GenerateIntermediateCACertificate(ca2, ca2Kp, $"{IntermediateName}-{suffix}-2", 2, intermediate2Kp);

        var clientKp = CertificateGenerator.GenerateRSAKeyPair();
        var client = CertificateGenerator.GenerateSignedClientCertificate(intermediate, intermediateKp, $"{ClientName}-{suffix}", 1, clientKp);

        var client2 = CertificateGenerator.GenerateSignedClientCertificate(intermediate2, intermediate2Kp, $"{ClientRenewedName}-{suffix}", 1, clientKp);

        return (ca, ca2, intermediate, intermediate2, client, client2);
    }

    private static (X509Certificate2 ca, X509Certificate2 intermediate, X509Certificate2 client, X509Certificate2 clientRenewed) GenerateAndRenewWithTheSameIntermediate()
    {
        var suffix = GenerateSuffix();
        var caKp = CertificateGenerator.GenerateRSAKeyPair();
        var ca = CertificateGenerator.GenerateRootCACertificate($"{CaName}-{suffix}", 5, caKp);

        var intermediateKp = CertificateGenerator.GenerateRSAKeyPair();
        var intermediate = CertificateGenerator.GenerateIntermediateCACertificate(ca, caKp, $"{IntermediateName}-{suffix}", 2, intermediateKp);

        var clientKp = CertificateGenerator.GenerateRSAKeyPair();
        var client = CertificateGenerator.GenerateSignedClientCertificate(intermediate, intermediateKp, $"{ClientName}-{suffix}", 1, clientKp);
        var client2 = CertificateGenerator.GenerateSignedClientCertificate(intermediate, intermediateKp, $"{ClientRenewedName}-{suffix}", 1, clientKp);

        return (ca, intermediate, client, client2);
    }

    private static (X509Certificate2 client, X509Certificate2 clientRenewed) GenerateAndRenewSelfSigned()
    {
        var suffix = GenerateSuffix();
        var clientKp = CertificateGenerator.GenerateRSAKeyPair();
        var client = CertificateGenerator.GenerateSelfSignedClientCertificate($"{ClientName}-{suffix}", 1, clientKp);
        var client2 = CertificateGenerator.GenerateSelfSignedClientCertificate($"{ClientRenewedName}-{suffix}", 1, clientKp);

        return (client, client2);
    }

    private static void PopulateCaStore(params X509Certificate2[] certificates)
    {
        const int maxRetries = 3;
        foreach (var certificate in certificates)
        {
            var retries = 0;
            while (IsCACertificateInStore(certificate) == false)
            {
                if (retries >= maxRetries)
                    throw new InvalidOperationException($"Failed to add certificate {certificate.Subject} to the certificate authority store");

                using (var store = new X509Store(StoreName.CertificateAuthority, StoreLocation.CurrentUser))
                {
                    store.Open(OpenFlags.ReadWrite);
                    store.Add(certificate);
                }

                Thread.Sleep(73);

                retries++;
            }
        }
    }

    private static bool IsCACertificateInStore(X509Certificate2 certificate)
    {
        using (var store = new X509Store(StoreName.CertificateAuthority, StoreLocation.CurrentUser))
        {
            store.Open(OpenFlags.ReadOnly);
            var certCollection = store.Certificates.Find(
                X509FindType.FindByThumbprint,
                certificate.Thumbprint,
                false);

            return certCollection.Count > 0;
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

    private static string GenerateSuffix()
    {
        var random = new Random();
        var sb = new StringBuilder();
        for (int i = 0; i < 8; i++)
        {
            sb.Append(random.Next(0, 10));
        }

        return sb.ToString();
    }

    private const string CaName = "raven-test-ca";
    private const string IntermediateName = "raven-test-intermediate";
    private const string ClientName = "raven-test-client";
    private const string ClientRenewedName = "raven-test-client-renewed";
}
