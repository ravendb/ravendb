using System;
using System.Collections.Generic;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Threading;
using FastTests;
using Raven.Server.Config.Categories;
using Raven.Server.Utils;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;
using System.Security.Cryptography;

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
            Assert.True(IsCACertificateInStore(certificates.ca), $"Certificate {certificates.ca.SubjectName} is not in store. {string.Join('\n', explanationsList)}");
            Assert.True(IsCACertificateInStore(certificates.intermediate),
                $"Certificate {certificates.intermediate.SubjectName} is not in store. {string.Join('\n', explanationsList)}");
            Assert.True(IsCACertificateInStore(certificates.intermediate2),
                $"Certificate {certificates.intermediate2.SubjectName} is not in store. {string.Join('\n', explanationsList)}");

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
        var ca = CertificateUtils.CreateCertificateAuthorityCertificate($"CaName-{suffix}",  out _);

        CertificateUtils.CreateSelfSignedCertificateBasedOnPrivateKey(
            commonNameValue: $"{IntermediateName}-{suffix}",
            issuerCN: ca.SubjectName,
            issuerKeyPair: (ca.GetExportableRsaPrivateKey(), ca.GetRSAPublicKey()),
            isClientCertificate: false,
            isCaCertificate: true,
            notAfter: DateTime.UtcNow.Date.AddYears(2),
            certBytes: out var intermediateBytes);
        var intermediate = new X509Certificate2(intermediateBytes);

        CertificateUtils.CreateSelfSignedCertificateBasedOnPrivateKey(
            commonNameValue: $"{IntermediateName}-{suffix}-2",
            issuerCN: ca.SubjectName,
            issuerKeyPair: (ca.GetExportableRsaPrivateKey(), ca.GetRSAPublicKey()),
            isClientCertificate: false,
            isCaCertificate: true,
            notAfter: DateTime.UtcNow.Date.AddYears(2),
            certBytes: out var intermediate2Bytes);
        var intermediate2 = new X509Certificate2(intermediate2Bytes);

        var clientKp = RSA.Create(2048);

        CertificateUtils.CreateSelfSignedCertificateBasedOnPrivateKey(
            commonNameValue: $"{ClientName}-{suffix}",
            issuerCN: intermediate.SubjectName,
            issuerKeyPair: (intermediate.GetRSAPrivateKey(), intermediate.GetRSAPublicKey()),
            isClientCertificate: true,
            isCaCertificate: false,
            notAfter: DateTime.UtcNow.Date.AddYears(1),
            certBytes: out var clientBytes,
            subjectPrivateKey: clientKp);
        var client = new X509Certificate2(clientBytes);

        CertificateUtils.CreateSelfSignedCertificateBasedOnPrivateKey(
            commonNameValue: $"{ClientRenewedName}-{suffix}",
            issuerCN: intermediate2.SubjectName,
            issuerKeyPair: (intermediate2.GetRSAPrivateKey(), intermediate2.GetRSAPublicKey()),
            isClientCertificate: true,
            isCaCertificate: false,
            notAfter: DateTime.UtcNow.Date.AddYears(1),
            certBytes: out var client2Bytes,
            subjectPrivateKey: clientKp);
        var client2 = new X509Certificate2(client2Bytes);

        return (ca, intermediate, intermediate2, client, client2);
    }

    private static (X509Certificate2 ca, X509Certificate2 ca2, X509Certificate2 intermediate, X509Certificate2 intermediate2, X509Certificate2 client, X509Certificate2
        clientRenewed)
        GenerateAndRenewWithDifferentChain()
    {
        var suffix = GenerateSuffix();
        var ca = CertificateUtils.CreateCertificateAuthorityCertificate($"{CaName}-{suffix}", out _);
        var ca2 = CertificateUtils.CreateCertificateAuthorityCertificate($"{CaName}-{suffix}-2", out _, generateNewKeyPair: true);

        CertificateUtils.CreateSelfSignedCertificateBasedOnPrivateKey(
            commonNameValue: $"{IntermediateName}-{suffix}",
            issuerCN: ca.SubjectName,
            issuerKeyPair: (ca.GetRSAPrivateKey(), ca.GetRSAPublicKey()),
            isClientCertificate: false,
            isCaCertificate: true,
            notAfter: DateTime.UtcNow.Date.AddYears(2),
            certBytes: out var intermediateBytes);
        var intermediate = new X509Certificate2(intermediateBytes);

        CertificateUtils.CreateSelfSignedCertificateBasedOnPrivateKey(
            commonNameValue: $"{IntermediateName}-{suffix}-2",
            issuerCN: ca2.SubjectName,
            issuerKeyPair: (ca2.GetRSAPrivateKey(), ca2.GetRSAPublicKey()),
            isClientCertificate: false,
            isCaCertificate: true,
            notAfter: DateTime.UtcNow.Date.AddYears(2),
            certBytes: out var intermediate2Bytes);
        var intermediate2 = new X509Certificate2(intermediate2Bytes);

        var clientKp = RSA.Create(2048);

        CertificateUtils.CreateSelfSignedCertificateBasedOnPrivateKey(
            commonNameValue: $"{ClientName}-{suffix}",
            issuerCN: intermediate.SubjectName,
            issuerKeyPair: (intermediate.GetRSAPrivateKey(), intermediate.GetRSAPublicKey()),
            isClientCertificate: true,
            isCaCertificate: false,
            notAfter: DateTime.UtcNow.Date.AddYears(1),
            certBytes: out var clientBytes,
            subjectPrivateKey: clientKp);
        var client = new X509Certificate2(clientBytes);

        CertificateUtils.CreateSelfSignedCertificateBasedOnPrivateKey(
            commonNameValue: $"{ClientRenewedName}-{suffix}",
            issuerCN: intermediate2.SubjectName,
            issuerKeyPair: (intermediate2.GetRSAPrivateKey(), intermediate2.GetRSAPublicKey()),
            isClientCertificate: true,
            isCaCertificate: false,
            notAfter: DateTime.UtcNow.Date.AddYears(1),
            certBytes: out var client2Bytes,
            subjectPrivateKey: clientKp);
        var client2 = new X509Certificate2(client2Bytes);

        return (ca, ca2, intermediate, intermediate2, client, client2);
    }

    private static (X509Certificate2 ca, X509Certificate2 intermediate, X509Certificate2 client, X509Certificate2 clientRenewed) GenerateAndRenewWithTheSameIntermediate()
    {
        var suffix = GenerateSuffix();
        var ca = CertificateUtils.CreateCertificateAuthorityCertificate($"{CaName}-{suffix}", out _);

        CertificateUtils.CreateSelfSignedCertificateBasedOnPrivateKey(
            commonNameValue: $"{IntermediateName}-{suffix}",
            issuerCN: ca.SubjectName,
            issuerKeyPair: (ca.GetExportableRsaPrivateKey(), ca.GetRSAPublicKey()),
            isClientCertificate: false,
            isCaCertificate: true,
            notAfter: DateTime.UtcNow.Date.AddYears(2),
            certBytes: out var intermediateBytes);
        var intermediate = new X509Certificate2(intermediateBytes);

        var clientKp = RSA.Create(2048);

        CertificateUtils.CreateSelfSignedCertificateBasedOnPrivateKey(
            commonNameValue: $"{ClientName}-{suffix}",
            issuerCN: intermediate.SubjectName,
            issuerKeyPair: (intermediate.GetRSAPrivateKey(), intermediate.GetRSAPublicKey()),
            isClientCertificate: true,
            isCaCertificate: false,
            notAfter: DateTime.UtcNow.Date.AddYears(1),
            certBytes: out var clientBytes,
            subjectPrivateKey: clientKp);
        var client = new X509Certificate2(clientBytes);

        CertificateUtils.CreateSelfSignedCertificateBasedOnPrivateKey(
            commonNameValue: $"{ClientRenewedName}-{suffix}",
            issuerCN: intermediate.SubjectName,
            issuerKeyPair: (intermediate.GetRSAPrivateKey(), intermediate.GetRSAPublicKey()),
            isClientCertificate: true,
            isCaCertificate: false,
            notAfter: DateTime.UtcNow.Date.AddYears(1),
            certBytes: out var client2Bytes,
            subjectPrivateKey: clientKp);
        var client2 = new X509Certificate2(client2Bytes);

        return (ca, intermediate, client, client2);
    }

    private static (X509Certificate2 client, X509Certificate2 clientRenewed) GenerateAndRenewSelfSigned()
    {
        var suffix = GenerateSuffix();
        var clientKp = RSA.Create(2048);

        CertificateUtils.CreateSelfSignedCertificateBasedOnPrivateKey(
            commonNameValue: $"{ClientName}-{suffix}",
            issuerCN: new X500DistinguishedName($"CN={ClientName}-{suffix}"),
            issuerKeyPair: (clientKp, clientKp),
            isClientCertificate: true,
            isCaCertificate: false,
            notAfter: DateTime.UtcNow.Date.AddYears(1),
            certBytes: out var clientBytes,
            subjectPrivateKey: clientKp);
        var client = new X509Certificate2(clientBytes);

        CertificateUtils.CreateSelfSignedCertificateBasedOnPrivateKey(
            commonNameValue: $"{ClientRenewedName}-{suffix}",
            issuerCN: new X500DistinguishedName($"CN={ClientName}-{suffix}"),
            issuerKeyPair: (clientKp, clientKp),
            isClientCertificate: true,
            isCaCertificate: false,
            notAfter: DateTime.UtcNow.Date.AddYears(1),
            certBytes: out var client2Bytes,
            subjectPrivateKey: clientKp);
        var client2 = new X509Certificate2(client2Bytes);

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
                
                    // Strip the private key. macOS Keychain crashes if you try to add 
                    // an ephemeral private key. Validation only needs the public cert anyway.
                    using (var publicOnlyCert = new X509Certificate2(certificate.Export(X509ContentType.Cert)))
                    {
                        store.Add(publicOnlyCert);
                    }
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
