using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Threading.Tasks;
using Org.BouncyCastle.Security;
using Raven.Client;
using Raven.Client.ServerWide.Operations.Certificates;
using Raven.Client.Util;
using Raven.Server.Commercial.LetsEncrypt;
using Raven.Server.Utils;
using Sparrow.Server;
using Tests.Infrastructure;
using Tests.Infrastructure.Utils;
using Xunit;

namespace SlowTests.Issues;

public class RavenDB_24495 : ClusterTestBase
{
    public RavenDB_24495(ITestOutputHelper output) : base(output)
    {
    }

    [RavenTheory(RavenTestCategory.Certificates)]
    [InlineData(true)]
    [InlineData(false)]
    public async Task CanReplaceServerCertificate_WithBouncyCastleGeneratedCertificate(bool with2Eku)
    {
        var result = await CreateRaftClusterWithSsl(3, with2Eku: with2Eku);
        var suffix = Guid.NewGuid().ToString().Split('-')[0];
        var newCertBytes = CertificateUtils.CreateSelfSignedTestCertificate($"server-bc-{suffix}", $"replace-server-cert-test-{suffix}", with2Eku: with2Eku);
        
        var first = result.Certificates.ServerCertificate.Value.Thumbprint;
        var certForCommunication = result.Certificates.ServerCertificateForCommunication.Value;

        var mre = new AsyncManualResetEvent();
        Server.ServerCertificateChanged += (sender, args) => mre.Set();
        
        using (var store = GetDocumentStore(new Options { AdminCertificate = certForCommunication, ClientCertificate = certForCommunication }))
        {
            var op = new ReplaceClusterCertificateOperation(newCertBytes, replaceImmediately: false);
            await store.Maintenance.Server.SendAsync(op);
        }

        await mre.WaitAsync(TimeSpan.FromMinutes(4));
        Assert.NotEqual(first, Server.Certificate.ServerCertificate.Thumbprint);
    }

    [RavenFact(RavenTestCategory.Certificates)]
    public async Task CreateZipArchive_ShouldHaveTheSameContent()
    {
        var suffix = Guid.NewGuid().ToString().Split('-')[0];
        var certName = $"test-{suffix}";
        var clientCertBytes = CertificateUtils.CreateSelfSignedTestCertificate(certName, $"zip-archive-test-{suffix}", with2Eku: true);

        using var ms = new MemoryStream();
        using (var archive = new ZipArchive(ms, ZipArchiveMode.Create, true))
        {
            await LetsEncryptCertificateUtil.WriteCertificateAsPemToZipArchiveAsync(certName, clientCertBytes, null, archive);
        }

        var dnResult = ms.ToArray();

        using var ms2 = new MemoryStream();
        using (var archive = new ZipArchive(ms2, ZipArchiveMode.Create, true))
        {
            await BouncyCastleCertificateUtils.WriteCertificateAsPemToZipArchiveAsync(certName, clientCertBytes, null, archive);
        }

        var bcResult = ms2.ToArray();

        string dnExtractedCert = PemUtils.NormalizePemContent(await ExtractFileFromZipAsync(dnResult, $"{certName}.crt"));
        string bcExtractedCert = PemUtils.NormalizePemContent(await ExtractFileFromZipAsync(bcResult, $"{certName}.crt"));
        
        string dnExtractedKey = PemUtils.NormalizePemContent(await ExtractFileFromZipAsync(dnResult, $"{certName}.key"));
        string bcExtractedKey = PemUtils.NormalizePemContent(await ExtractFileFromZipAsync(bcResult, $"{certName}.key"));

        Assert.Equal(bcExtractedCert, dnExtractedCert);
        Assert.Equal(bcExtractedKey, dnExtractedKey);
    }

    [RavenFact(RavenTestCategory.Certificates)]
    public void CreateSelfSignedTestCertificate_ShouldProduceEquivalentProperties()
    {
        var cn = $"test-{Guid.NewGuid():N}";

        // Act
        var bcBytes = BouncyCastleCertificateUtils.CreateSelfSignedTestCertificate(cn, issuerName: null, with2Eku: true);
        var dotnetBytes = CertificateUtils.CreateSelfSignedTestCertificate(cn, issuerName: null, with2Eku: true);

#pragma warning disable SYSLIB0057
        using var bcCert = new X509Certificate2(bcBytes);
        using var dnCert = new X509Certificate2(dotnetBytes);
#pragma warning restore SYSLIB0057

        // Assert basic equivalence
        Assert.Equal($"CN={cn}", GetSubjectCN(bcCert));
        Assert.Equal(GetSubjectCN(bcCert), GetSubjectCN(dnCert));
        Assert.Equal(GetIssuerCN(bcCert), GetIssuerCN(dnCert));

        // Not a CA (either no BasicConstraints extension or explicitly not a CA)
        Assert.True(IsEndEntity(bcCert));
        Assert.True(IsEndEntity(dnCert));

        // Key usage contains DigitalSignature and KeyEncipherment
        Assert.True(GetKeyUsage(bcCert).HasFlag(X509KeyUsageFlags.DigitalSignature));
        Assert.True(GetKeyUsage(bcCert).HasFlag(X509KeyUsageFlags.KeyEncipherment));
        Assert.True(GetKeyUsage(dnCert).HasFlag(X509KeyUsageFlags.DigitalSignature));
        Assert.True(GetKeyUsage(dnCert).HasFlag(X509KeyUsageFlags.KeyEncipherment));

        // EKUs include both client and server when with2Eku = true
        var bcEkus = GetEkus(bcCert);
        var dnEkus = GetEkus(dnCert);
        Assert.Contains(Constants.Certificates.ClientAuthenticationOid, bcEkus);
        Assert.Contains(Constants.Certificates.ServerAuthenticationOid, bcEkus);
        Assert.Contains(Constants.Certificates.ClientAuthenticationOid, dnEkus);
        Assert.Contains(Constants.Certificates.ServerAuthenticationOid, dnEkus);

        // SANs contain provided entries
        var expectedSans = new[] { cn, "localhost", $"*.{cn}" };
        var bcSans = GetSanDnsNames(bcCert).ToHashSet(StringComparer.OrdinalIgnoreCase);
        var dnSans = GetSanDnsNames(dnCert).ToHashSet(StringComparer.OrdinalIgnoreCase);
        foreach (var san in expectedSans)
        {
            Assert.Contains(san, bcSans);
            Assert.Contains(san, dnSans);
        }

        // Validity similar (allow for small differences)
        Assert.True((bcCert.NotAfter - dnCert.NotAfter).Duration() < TimeSpan.FromDays(2));
    }

    [RavenFact(RavenTestCategory.Certificates)]
    public void CreateSelfSignedClientCertificate_ShouldHaveClientEku()
    {
        var cn = $"srv-{Guid.NewGuid():N}";
        var clientCn = $"client-{Guid.NewGuid():N}";

        var dnServerBytes = CertificateUtils.CreateSelfSignedTestCertificate(cn, issuerName: null, with2Eku: true);
        using var serverCert = CertificateLoaderUtil.CreateCertificate(dnServerBytes, flags: CertificateLoaderUtil.FlagsForExport);

        var serverPrivateKey = serverCert.GetExportableRsaPrivateKey();
        var bcClient = BouncyCastleCertificateUtils.CreateSelfSignedClientCertificate(clientCn, serverCert, DotNetUtilities.GetRsaKeyPair(serverPrivateKey).Private, out byte[] _, DateTime.UtcNow.AddMonths(3));
        var dnClient = CertificateUtils.CreateSelfSignedClientCertificate(clientCn, serverCert, serverPrivateKey, out byte[] _, DateTime.UtcNow.AddMonths(3));

        using (bcClient)
        using (dnClient)
        {
            Assert.Contains(Constants.Certificates.ClientAuthenticationOid, GetEkus(bcClient));
            Assert.Contains(Constants.Certificates.ClientAuthenticationOid, GetEkus(dnClient));

            Assert.Equal($"CN={cn}", bcClient.Issuer);
            Assert.Equal($"CN={cn}", dnClient.Issuer);
        }
    }

    [RavenFact(RavenTestCategory.Certificates)]
    public void CreateClientCertificateFromServerCertificate_ShouldEmbedServerCertAndHaveClientEku()
    {
        var cn = $"srv-{Guid.NewGuid():N}";

        var dnServerBytes = CertificateUtils.CreateSelfSignedTestCertificate(cn, issuerName: null, with2Eku: true);
        using var serverCert = CertificateLoaderUtil.CreateCertificate(dnServerBytes, flags: CertificateLoaderUtil.FlagsForExport);

        var bcClient = BouncyCastleCertificateUtils.CreateClientCertificateFromServerCertificate(serverCert, out byte[] _);
        var dnClient = CertificateUtils.CreateClientCertificateFromServerCertificate(serverCert, out byte[] _);

        using (bcClient)
        using (dnClient)
        {
            Assert.Contains(Constants.Certificates.ClientAuthenticationOid, GetEkus(bcClient));
            Assert.Contains(Constants.Certificates.ClientAuthenticationOid, GetEkus(dnClient));

            var bcExtracted = CertificateUtils.ExtractServerCertificateFromExtension(bcClient);
            var dnExtracted = CertificateUtils.ExtractServerCertificateFromExtension(dnClient);

            Assert.NotNull(bcExtracted);
            Assert.NotNull(dnExtracted);

            using (bcExtracted)
            using (dnExtracted)
            {
                Assert.Equal(serverCert.Thumbprint, bcExtracted.Thumbprint);
                Assert.Equal(serverCert.Thumbprint, dnExtracted.Thumbprint);
            }
        }
    }

    [RavenFact(RavenTestCategory.Certificates)]
    public void CreateCertificateAuthorityCertificate_ShouldBeComparableAsCA()
    {
        var cn = $"ca-{Guid.NewGuid():N}";
        var bcCa = BouncyCastleCertificateUtils.CreateCertificateAuthorityCertificate(cn, out _, out _);
        var dnCa = CertificateUtils.CreateCertificateAuthorityCertificate(cn, out _);

        using (bcCa)
        using (dnCa)
        {
            Assert.Equal($"CN={cn}", GetSubjectCN(bcCa));
            Assert.Equal(GetSubjectCN(bcCa), GetSubjectCN(dnCa));

            var bcBc = GetBasicConstraintsOrNull(bcCa);
            var dnBc = GetBasicConstraintsOrNull(dnCa);
            Assert.NotNull(bcBc);
            Assert.NotNull(dnBc);
            Assert.True(bcBc!.CertificateAuthority);
            Assert.True(dnBc!.CertificateAuthority);

            Assert.True(GetKeyUsage(bcCa).HasFlag(X509KeyUsageFlags.KeyCertSign));
            Assert.True(GetKeyUsage(bcCa).HasFlag(X509KeyUsageFlags.CrlSign));
            Assert.True(GetKeyUsage(dnCa).HasFlag(X509KeyUsageFlags.KeyCertSign));
            Assert.True(GetKeyUsage(dnCa).HasFlag(X509KeyUsageFlags.CrlSign));
        }
    }

    private static string GetSubjectCN(X509Certificate2 cert) => cert.Subject;
    private static string GetIssuerCN(X509Certificate2 cert) => cert.Issuer;

    private static X509BasicConstraintsExtension GetBasicConstraintsOrNull(X509Certificate2 cert)
    {
        return (X509BasicConstraintsExtension)cert.Extensions
            .OfType<X509Extension>()
            .FirstOrDefault(e => e.Oid?.Value == "2.5.29.19");
    }

    private static bool IsEndEntity(X509Certificate2 cert)
    {
        var bc = GetBasicConstraintsOrNull(cert);
        return bc == null || bc.CertificateAuthority == false;
    }

    private static X509KeyUsageFlags GetKeyUsage(X509Certificate2 cert)
    {
        var ext = (X509KeyUsageExtension)cert.Extensions
            .OfType<X509Extension>()
            .First(e => e.Oid?.Value == "2.5.29.15");
        return ext.KeyUsages;
    }

    private static IEnumerable<string> GetEkus(X509Certificate2 cert)
    {
        var ext = (X509EnhancedKeyUsageExtension)cert.Extensions
            .OfType<X509Extension>()
            .FirstOrDefault(e => e.Oid?.Value == "2.5.29.37");
        if (ext == null) yield break;
        foreach (var oid in ext.EnhancedKeyUsages)
            yield return oid.Value;
    }

    private static IEnumerable<string> GetSanDnsNames(X509Certificate2 cert)
    {
#if NET8_0_OR_GREATER
        var san = (X509SubjectAlternativeNameExtension)cert.Extensions
            .OfType<X509Extension>()
            .FirstOrDefault(e => e.Oid?.Value == "2.5.29.17");
        if (san != null)
        {
            foreach (var dns in san.EnumerateDnsNames())
                yield return dns;
            yield break;
        }
#endif
        // Fallback: attempt to parse raw SAN extension minimally for DNS= entries
        var sanExt = cert.Extensions["2.5.29.17"];
        if (sanExt != null)
        {
            var raw = sanExt.Format(true);
            // Raw text may contain DNS Name= entries depending on platform
            foreach (var line in raw.Split(new[] { '\r', '\n' }, StringSplitOptions.RemoveEmptyEntries))
            {
                const string marker = "DNS Name=";
                var idx = line.IndexOf(marker, StringComparison.OrdinalIgnoreCase);
                if (idx >= 0)
                    yield return line.Substring(idx + marker.Length).Trim();
            }
        }
    }

    public static async Task<string> ExtractFileFromZipAsync(byte[] zipArchiveBytes, string fileName)
    {
        using var stream = new MemoryStream(zipArchiveBytes);

        using var archive = new ZipArchive(stream, ZipArchiveMode.Read);

        var entry = archive.GetEntry(fileName);

        if (entry == null)
        {
            Console.WriteLine($"Error: File entry '{fileName}' not found in the archive.");
            return string.Empty;
        }

        using var entryStream = entry.Open();

        using var reader = new StreamReader(entryStream, Encoding.UTF8);
        return await reader.ReadToEndAsync();
    }
}
