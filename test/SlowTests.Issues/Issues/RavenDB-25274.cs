using System;
using System.IO;
using System.IO.Compression;
using System.Security.Cryptography.X509Certificates;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.ServerWide.Operations.Certificates;
using Raven.Client.Util;
using Raven.Server.Web.Authentication;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues;

public class RavenDB_25274 : RavenTestBase
{
    public RavenDB_25274(ITestOutputHelper output) : base(output)
    {
    }

    [RavenTheory(RavenTestCategory.Certificates | RavenTestCategory.Security)]
    [InlineData(null)]
    [InlineData("sec3tp@ssw0rd")]
    public async Task CanGenerateCertificate(string password)
    {
        var certificates = Certificates.SetupServerAuthentication();
        using var store = GetDocumentStore(new Options
        {
            CreateDatabase = true,
            AdminCertificate = certificates.ServerCertificateForCommunication.Value,
            ClientCertificate = certificates.ServerCertificateForCommunication.Value
        });

        var notAfter = DateTime.UtcNow.AddDays(7);
        const string name = "test-cert";
        var zip = await AdminCertificatesHandler.GenerateCertificateInternal(new CertificateDefinition
            {
                Password = password,
                Name = name,
                NotAfter = notAfter
            },
            Server.ServerStore,
            null,
            RaftIdGenerator.NewId());

        using var ms = new MemoryStream(zip);
        using var archive = new ZipArchive(ms);
        var entry = archive.GetEntry($"{name}.pfx");
        Assert.NotNull(entry);

        byte[] certificateBytes;
        using (var entryStream = entry.Open())
        using (var ms2 = new MemoryStream())
        {
            await entryStream.CopyToAsync(ms2);
            certificateBytes = ms2.ToArray();
        }

#pragma warning disable SYSLIB0057
        var certificate = new X509Certificate2(certificateBytes, password);
#pragma warning restore SYSLIB0057
        Assert.Equal(notAfter.Date, certificate.NotAfter.ToUniversalTime().Date);
    }
}
