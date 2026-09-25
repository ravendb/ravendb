using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Security.Cryptography.X509Certificates;
using System.Threading.Tasks;
using FastTests;
using Newtonsoft.Json.Linq;
using Raven.Client.Documents.Operations.Configuration;
using Raven.Client.Util;
using Raven.Server.Commercial;
using Raven.Server.Commercial.LetsEncrypt;
using Raven.Server.Config;
using Raven.Server.Utils;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues;

public class RavenDB_26546 : RavenTestBase
{
    public RavenDB_26546(ITestOutputHelper output) : base(output)
    {
    }

    private static readonly string[] SharedCertSans =
    [
        "a.hub.test.local", "b.hub.test.local", "c.hub.test.local",
        "a.sink1.test.local", "b.sink1.test.local", "c.sink1.test.local",
        "*.hub.test.local", "*.sink1.test.local"
    ];

    [RavenFact(RavenTestCategory.Setup)]
    public void Domain_selects_matching_san_over_first_san()
    {
        using var cert = CreateCert(SharedCertSans);
        var setupInfo = CreateSetupInfo("sink1");

        var url = CertificateUtils.GetServerUrlFromCertificate(cert, setupInfo, "B", 443, 38888, out var tcpUrl, out var domain);

        Assert.Equal("https://b.sink1.test.local", url);
        Assert.Equal("tcp://b.sink1.test.local:38888", tcpUrl);
        Assert.Equal("b.sink1.test.local", domain);
    }

    [RavenFact(RavenTestCategory.Setup)]
    public void Domain_selects_matching_wildcard_san_over_first_wildcard()
    {
        using var cert = CreateCert(["*.hub.test.local", "*.sink1.test.local"]);
        var setupInfo = CreateSetupInfo("sink1");

        var url = CertificateUtils.GetServerUrlFromCertificate(cert, setupInfo, "A", 8443, 38888, out var tcpUrl, out var domain);

        Assert.Equal("https://a.sink1.test.local:8443", url);
        Assert.Equal("tcp://a.sink1.test.local:38888", tcpUrl);
        Assert.Equal("sink1.test.local", domain);
    }

    [RavenFact(RavenTestCategory.Setup)]
    public void Explicit_PublicServerUrl_is_kept()
    {
        using var cert = CreateCert(SharedCertSans);
        var setupInfo = CreateSetupInfo("sink1");
        setupInfo.NodeSetupInfos["A"].PublicServerUrl = "https://a.sink1.test.local:443";

        var url = CertificateUtils.GetServerUrlFromCertificate(cert, setupInfo, "A", 443, 38888, out var tcpUrl, out var domain);

        Assert.Equal("https://a.sink1.test.local:443", url);
        Assert.Equal("https://a.sink1.test.local:443", setupInfo.NodeSetupInfos["A"].PublicServerUrl);
        Assert.Equal("tcp://a.sink1.test.local:38888", tcpUrl);
        Assert.Equal("a.sink1.test.local", domain);
    }

    [RavenFact(RavenTestCategory.Setup)]
    public void Without_Domain_first_matching_san_is_used()
    {
        using var cert = CreateCert(SharedCertSans);
        var setupInfo = CreateSetupInfo(domain: null);

        var url = CertificateUtils.GetServerUrlFromCertificate(cert, setupInfo, "A", 443, 38888, out _, out _);

        Assert.Equal("https://a.hub.test.local", url);
    }

    [RavenFact(RavenTestCategory.Setup)]
    public async Task Setup_package_honors_Domain_and_PublicServerUrl()
    {
        using var cert = CreateCert(SharedCertSans);
        var setupInfo = CreateSetupInfo("sink1");
        setupInfo.NodeSetupInfos["C"].PublicServerUrl = "https://c-override.sink1.test.local:443";
        setupInfo.NodeSetupInfos["C"].PublicTcpServerUrl = "tcp://c-override.sink1.test.local:38889";

        var zipBytes = await SettingsZipFileHelper.GetSetupZipFileSecuredSetup(new GetSetupZipFileParameters
        {
            SetupMode = SetupMode.Secured,
            ZipOnly = true,
            Progress = new SetupProgressAndResult(_ => { }, SetupMode.Secured, zipOnly: true),
            SetupInfo = setupInfo,
            CompleteClusterConfigurationResult = new CompleteClusterConfigurationResult
            {
                Domain = "sink1.test.local",
                ServerCert = cert,
                ServerCertBytes = cert.Export(X509ContentType.Pfx),
                ClientCert = cert,
                CertBytes = cert.Export(X509ContentType.Pfx),
                PublicServerUrl = "https://a.sink1.test.local"
            }
        });

        var a = ExtractSettingsJson(zipBytes, "A/settings.json");
        var c = ExtractSettingsJson(zipBytes, "C/settings.json");

        Assert.Equal("https://a.sink1.test.local", a[RavenConfiguration.GetKey(x => x.Core.PublicServerUrl)]?.Value<string>());
        Assert.Equal("tcp://a.sink1.test.local:38888", a[RavenConfiguration.GetKey(x => x.Core.PublicTcpServerUrl)]?.Value<string>());
        Assert.Equal("https://c-override.sink1.test.local:443", c[RavenConfiguration.GetKey(x => x.Core.PublicServerUrl)]?.Value<string>());
        Assert.Equal("tcp://c-override.sink1.test.local:38889", c[RavenConfiguration.GetKey(x => x.Core.PublicTcpServerUrl)]?.Value<string>());
    }

    private static SetupInfo CreateSetupInfo(string domain)
    {
        return new SetupInfo
        {
            Domain = domain,
            RootDomain = "test.local",
            LocalNodeTag = "A",
            ZipOnly = true,
            Environment = StudioConfiguration.StudioEnvironment.None,
            NodeSetupInfos = new Dictionary<string, NodeInfo>
            {
                ["A"] = new() { Port = 443, TcpPort = 38888, Addresses = new List<string>() },
                ["B"] = new() { Port = 443, TcpPort = 38888, Addresses = new List<string>() },
                ["C"] = new() { Port = 443, TcpPort = 38889, Addresses = new List<string>() }
            }
        };
    }

    private static X509Certificate2 CreateCert(IEnumerable<string> sans)
    {
        var caCert = CertificateUtils.CreateCertificateAuthorityCertificate("RavenDB_26546 CA", out var caSubjectName);

        CertificateUtils.CreateSelfSignedCertificateBasedOnPrivateKey(
            commonNameValue: "a.hub.test.local",
            issuerCN: caSubjectName,
            issuerKeyPair: (caCert.GetExportableRsaPrivateKey(), caCert.GetRSAPublicKey()),
            isClientCertificate: false,
            isCaCertificate: false,
            notAfter: DateTime.UtcNow.Date.AddMonths(1),
            certBytes: out var certBytes,
            sans: sans);

        return CertificateLoaderUtil.CreateCertificate(certBytes, flags: CertificateLoaderUtil.FlagsForExport);
    }

    private static JObject ExtractSettingsJson(byte[] zipBytes, string entryName)
    {
        using var ms = new MemoryStream(zipBytes);
        using var archive = new ZipArchive(ms, ZipArchiveMode.Read);
        var entry = archive.GetEntry(entryName);
        Assert.NotNull(entry);
        using var reader = new StreamReader(entry.Open());
        return JObject.Parse(reader.ReadToEnd());
    }
}
