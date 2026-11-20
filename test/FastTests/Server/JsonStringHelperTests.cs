using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Reflection;
using Newtonsoft.Json;
using Raven.Client.Documents.Operations.ETL.ElasticSearch;
using Raven.Client.Documents.Operations.ETL.Queue;
using Raven.Client.Documents.Smuggler;
using Raven.Client.ServerWide.Operations;
using Raven.Server.Commercial;
using Raven.Server.Documents.Studio;
using Raven.Server.Smuggler.Documents.Data;
using Raven.Server.Utils;
using Tests.Infrastructure;
using Xunit;

namespace FastTests.Server;

public class JsonStringHelperTests(ITestOutputHelper output) : NoDisposalNeeded(output)
{
    [RavenTheory(RavenTestCategory.Configuration)]
    [InlineData("{}")]
    [InlineData("[]")]
    [InlineData("{\"ServerUrl\":\"https://a.example.com:443\",\"Setup.Mode\":\"LetsEncrypt\",\"DataDir\":\"C:\\\\Raven\\\\Data\"}")]
    [InlineData("{\"Security\":{\"Certificate\":{\"Path\":\"cluster.server.certificate.pfx\",\"Password\":null}},\"Ports\":[443,38888],\"Enabled\":true,\"Disabled\":false}")]
    [InlineData("{\"Name\":\"Zażółć gęślą jaźń <b>&'\\\"+\",\"Empty\":{},\"None\":[],\"Nested\":[{\"A\":1},{\"B\":[1,2,{\"C\":-3}]}]}")]
    [InlineData("{\"Id\":\"a5a5a5a5-0000-0000-0000-000000000000\",\"Keys\":[\"abc+/==\"],\"MaxClusterSize\":3,\"Ratio\":1.5}")]
    public void IndentsLikeNewtonsoft(string json)
    {
        Assert.Equal(IndentWithNewtonsoft(json), JsonStringHelper.Indent(json));
    }

    public static TheoryData<object> Payloads => new()
    {
        CreateLicense(),
        new RegistrationInfo
        {
            License = CreateLicense(),
            Domain = "żółw",
            RootDomain = "development.run",
            Challenge = "a+b/c==",
            SubDomains = [new RegistrationNodeInfo { SubDomain = "a", Ips = ["127.0.0.1", "::1"] }]
        },
        new LeaseLicenseInfo
        {
            License = CreateLicense(),
            BuildInfo = new BuildNumber { BuildVersion = 80, ProductVersion = "8.0", CommitHash = "abc", FullVersion = "8.0.0", AssemblyVersion = "8.0.0.80" },
            ClusterId = "cluster",
            UtilizedCores = 3,
            NodeTag = "A",
            OsInfo = new OsInfo { Type = OSType.Linux, FullName = "Linux <x>", Version = "6.1", BuildVersion = "1", Is64Bit = true }
        },
        new UserRegistrationInfo { FirstName = "A", LastName = "B", Email = "a@b.c", Company = null, BuildInfo = new BuildNumber() },
        new FeedbackForm
        {
            Message = "it's \"great\" & <fast>",
            Product = new FeedbackForm.FeedbackProduct { Name = "RavenDB", Version = "8.0" },
            User = new FeedbackForm.FeedbackUser { Name = "A", Email = "a@b.c" }
        },
        new DatabaseSmugglerOptionsServerSide(Raven.Server.Routing.AuthorizationStatus.DatabaseAdmin)
        {
            OperateOnTypes = DatabaseItemType.Documents | DatabaseItemType.Indexes,
            Collections = ["Orders"],
            TransformScript = "this.A = 1;",
            MaxStepsForTransformScript = 10
        },
        new Dictionary<string, object> { ["Message"] = "m", ["Count"] = 3, ["When"] = new DateTime(2026, 1, 2, 3, 4, 5, DateTimeKind.Utc), ["Missing"] = null },
        new { Message = "error", Response = (object)null, Error = "boom", Type = typeof(InvalidOperationException).FullName }
    };

    [RavenTheory(RavenTestCategory.Configuration)]
    [MemberData(nameof(Payloads))]
    public void SerializesLikeNewtonsoft(object payload)
    {
        Assert.Equal(JsonConvert.SerializeObject(payload), JsonStringHelper.Serialize(payload));
        Assert.Equal(JsonConvert.SerializeObject(payload, Formatting.Indented), JsonStringHelper.Serialize(payload, indented: true));
    }

    [RavenFact(RavenTestCategory.Licensing)]
    public void ReadsLicensePublicKeyWithDifferentlyCasedName()
    {
        const string json = "{\"RSAKeyValue\":{\"Modulus\":\"AQIDBA==\",\"Exponent\":\"AQAB\"}}";

        RSAPublicParameters expected = JsonConvert.DeserializeObject<RSAPublicParameters>(json);
        RSAPublicParameters actual = JsonStringHelper.Deserialize<RSAPublicParameters>(json);

        Assert.Equal(expected.RsaKeyValue.Modulus, actual.RsaKeyValue.Modulus);
        Assert.Equal(expected.RsaKeyValue.Exponent, actual.RsaKeyValue.Exponent);
    }

    [RavenFact(RavenTestCategory.Etl)]
    public void ReadsConnectionSettingsLikeNewtonsoft()
    {
        const string kafka = "{\"BootstrapServers\":\"localhost:9092\",\"ConnectionOptions\":{\"security.protocol\":\"SSL\"},\"UseRavenCertificate\":true}";
        Assert.Equal(JsonConvert.SerializeObject(JsonConvert.DeserializeObject<KafkaConnectionSettings>(kafka)),
            JsonConvert.SerializeObject(JsonStringHelper.Deserialize<KafkaConnectionSettings>(kafka)));

        const string azure = "{\"EntraId\":{\"StorageAccountName\":\"a\",\"TenantId\":\"t\",\"ClientId\":\"c\",\"ClientSecret\":\"s\"},\"ConnectionString\":null,\"Passwordless\":null}";
        Assert.Equal(JsonConvert.SerializeObject(JsonConvert.DeserializeObject<AzureQueueStorageConnectionSettings>(azure)),
            JsonConvert.SerializeObject(JsonStringHelper.Deserialize<AzureQueueStorageConnectionSettings>(azure)));

        const string elastic = "{\"ApiKey\":{\"ApiKeyId\":\"id\",\"ApiKey\":\"key\"},\"Basic\":{\"Username\":\"u\",\"Password\":\"p\"},\"Certificate\":{\"CertificatesBase64\":[\"AQID\"]}}";
        Assert.Equal(JsonConvert.SerializeObject(JsonConvert.DeserializeObject<Authentication>(elastic)),
            JsonConvert.SerializeObject(JsonStringHelper.Deserialize<Authentication>(elastic)));

        Assert.Null(JsonStringHelper.Deserialize<KafkaConnectionSettings>(null));
        Assert.Null(JsonStringHelper.Deserialize<KafkaConnectionSettings>(""));
        Assert.Null(JsonStringHelper.Deserialize<KafkaConnectionSettings>("null"));
    }

    [RavenFact(RavenTestCategory.Certificates)]
    public void LetsEncryptAccountCacheWrittenByNewtonsoftIsReadBack()
    {
        Type cacheType = typeof(LetsEncryptClient).GetNestedType("RegistrationCache", BindingFlags.NonPublic);
        Type certificateType = typeof(LetsEncryptClient).GetNestedType("CertificateCache", BindingFlags.NonPublic);

        object cache = Activator.CreateInstance(cacheType, nonPublic: true);
        cacheType.GetField("AccountKey").SetValue(cache, new byte[] { 1, 2, 3, 250 });
        cacheType.GetField("Id").SetValue(cache, "account/1");
        cacheType.GetField("Location").SetValue(cache, new Uri("https://acme.example.com/acct/1"));

        object certificate = Activator.CreateInstance(certificateType, nonPublic: true);
        certificateType.GetField("Cert").SetValue(certificate, "-----BEGIN CERTIFICATE-----\nAAAA\n-----END CERTIFICATE-----");
        certificateType.GetField("Private").SetValue(certificate, new byte[] { 9, 8, 7 });
        var certificates = (IDictionary)cacheType.GetField("CachedCerts").GetValue(cache);
        certificates["A.Example.com"] = certificate;

        string writtenByNewtonsoft = JsonConvert.SerializeObject(cache, Formatting.Indented);
        Assert.Equal(writtenByNewtonsoft, JsonStringHelper.Serialize(cache, indented: true));

        object readBack = typeof(JsonStringHelper).GetMethod(nameof(JsonStringHelper.Deserialize)).MakeGenericMethod(cacheType).Invoke(null, [writtenByNewtonsoft]);

        Assert.Equal(writtenByNewtonsoft, JsonConvert.SerializeObject(readBack, Formatting.Indented));
        var readCertificates = (IDictionary)cacheType.GetField("CachedCerts").GetValue(readBack);
        Assert.True(readCertificates.Contains("a.example.COM"));
    }

    private static License CreateLicense()
    {
        return new License
        {
            Id = new Guid("a5a5a5a5-0000-0000-0000-000000000001"),
            Name = "Żółć & Sons <dev>",
            Keys = ["abc+/==", "def"]
        };
    }

    private static string IndentWithNewtonsoft(string json)
    {
        using (var stringReader = new StringReader(json))
        using (var stringWriter = new StringWriter())
        {
            var jsonReader = new JsonTextReader(stringReader) { DateParseHandling = DateParseHandling.None };
            var jsonWriter = new JsonTextWriter(stringWriter) { Formatting = Formatting.Indented };
            jsonWriter.WriteToken(jsonReader);
            return stringWriter.ToString();
        }
    }
}
