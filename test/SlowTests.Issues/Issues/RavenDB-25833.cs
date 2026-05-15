using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Threading.Tasks;
using FastTests;
using Newtonsoft.Json.Linq;
using Raven.Server.Commercial;
using Raven.Server.Commercial.LetsEncrypt;
using Raven.Server.Config;
using Raven.Server.Utils.Features;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues;

public class RavenDB_25833 : RavenTestBase
{
    public RavenDB_25833(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.Setup)]
    public static async Task UnsecuredSetup_ExperimentalFeaturesEnabled_SetsKeysInSettingsJson()
    {
        byte[] zipBytes = await SettingsZipFileHelper.GetSetupZipFileUnsecuredSetup(CreateUnsecuredParameters(enableExperimentalFeatures: true));

        JObject settings = ExtractSettingsJson(zipBytes, "A/settings.json");

        Assert.Equal(nameof(FeaturesAvailability.Experimental), settings[RavenConfiguration.GetKey(x => x.Core.FeaturesAvailability)]?.Value<string>());
        Assert.True(settings[RavenConfiguration.GetKey(x => x.Integrations.PostgreSql.Enabled)]?.Value<bool>());
    }

    [RavenFact(RavenTestCategory.Setup)]
    public static async Task UnsecuredSetup_ExperimentalFeaturesDisabled_DoesNotSetKeysInSettingsJson()
    {
        byte[] zipBytes = await SettingsZipFileHelper.GetSetupZipFileUnsecuredSetup(CreateUnsecuredParameters(enableExperimentalFeatures: false));

        JObject settings = ExtractSettingsJson(zipBytes, "A/settings.json");

        Assert.Null(settings[RavenConfiguration.GetKey(x => x.Core.FeaturesAvailability)]);
        Assert.Null(settings[RavenConfiguration.GetKey(x => x.Integrations.PostgreSql.Enabled)]);
    }

    private static GetSetupZipFileParameters CreateUnsecuredParameters(bool enableExperimentalFeatures)
    {
        return new GetSetupZipFileParameters
        {
            SetupMode = SetupMode.Unsecured,
            ZipOnly = true,
            Progress = new SetupProgressAndResult(_ => { }, SetupMode.Unsecured, zipOnly: true),
            CompleteClusterConfigurationResult = new CompleteClusterConfigurationResult { PublicServerUrl = "http://localhost:8080" },
            UnsecuredSetupInfo = new UnsecuredSetupInfo
            {
                EnableExperimentalFeatures = enableExperimentalFeatures,
                ZipOnly = true,
                LocalNodeTag = "A",
                NodeSetupInfos = new Dictionary<string, NodeInfo>
                {
                    ["A"] = new NodeInfo { Addresses = new List<string>(), Port = 8080, TcpPort = 38888 }
                }
            }
        };
    }

    private static JObject ExtractSettingsJson(byte[] zipBytes, string entryName)
    {
        using MemoryStream ms = new MemoryStream(zipBytes);
        using ZipArchive archive = new ZipArchive(ms, ZipArchiveMode.Read);
        ZipArchiveEntry entry = archive.GetEntry(entryName);
        Assert.NotNull(entry);
        using StreamReader reader = new StreamReader(entry.Open());
        return JObject.Parse(reader.ReadToEnd());
    }
}
