using Raven.Server.Documents.PeriodicBackup.Aws;
using SlowTests.Server.Documents.PeriodicBackup.Restore;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Documents.PeriodicBackup;

public class CustomS3 : RestoreFromS3
{
    public CustomS3(ITestOutputHelper output) : base(output, isCustom: true)
    {
    }

    [CustomS3Fact]
    public void can_use_custom_region()
    {
        const string customUrl = "https://s3.pl-waw.scw.cloud";
        const string customRegion = "pl-waw";

        var settings = GetS3Settings();
        settings.CustomServerUrl = customUrl;
        settings.AwsRegionName = customRegion;

        using (var client = new RavenAwsS3Client(settings, DefaultConfiguration))
        {
            Assert.Equal(customUrl, client.Config.DetermineServiceURL());
            Assert.Equal(customRegion, client.Config.RegionEndpoint.SystemName);
        }
    }
}
