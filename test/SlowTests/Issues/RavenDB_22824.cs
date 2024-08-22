using System.IO;
using System.Threading.Tasks;
using FastTests;
using OpenTelemetry.Exporter;
using Raven.Server.Config;
using Raven.Server.ServerWide;
using Tests.Infrastructure;
using Xunit.Abstractions;


namespace SlowTests.Issues;

public class RavenDB_22824 : RavenTestBase
{
    public RavenDB_22824(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.Configuration)]
    public async Task RavenConfigurationSupportsNullableEnums()
    {
        var settingJsonPath = NewDataPath();
        var content = $"{{ \"{RavenConfiguration.GetKey(x => x.Monitoring.OpenTelemetry.OtlpProtocol)}\": \"{OtlpExportProtocol.HttpProtobuf}\" }}";
        await File.WriteAllTextAsync(settingJsonPath, content);
        
        var configuration = RavenConfiguration.CreateForTesting(null, ResourceType.Server, settingJsonPath);
        configuration.Initialize();
    }
}
