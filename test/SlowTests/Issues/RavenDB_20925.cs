using System.Collections.Generic;
using System;
using System.IO;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.ServerWide.Operations.Logs;
using Raven.Embedded;
using Raven.Server.Config;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Logging;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_20925 : RavenTestBase
{
    public RavenDB_20925(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.Logging)]
    public async Task Logging_Backward_Compatibility_Settings()
    {
        var settingsJsonPath = GetTempFileName();
        var content = """
                      {
                          "Logs.Mode":"Information",
                          "Logs.MaxFileSizeInMb":333,
                          "Logs.RetentionTimeInHrs":240
                      }
                      """;

        await File.WriteAllTextAsync(settingsJsonPath, content);

        var options = new ServerOptions
        {
            ServerDirectory = Environment.CurrentDirectory,
            LogsPath = NewDataPath(),
            DataDirectory = NewDataPath(),
            CommandLineArgs = new List<string> { $"-c={settingsJsonPath}" }
        };

        using (var embedded = new EmbeddedServer())
        {
            embedded.StartServer(options);
            using var store = await embedded.GetDocumentStoreAsync("PersistLogConfiguration");
            var configurationResult = await store.Maintenance.Server.SendAsync(new GetLogsConfigurationOperation());

            Assert.Equal(LogLevel.Debug, configurationResult.Logs.CurrentMinLevel);
            Assert.Equal(333, configurationResult.Logs.ArchiveAboveSizeInMb);
            Assert.Equal(10, configurationResult.Logs.MaxArchiveDays);
        }
    }
}
