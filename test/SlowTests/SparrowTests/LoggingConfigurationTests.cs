using System;
using System.Collections.Generic;
using System.IO;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using FastTests;
using Microsoft.Extensions.Logging;
using Raven.Client.ServerWide.Operations.Logs;
using Raven.Embedded;
using Raven.Server.Config;
using Raven.Server.ServerWide;
using Sparrow;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Logging;
using Sparrow.Server.Json.Sync;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.SparrowTests;

public class LoggingConfigurationTests : RavenTestBase
{
    public LoggingConfigurationTests(ITestOutputHelper output) : base(output)
    {
    }

    
    [RavenFact(RavenTestCategory.Logging)]
    public async Task JsonFileModifier_WhenModifyUnexistSettingsFile_ShouldCreateOne()
    {
        var settingJsonPath = NewDataPath();
        
        var settingJsonModifier = new JsonConfigFileModifier(settingJsonPath);
        using (var context = JsonOperationContext.ShortTermSingleUse())
        {
            await settingJsonModifier.Execute(context, j =>
            {
                j[RavenConfiguration.GetKey(x => x.Logs.Mode)] = LogMode.Information;
            });
        }
        
        var configuration = RavenConfiguration.CreateForTesting(null, ResourceType.Server, settingJsonPath);
        configuration.Initialize();
        Assert.Equal(LogMode.Information, configuration.Logs.Mode);
    }
    
    [RavenFact(RavenTestCategory.Logging)]
    public async Task JsonFileModifier_WhenModifySettingsWithNoFile_ShouldCreateOne()
    {
        var settingJsonPath = NewDataPath();
        var content = @"
{
    ""Logs.Mode"":""Operation""
}";
        await File.WriteAllTextAsync(settingJsonPath, content);
        
        var settingJsonModifier = new JsonConfigFileModifier(settingJsonPath);
       
        using (var context = JsonOperationContext.ShortTermSingleUse())
        {
            await settingJsonModifier.Execute(context, j =>
            {
                j[RavenConfiguration.GetKey(x => x.Logs.Mode)] = LogMode.Information;
            });
        }
        
        var configuration = RavenConfiguration.CreateForTesting(null, ResourceType.Server, settingJsonPath);
        configuration.Initialize();
        Assert.Equal(LogMode.Information, configuration.Logs.Mode);
    }
    
    [RavenFact(RavenTestCategory.Logging)]
    public async Task JsonFileModifier_WhenModifySettingsWithNestedKey_ShouldCreateOne()
    {
        var settingJsonPath = NewDataPath();
        var content = @"
{
    ""Logs.Mode"":""Operation"",
    ""Logs.RetentionTimeInHrs"":100,
    ""Logs.RetentionSizeInMb"":500,
    ""Logs.Compress"":false
}";
        await File.WriteAllTextAsync(settingJsonPath, content);

        var settingJsonModifier = new JsonConfigFileModifier(settingJsonPath);
        using (var context = JsonOperationContext.ShortTermSingleUse())
        {
            await settingJsonModifier.Execute(context, j =>
            {
                j[RavenConfiguration.GetKey(x => x.Logs.Mode)] = LogMode.Information;
                j[RavenConfiguration.GetKey(x => x.Logs.RetentionTime)] = 200;
                j[RavenConfiguration.GetKey(x => x.Logs.RetentionSize)] = 600;
                j[RavenConfiguration.GetKey(x => x.Logs.Compress)] = true;
            });
        }
        
        var configuration = RavenConfiguration.CreateForTesting(null, ResourceType.Server, settingJsonPath);
        configuration.Initialize();
        Assert.Equal(LogMode.Information, configuration.Logs.Mode);
        Assert.Equal(TimeSpan.FromHours(200), configuration.Logs.RetentionTime.Value.AsTimeSpan);
        Assert.Equal(new Size(600, SizeUnit.Megabytes), configuration.Logs.RetentionSize);
        Assert.Equal(true, configuration.Logs.Compress);
    }

    [RavenFact(RavenTestCategory.Logging)]
    public async Task JsonFileModifier_WhenOriginLogConfigurationSetAsNested_ShouldOverrideThemAsWell()
    {
        var settingJsonPath = NewDataPath();
        const string content = """
       {
           "Logs": {
               "Mode":"Operation",
               "RetentionTimeInHrs":100,
               "RetentionSizeInMb":500,
               "Compress":false
           }
       }
       """;
        await File.WriteAllTextAsync(settingJsonPath, content);
        
        var settingJsonModifier = new JsonConfigFileModifier(settingJsonPath)
        {
 
        };
        using (var context = JsonOperationContext.ShortTermSingleUse())
        {
            await settingJsonModifier.Execute(context, j =>
            {
                j[RavenConfiguration.GetKey(x => x.Logs.Mode)] = LogMode.Information;
                j[RavenConfiguration.GetKey(x => x.Logs.RetentionTime)] = 200;
                j[RavenConfiguration.GetKey(x => x.Logs.RetentionSize)] = 600;
                j[RavenConfiguration.GetKey(x => x.Logs.Compress)] = true;
            });
        }
        
        var configuration = RavenConfiguration.CreateForTesting(null, ResourceType.Server, settingJsonPath);
        configuration.Initialize();
        Assert.Equal(LogMode.Information, configuration.Logs.Mode);
        Assert.Equal(TimeSpan.FromHours(200), configuration.Logs.RetentionTime.Value.AsTimeSpan);
        Assert.Equal(new Size(600, SizeUnit.Megabytes), configuration.Logs.RetentionSize);
        Assert.Equal(true, configuration.Logs.Compress);
    }
    
    [RavenFact(RavenTestCategory.Logging)]
    public async Task PersistLogConfiguration()
    {
        var newParams = new SetLogsConfigurationOperation.Parameters
        {
            Mode = LogMode.Information,
            RetentionSize = new Size(300, SizeUnit.Megabytes),
            RetentionTime = TimeSpan.FromHours(200),
            Compress = true
        };
        
        var settingsJsonPath = Path.GetTempFileName();
        var options = await CreateSettingsJsonFile(settingsJsonPath);
        using (var embedded = new EmbeddedServer())
        {
            embedded.StartServer(options);

            using var store = await embedded.GetDocumentStoreAsync("PersistLogConfiguration");
        
            await store.Maintenance.Server.SendAsync(new SetLogsConfigurationOperation(newParams, true));

            using (store.GetRequestExecutor().ContextPool.AllocateOperationContext(out JsonOperationContext context))
            {
                using var configuration = ReadConfiguration(context, settingsJsonPath);
            
                Assert.True(configuration.TryGet(RavenConfiguration.GetKey(x => x.Logs.Mode), out LogMode mode));
                Assert.Equal(newParams.Mode, mode);

                Assert.True(configuration.TryGet(RavenConfiguration.GetKey(x => x.Logs.RetentionSize), out int retentionSizeInMb));
                Assert.Equal(newParams.RetentionSize, new Size(retentionSizeInMb, SizeUnit.Megabytes));
            
                Assert.True(configuration.TryGet(RavenConfiguration.GetKey(x => x.Logs.RetentionTime), out int retentionTimeInHrs));
                Assert.Equal(newParams.RetentionTime, TimeSpan.FromHours(retentionTimeInHrs));

                Assert.True(configuration.TryGet(RavenConfiguration.GetKey(x => x.Logs.Compress), out bool compress));
                Assert.Equal(newParams.Compress, compress);
            }
        }

        using (var embedded = new EmbeddedServer())
        {
            embedded.StartServer(options);
            var a = await File.ReadAllTextAsync(settingsJsonPath);
            using var store = await embedded.GetDocumentStoreAsync("PersistLogConfiguration");
            var configurationResult = await store.Maintenance.Server.SendAsync(new GetLogsConfigurationOperation());
            
            Assert.Equal(newParams.Mode, configurationResult.Mode);
            Assert.Equal(newParams.RetentionSize, configurationResult.RetentionSize);
            Assert.Equal(newParams.RetentionTime, configurationResult.RetentionTime);
            Assert.Equal(newParams.Compress, configurationResult.Compress);
        }
    }

    private async Task<ServerOptions> CreateSettingsJsonFile(string settingsJsonPath, DynamicJsonValue settingJson = null)
    {
        settingJson ??= new DynamicJsonValue();
        settingJson[RavenConfiguration.GetKey(x => x.Logs.Mode)] = "None";

        using var context = JsonOperationContext.ShortTermSingleUse();
        var settingJsonStr =  context.ReadObject(settingJson, "settings-json");
        await File.WriteAllTextAsync(settingsJsonPath, settingJsonStr.ToString());
        return new ServerOptions
        {
            ServerDirectory = Environment.CurrentDirectory,
            LogsPath = NewDataPath(),
            DataDirectory = NewDataPath(),
            CommandLineArgs = new List<string> { $"-c={settingsJsonPath}" }
        };
    }

    private BlittableJsonReaderObject ReadConfiguration(JsonOperationContext context, string settingsJsonPath)
    {
        using var fs = new FileStream(settingsJsonPath, FileMode.Open, FileAccess.Read);
        return context.Sync.ReadForMemory(fs, Path.GetFileName(settingsJsonPath));
    }

    [RavenFact(RavenTestCategory.Logging)]
    public async Task PersistMicrosoftLogConfiguration()
    {
        var settingsJsonPath = Path.GetTempFileName();
        var options = await CreateSettingsJsonFile(settingsJsonPath);
        
        using (var embedded = new EmbeddedServer())
        {
            embedded.StartServer(options);
            using var store = await embedded.GetDocumentStoreAsync("PersistLogConfiguration");

            var requestExecutor = store.GetRequestExecutor();
            var httpClient = requestExecutor.HttpClient;

            var url = await embedded.GetServerUriAsync();
            var requestUri = $"{url.AbsoluteUri}admin/logs/microsoft/configuration?persist=true";

            var stringContent = new StringContent("{\"\":\"Trace\"}", Encoding.UTF8, "application/json");
            
            var response = await httpClient.PostAsync(requestUri, stringContent).ConfigureAwait(false);
            Assert.True(response.IsSuccessStatusCode);
            
            using (requestExecutor.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            {
                using var settingsJson = ReadConfiguration(context, settingsJsonPath);
                Assert.True(settingsJson.TryGet(RavenConfiguration.GetKey(x => x.Logs.DisableMicrosoftLogs), out bool disableMicrosoftLogs));
                Assert.False(disableMicrosoftLogs);
                
                using var microsoftJson = ReadConfiguration(context, Path.Combine(options.ServerDirectory, "settings.logs.microsoft.json"));
                Assert.True(microsoftJson.TryGet("", out LogLevel logLevel));
                Assert.Equal(LogLevel.Trace, logLevel);
            }
        }

        using (var embedded = new EmbeddedServer())
        {
            embedded.StartServer(options);
            using var store = await embedded.GetDocumentStoreAsync("PersistLogConfiguration");

            var requestExecutor = store.GetRequestExecutor();
            var httpClient = requestExecutor.HttpClient;

            var url = await embedded.GetServerUriAsync();
            var requestUri = $"{url.AbsoluteUri}admin/logs/microsoft/loggers?persist=true";
            var r = await httpClient.GetAsync(requestUri);
            Assert.True(r.IsSuccessStatusCode);

            using (requestExecutor.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            await using (var stream = await r.Content.ReadAsStreamAsync())
            {
                var loggers = await context.ReadForMemoryAsync(stream, "loggers");
                foreach (var propertyName in loggers.GetPropertyNames())
                {
                    Assert.True(loggers.TryGet(propertyName, out LogLevel logLevel));
                    Assert.Equal(LogLevel.Trace, logLevel);
                }
            }
        }
    }
}
