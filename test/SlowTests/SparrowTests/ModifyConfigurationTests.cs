using System;
using System.Collections.Generic;
using System.IO;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using FastTests;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Raven.Client.Documents.Changes;
using Raven.Client.ServerWide.Operations.Logs;
using Raven.Client.ServerWide.Operations.TrafficWatch;
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
using LogLevel = Sparrow.Logging.LogLevel;

namespace SlowTests.SparrowTests;

public class ModifyConfigurationTests : RavenTestBase
{
    public ModifyConfigurationTests(ITestOutputHelper output) : base(output)
    {
    }


    [RavenFact(RavenTestCategory.Logging)]
    public async Task JsonFileModifier_WhenModifyUnexistSettingsFile_ShouldCreateOne()
    {
        var settingJsonPath = NewDataPath();

        using (var context = JsonOperationContext.ShortTermSingleUse())
        using (var settingJsonModifier = SettingsJsonModifier.Create(context, settingJsonPath))
        {
            settingJsonModifier.SetOrRemoveIfDefault(LogLevel.Info, x => x.Logs.MinLevel);
            await settingJsonModifier.ExecuteAsync();
        }

        var configuration = RavenConfiguration.CreateForTesting(null, ResourceType.Server, settingJsonPath);
        configuration.Initialize();
        Assert.Equal(LogLevel.Info, configuration.Logs.MinLevel);
    }

    [RavenFact(RavenTestCategory.Logging)]
    public async Task JsonFileModifier_WhenModifySettingsWithNoFile_ShouldCreateOne()
    {
        var settingJsonPath = NewDataPath();
        var content = @"
{
    ""Logs.MinLevel"":""Error""
}";
        await File.WriteAllTextAsync(settingJsonPath, content);

        using (var context = JsonOperationContext.ShortTermSingleUse())
        using (var settingJsonModifier = SettingsJsonModifier.Create(context, settingJsonPath))
        {
            settingJsonModifier.SetOrRemoveIfDefault(LogLevel.Info, x => x.Logs.MinLevel);
            await settingJsonModifier.ExecuteAsync();
        }

        var configuration = RavenConfiguration.CreateForTesting(null, ResourceType.Server, settingJsonPath);
        configuration.Initialize();
        Assert.Equal(LogLevel.Info, configuration.Logs.MinLevel);
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

        using (var context = JsonOperationContext.ShortTermSingleUse())
        using (var settingJsonModifier = SettingsJsonModifier.Create(context, settingJsonPath))
        {
            settingJsonModifier.SetOrRemoveIfDefault(LogMode.Information, x => x.Logs.Mode);
            settingJsonModifier.SetOrRemoveIfDefault(200, x => x.Logs.RetentionTime);
            settingJsonModifier.SetOrRemoveIfDefault(600, x => x.Logs.RetentionSize);
            settingJsonModifier.SetOrRemoveIfDefault(true, x => x.Logs.Compress);
            await settingJsonModifier.ExecuteAsync();
        }

        var configuration = RavenConfiguration.CreateForTesting(null, ResourceType.Server, settingJsonPath);
        configuration.Initialize();
        Assert.Equal(LogMode.Information, configuration.Logs.Mode);
        Assert.Equal(TimeSpan.FromHours(200), configuration.Logs.RetentionTime.Value.AsTimeSpan);
        Assert.Equal(new Size(600, SizeUnit.Megabytes), configuration.Logs.RetentionSize);
        Assert.Equal(true, configuration.Logs.Compress);
    }

    [RavenFact(RavenTestCategory.Logging)]
    public async Task JsonFileModifier_WhenOriginValueSetExplicitlyToDefault_ShouldNotRemoveIt()
    {
        var settingJsonPath = NewDataPath();
        var content = @"
{
    ""Logs.RetentionTimeInHrs"":72,
}";
        await File.WriteAllTextAsync(settingJsonPath, content);

        using (var context = JsonOperationContext.ShortTermSingleUse())
        using (var settingJsonModifier = SettingsJsonModifier.Create(context, settingJsonPath))
        {
            settingJsonModifier.SetOrRemoveIfDefault(72, x => x.Logs.RetentionTime);
            await settingJsonModifier.ExecuteAsync();
        }

        var modifiedContent = await File.ReadAllTextAsync(settingJsonPath);
        var json = JsonConvert.DeserializeObject<JObject>(modifiedContent);
        Assert.True(json.TryGetValue("Logs.RetentionTimeInHrs", out var value));
        Assert.Equal(72, value.Value<int>());
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

        using (var context = JsonOperationContext.ShortTermSingleUse())
        using (var settingJsonModifier = SettingsJsonModifier.Create(context, settingJsonPath))
        {
            settingJsonModifier.SetOrRemoveIfDefault(LogMode.Information, x => x.Logs.Mode);
            settingJsonModifier.SetOrRemoveIfDefault(200, x => x.Logs.RetentionTime);
            settingJsonModifier.SetOrRemoveIfDefault(600, x => x.Logs.RetentionSize);
            settingJsonModifier.SetOrRemoveIfDefault(true, x => x.Logs.Compress);
            await settingJsonModifier.ExecuteAsync();
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
            Compress = true,
            Persist = true
        };

        var settingsJsonPath = Path.GetTempFileName();
        var options = await CreateSettingsJsonFile(settingsJsonPath);
        using (var embedded = new EmbeddedServer())
        {
            embedded.StartServer(options);

            using var store = await embedded.GetDocumentStoreAsync("PersistLogConfiguration");

            await store.Maintenance.Server.SendAsync(new SetLogsConfigurationOperation(newParams));

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
        var settingJsonStr = context.ReadObject(settingJson, "settings-json");
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

    [RavenFact(RavenTestCategory.Logging, Skip = "TODO [Igal]")]
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
            var requestUri = $"{url.AbsoluteUri}admin/logs/microsoft/configuration";

            var stringContent = new StringContent("{\"Configuration\":{\"\":\"Trace\"}, \"Persist\":true}", Encoding.UTF8, "application/json");

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


    [RavenFact(RavenTestCategory.Logging)]
    public async Task PersistTrafficWatchConfiguration()
    {
        var settingsJsonPath = Path.GetTempFileName();
        var options = await CreateSettingsJsonFile(settingsJsonPath);

        var setConfiguration = new PutTrafficWatchConfigurationOperation.Parameters()
        {
            TrafficWatchMode = TrafficWatchMode.ToLogFile,
            Databases = new List<string> { "test1" },
            StatusCodes = new List<int> { 200 },
            MinimumResponseSizeInBytes = new Size(11, SizeUnit.Bytes),
            MinimumRequestSizeInBytes = new Size(22, SizeUnit.Bytes),
            MinimumDurationInMs = 33,
            HttpMethods = new List<string> { "POST" },
            ChangeTypes = new List<TrafficWatchChangeType> { TrafficWatchChangeType.Queries },
            CertificateThumbprints = new List<string> { "0123456789ABCDEF0123456789ABCDEF01234567" },
            Persist = true
        };
        using (var embedded = new EmbeddedServer())
        {
            embedded.StartServer(options);
            using var store = await embedded.GetDocumentStoreAsync("PersistLogConfiguration");

            var requestExecutor = store.GetRequestExecutor();

            await store.Maintenance.Server.SendAsync(new PutTrafficWatchConfigurationOperation(setConfiguration));

            using (requestExecutor.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            {
                using var settingsJson = ReadConfiguration(context, settingsJsonPath);
                Assert.True(settingsJson.TryGet(RavenConfiguration.GetKey(x => x.TrafficWatch.TrafficWatchMode), out TrafficWatchMode trafficWatchMode));
                Assert.Equal(setConfiguration.TrafficWatchMode, trafficWatchMode);

                Assert.True(settingsJson.TryGet(RavenConfiguration.GetKey(x => x.TrafficWatch.Databases), out string databases));
                Assert.Equal(string.Join(';', setConfiguration.Databases), databases);

                Assert.True(settingsJson.TryGet(RavenConfiguration.GetKey(x => x.TrafficWatch.StatusCodes), out string statusCodes));
                Assert.Equal(string.Join(';', setConfiguration.StatusCodes), statusCodes);

                Assert.True(settingsJson.TryGet(RavenConfiguration.GetKey(x => x.TrafficWatch.MinimumResponseSize), out long minimumResponseSize));
                Assert.Equal(setConfiguration.MinimumResponseSizeInBytes.GetValue(SizeUnit.Bytes), minimumResponseSize);

                Assert.True(settingsJson.TryGet(RavenConfiguration.GetKey(x => x.TrafficWatch.MinimumRequestSize), out long minimumRequestSize));
                Assert.Equal(setConfiguration.MinimumRequestSizeInBytes.GetValue(SizeUnit.Bytes), minimumRequestSize);

                Assert.True(settingsJson.TryGet(RavenConfiguration.GetKey(x => x.TrafficWatch.MinimumDuration), out long minimumDuration));
                Assert.Equal(setConfiguration.MinimumDurationInMs, minimumDuration);

                Assert.True(settingsJson.TryGet(RavenConfiguration.GetKey(x => x.TrafficWatch.HttpMethods), out string httpMethods));
                Assert.Equal(string.Join(';', setConfiguration.HttpMethods), httpMethods);

                Assert.True(settingsJson.TryGet(RavenConfiguration.GetKey(x => x.TrafficWatch.ChangeTypes), out string changeTypes));
                Assert.Equal(string.Join(';', setConfiguration.ChangeTypes), changeTypes);

                Assert.True(settingsJson.TryGet(RavenConfiguration.GetKey(x => x.TrafficWatch.CertificateThumbprints), out string certificateThumbprints));
                Assert.Equivalent(string.Join(';', setConfiguration.CertificateThumbprints), certificateThumbprints);
            }
        }

        using (var embedded = new EmbeddedServer())
        {
            embedded.StartServer(options);
            using var store = await embedded.GetDocumentStoreAsync("PersistLogConfiguration");

            var getConfiguration = await store.Maintenance.Server.SendAsync(new GetTrafficWatchConfigurationOperation());

            Assert.Equal(setConfiguration.TrafficWatchMode, getConfiguration.TrafficWatchMode);
            Assert.Equivalent(setConfiguration.Databases, getConfiguration.Databases);
            Assert.Equivalent(setConfiguration.StatusCodes, getConfiguration.StatusCodes);
            Assert.Equal(setConfiguration.MinimumResponseSizeInBytes, getConfiguration.MinimumResponseSizeInBytes);
            Assert.Equal(setConfiguration.MinimumRequestSizeInBytes, getConfiguration.MinimumRequestSizeInBytes);
            Assert.Equal(setConfiguration.MinimumDurationInMs, getConfiguration.MinimumDurationInMs);
            Assert.Equivalent(setConfiguration.HttpMethods, getConfiguration.HttpMethods);
            Assert.Equivalent(setConfiguration.ChangeTypes, getConfiguration.ChangeTypes);
            Assert.Equivalent(setConfiguration.CertificateThumbprints, getConfiguration.CertificateThumbprints);
        }
    }
}
