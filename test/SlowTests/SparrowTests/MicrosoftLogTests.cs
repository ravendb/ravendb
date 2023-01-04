using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Threading.Tasks;
using FastTests;
using Microsoft.Extensions.Logging;
using Raven.Server.Utils.MicrosoftLogging;
using Sparrow.Json;
using Sparrow.Logging;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.SparrowTests;

public class MicrosoftLogTests : RavenTestBase
{
    public MicrosoftLogTests(ITestOutputHelper output) : base(output)
    {
    }

    [Fact]
    public async Task EnableMicrosoftLogs_WhenConfigFileDoesntExistAndOtherConfigsAreDefault_ShouldCreateLogInsideMainLogDirAndLogOnlyCritical()
    {
        string mainLogPath = GetEmptyDirectory();

        var options = new ServerCreationOptions();
        options.CustomSettings = new Dictionary<string, string>
        {
            {"Logs.Microsoft.Disable","false"},
            {"Logs.Path",mainLogPath}
        };
        var server = GetNewServer(options);
        GetDocumentStore(new Options {Server = server}).Dispose();

        string combine = Path.Combine(mainLogPath, "MicrosoftLogs");
        var logFile = await AssertWaitForNotNullAsync(() => Task.FromResult(Directory.GetFiles(combine).FirstOrDefault()));
        await AssertLogs(logFile, LogLevel.Critical); 
    }
    
    [Fact]
    public async Task EnableMicrosoftLogs_WhenSetDefaultToDebug_ShouldWriteMinimumDebug()
    {
        string logPath = GetEmptyDirectory();

        var configurationFile = await CreateConfigurationFile(@"
{
    """": ""Debug""
}");

        var options = new ServerCreationOptions();
        options.CustomSettings = new Dictionary<string, string>
        {
            {"Logs.Microsoft.Disable","false"},
            {"Logs.Microsoft.Path",logPath},
            {"Logs.Microsoft.ConfigurationPath", configurationFile}
        };
        var server = GetNewServer(options);
        GetDocumentStore(new Options {Server = server}).Dispose();

        var logFile = await AssertWaitForNotNullAsync(() => Task.FromResult(Directory.GetFiles(logPath).FirstOrDefault()));
        await AssertLogs(logFile, LogLevel.Debug); 
    }

    private async Task<string> CreateConfigurationFile(string configurationContent)
    {
        string configurationFile = GetTempFileName();
        await File.WriteAllTextAsync(configurationFile, configurationContent);
        return configurationFile;
    }

    [Fact]
    public async Task EnableMicrosoftLogs_WhenSetByEndpointDefaultToDebug_ShouldWriteMinimumDebug()
    {
        string logPath = GetEmptyDirectory();
        var options = new ServerCreationOptions();
        options.CustomSettings = new Dictionary<string, string>
        {
            {"Logs.Microsoft.Disable","false"},
            {"Logs.Microsoft.Path",logPath},
        };

        string configurationContent = @"
{
    """": ""Debug""
}";
        
        var server = GetNewServer(options);
        
        using (var httpClient = new HttpClient())
        {
            var serverWebUrl = $"{server.WebUrl}/admin/logs/microsoft/configuration";
            var httpMethod = new HttpMethod("Post");
            var httpRequestMessage = new HttpRequestMessage(httpMethod, new Uri(serverWebUrl));
            httpRequestMessage.Content = new StringContent(configurationContent);
            await httpClient.SendAsync(httpRequestMessage);
        }
        GetDocumentStore(new Options {Server = server}).Dispose();
        
        var logFile = await AssertWaitForNotNullAsync(() => Task.FromResult(Directory.GetFiles(logPath).FirstOrDefault()));
        await AssertLogs(logFile, LogLevel.Debug); 
    }
    
    [Fact]
    public async Task EnableMicrosoftLogs_WhenSetSpecificLogger_ShouldApplyOnlyOnIt()
    {
        string logPath = GetEmptyDirectory();
        string configurationFile = GetTempFileName();

        const string configurationContent = @"
{
    ""Microsoft.AspNetCore.Server.Kestrel"": ""Debug""
}";
        await File.WriteAllTextAsync(configurationFile, configurationContent);
        
        var options = new ServerCreationOptions();
        options.CustomSettings = new Dictionary<string, string>
        {
            {"Logs.Microsoft.Disable","false"},
            {"Logs.Microsoft.Path",logPath},
            {"Logs.Microsoft.ConfigurationPath", configurationFile}
        };
        var server = GetNewServer(options);
        GetDocumentStore(new Options {Server = server}).Dispose();

        var logFile = await AssertWaitForNotNullAsync(() => Task.FromResult(Directory.GetFiles(logPath).FirstOrDefault()));
        await AssertLogs(logFile, LogLevel.Debug, "Microsoft.AspNetCore.Server.Kestrel"); 
    }

    private static async Task AssertLogs(string logFile, LogLevel logLevel)
    {
        await AssertLogs(logFile, (lineNum, line) =>
        {
            var lineLogLevel = Enum.Parse<LogLevel>(line.Split(',')[5]);
            Assert.True(lineLogLevel >= logLevel, $"Line {lineNum} contains loglevel is greater then {logLevel} - {line}");
        });
    }
    
    private static async Task AssertLogs(string logFile, LogLevel logLevel, string category)
    {
        await AssertLogs(logFile, (lineNum, line) =>
        {
            var lineParts = line.Split(',');
            var lineLogLevel = Enum.Parse<LogLevel>(lineParts[5]);
            Assert.True(lineLogLevel >= logLevel, $"Line {lineNum} contains loglevel is greater then {logLevel} - {line}");
            var lineCategory = (lineParts[3] + '.' + lineParts[4]).Replace(" ", "");
            Assert.True( lineCategory.StartsWith(category), $"Line {lineNum} contains category that is not {category} - {line}");
        });
    }
    
    private static async Task AssertLogs(string logFile, Action<int, string> predicate)
    {
        using (StreamReader file = new StreamReader(logFile))
        {
            await file.ReadLineAsync();
            var lineNum = 1;
            while (await file.ReadLineAsync() is { } line)
            {
                predicate.Invoke(lineNum, line);
                lineNum++;
            }
        }
    }

    private string GetEmptyDirectory()
    {
        var mainLogPath = NewDataPath();
        try
        {
            Directory.Delete(mainLogPath, true);
        }
        catch (Exception e)
        {
        }

        return mainLogPath;
    }
    
    
    [Fact]
    public async Task MicrosoftLoggerProvider_WhenDefineNestedCategory_ShouldHandleAsRootProp()
    {
        var loggingSource = new LoggingSource(LogMode.None, "", "", TimeSpan.Zero, 0);
        var provider = new MicrosoftLoggingProvider(loggingSource, Server.ServerStore.NotificationCenter);
        
        var configurationFile = await CreateConfigurationFile(@"
{
    ""Microsoft"": ""Debug"",
    ""Key1"": {
        ""LogLevel"" : ""Information"",
        ""Key2"" : ""Error""
    }
}");

        using (var context = JsonOperationContext.ShortTermSingleUse())
        {
            await provider.InitAsync(configurationFile, context);
        }

        var configuration = provider.GetConfiguration().ToArray();
        Assert.Contains(configuration, x => x is {category: "Microsoft", logLevel: LogLevel.Debug});
        Assert.Contains(configuration, x => x is {category: "Key1", logLevel: LogLevel.Information});
        Assert.Contains(configuration, x => x is {category: "Key1.Key2", logLevel: LogLevel.Error});
    }
    
    [Fact]
    public async Task MicrosoftLoggerProvider_WhenErrorConfiguration_ShouldNotThrow()
    {
        var loggingSource = new LoggingSource(LogMode.None, "", "", TimeSpan.Zero, 0);
        var provider = new MicrosoftLoggingProvider(loggingSource, Server.ServerStore.NotificationCenter);
        
        var configurationFile = await CreateConfigurationFile(@"
{
    ""Microsoft"", ""Debug"",
    ""Microsoft2"", ""Information"",
");

        using (var context = JsonOperationContext.ShortTermSingleUse())
        {
            await provider.InitAsync(configurationFile, context);
        }

        var configuration = provider.GetConfiguration().ToArray();
        Assert.Empty(configuration);
    }
}
