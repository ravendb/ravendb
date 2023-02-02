using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.WebSockets;
using System.Runtime.CompilerServices;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Microsoft.Extensions.Logging;
using Raven.Server;
using Raven.Server.Utils.MicrosoftLogging;
using Sparrow;
using Sparrow.Json;
using Sparrow.Logging;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.SparrowTests;

public class MicrosoftLogTests : RavenTestBase
{
    private const string LogHeader = "Time, Thread, Level, Source, Logger, Message, Exception";
    private static readonly (string source, string logger) ForTest = ("Microsoft.AspNetCore", "ForTest End");
    private readonly Logger _logger = LoggingSource.Instance.GetLogger(ForTest.source, ForTest.logger);
    private readonly string _terminatorMessage = Guid.NewGuid().ToString("N");

    public MicrosoftLogTests(ITestOutputHelper output) : base(output)
    {
    }

    private Func<RavenServer> _serverFactory;
    protected override RavenServer GetNewServer(ServerCreationOptions options = null, string caller = null)
    {
        return _serverFactory != null
            ? _serverFactory.Invoke()
            : base.GetNewServer(options, caller);
    }

    [Fact]
    public async Task EnableMicrosoftLogs_WhenDisabled_ShouldNotLogMicrosoftLogs()
    {
        bool Predicate(string m)
        {
            AssertTrue(m.Contains(_terminatorMessage) == false, $"Should not contain any log of Microsoft.AspNetCore.ResponseCompression.ResponseCompressionProvider. {m}");
            return true;
        }
        await Test(Predicate);
    }

    [Fact]
    public async Task EnableMicrosoftLogs_WhenEnabledAndConfigurationFileDoesNotExist_ShouldNotLogMicrosoftLogs()
    {
        _serverFactory = () =>
        {
            var optionsCustomSettings = new Dictionary<string, string> 
            {
                { "Logs.Microsoft.Disable", "false" }
            };
            var options = new ServerCreationOptions();
            options.CustomSettings = optionsCustomSettings;
            return base.GetNewServer(options);
        };
        
        bool Predicate(string m)
        {
            AssertTrue(m.Contains(_terminatorMessage) == false, $"Should not contain any log of Microsoft.AspNetCore.ResponseCompression.ResponseCompressionProvider. {m}");
            return true;
        }
        await Test(Predicate);
    }
    
    [Fact]
    public async Task EnableMicrosoftLogs_WhenEnabledAndConfigureDefaultToInformation_ShouldLogMinimumInformationLevel()
    {
        var configurationFile = await CreateConfigurationFileAsync($@"
{{
    """": ""{LogLevel.Information}""
}}");

        var reg = new Regex(@"Microsoft.AspNetCore.*, \w+, (?<logLevel>\w+),");
        
        _serverFactory = () =>
        {
            var optionsCustomSettings = new Dictionary<string, string> 
            {
                { "Logs.Microsoft.Disable", "false" },
                {"Logs.Microsoft.ConfigurationPath", configurationFile}
            };
            var options = new ServerCreationOptions();
            options.CustomSettings = optionsCustomSettings;
            return base.GetNewServer(options);
        };
        
        bool ShouldEndTest(string m)
        {
            var match = reg.Match(m);
            if (match.Success == false)
            {
                if (m.Contains(LogHeader))
                    return false;
                if(m.Contains(_terminatorMessage))
                    return true;
            }
            var logLevel = Enum.Parse<LogLevel>(match.Groups["logLevel"].Value);
            AssertTrue(logLevel >= LogLevel.Information, $"Should not contain any log of Microsoft.AspNetCore.ResponseCompression.ResponseCompressionProvider. {m}");
            return false;
        }
        
        await Test(ShouldEndTest);
    }
    
    private async Task Test(Predicate<string> shouldEndTest)
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10000));
        try
        {
            var dummyStream = new MyDummyWebSocket(shouldEndTest, cts.Token);
            var context = new LoggingSource.WebSocketContext();
            context.Filter.Add($"source:Microsoft.AspNetCore", true);

            var registerTask = LoggingSource.Instance.Register(dummyStream, context, cts.Token);

            var server = GetNewServer();
            GetDocumentStore(new Options { Server = server }).Dispose();

            _logger.Info(_terminatorMessage);

            await dummyStream.Task;

            cts.Cancel();
            await registerTask;
        }
        catch (OperationCanceledException)
        {
        }
        finally
        {
            cts.Cancel();
        }
    }


    [Fact]
    public async Task MicrosoftLoggerProvider_WhenDefineNestedCategory_ShouldHandleAsRootProp()
    {
        var loggingSource = new LoggingSource(LogMode.None, "", "", TimeSpan.Zero, 0);
        var provider = new MicrosoftLoggingProvider(loggingSource, Server.ServerStore.NotificationCenter);

        var configurationFile = await CreateConfigurationFileAsync($@"
{{
    ""Microsoft"": ""Debug"",
    ""Key1"": {{
        ""LogLevel"" : ""{LogLevel.Information}"",
        ""Key2"" : ""{LogLevel.Error}""
    }}
}}");

        using (var context = JsonOperationContext.ShortTermSingleUse())
        {
            provider.Configuration.ReadConfiguration(configurationFile, context, true);
        }

        var configuration = provider.Configuration.ToArray();
        Assert.Contains(configuration, x => x is { Category: "Microsoft", LogLevel: LogLevel.Debug });
        Assert.Contains(configuration, x => x is { Category: "Key1", LogLevel: LogLevel.Information });
        Assert.Contains(configuration, x => x is { Category: "Key1.Key2", LogLevel: LogLevel.Error });
    }

    [Fact]
    public async Task MicrosoftLoggerProvider_WhenErrorConfiguration_ShouldNotThrow()
    {
        var loggingSource = new LoggingSource(LogMode.None, "", "", TimeSpan.Zero, 0);
        var provider = new MicrosoftLoggingProvider(loggingSource, Server.ServerStore.NotificationCenter);

        var configurationFile = await CreateConfigurationFileAsync(@"
{
    ""Microsoft"", ""Debug"",
    ""Microsoft2"", ""Information"",
"); // No closer

        using (var context = JsonOperationContext.ShortTermSingleUse())
        {
            provider.Configuration.ReadConfiguration(configurationFile, context, true);
        }

        Assert.Empty(provider.Configuration);
    }
    
    [Fact]
    public async Task MicrosoftLoggerProvider_WhenSetOnlyDefaultLogLevel_AllLogsLogLevelShouldBeAsDefault()
    {
        var path = NewDataPath(forceCreateDir: true);
        var loggingSource = new LoggingSource(LogMode.None, path, "", TimeSpan.Zero, 0);
        try
        {
            await using var stream = new DummyStream();
            loggingSource.AttachPipeSink(stream);
            var provider = new MicrosoftLoggingProvider(loggingSource, Server.ServerStore.NotificationCenter);

            var configurationFile = await CreateConfigurationFileAsync($@"
{{
    """", ""{LogLevel.Debug}"",
}}");

            _ = provider.CreateLogger("Test.Logger");
            using (var context = JsonOperationContext.ShortTermSingleUse())
            {
                provider.Configuration.ReadConfiguration(configurationFile, context, true);
                provider.ApplyConfiguration();
            }

            Assert.All(provider.GetLoggers(), (logger, i) =>
            {
                AssertTrue(logger.MinLogLevel == LogLevel.Debug, $"logger {logger.Name} log level should be Debug but was {logger.MinLogLevel}"); 
            });
        }
        finally
        {
            loggingSource.EndLogging();
        }
    }
    
    [Fact]
    public async Task MicrosoftLoggerProvider_WhenDisable_AllLogsLogLevelShouldBeNone()
    {
        var path = NewDataPath(forceCreateDir: true);
        var loggingSource = new LoggingSource(LogMode.None, path, "", TimeSpan.Zero, 0);
        try
        {
            await using var stream = new DummyStream();
            loggingSource.AttachPipeSink(stream);
            var provider = new MicrosoftLoggingProvider(loggingSource, Server.ServerStore.NotificationCenter);

            var configurationFile = await CreateConfigurationFileAsync($@"
{{
    """", ""{LogLevel.Debug}"",
}}");

            _ = provider.CreateLogger("Test.Logger");
            using (var context = JsonOperationContext.ShortTermSingleUse())
            {
                provider.Configuration.ReadConfiguration(configurationFile, context, true);
                provider.ApplyConfiguration();
            }

            provider.DisableLogging();
        
            Assert.All(provider.GetLoggers(), (logger, i) =>
            {
                AssertTrue(logger.MinLogLevel == LogLevel.None, $"logger {logger.Name} log level should be None but {logger.MinLogLevel}"); 
            });
        }
        finally
        {
            loggingSource.EndLogging();
        }
    }
    
    private class DummyStream : Stream
    {
        public override void Flush() { }
        public override int Read(byte[] buffer, int offset, int count) => count;
        public override long Seek(long offset, SeekOrigin origin) => offset;
        public override void SetLength(long value){}
        public override void Write(byte[] buffer, int offset, int count) { }
        public override bool CanRead => false;
        public override bool CanSeek => false;
        public override bool CanWrite => true;
        public override long Length => long.MaxValue;
        public override long Position { get; set; }
    }
    
    private class MyDummyWebSocket : WebSocket
    {
        private readonly Predicate<string> _assert;
        private readonly TaskCompletionSource<WebSocketReceiveResult> _tcs = new TaskCompletionSource<WebSocketReceiveResult>();
        private readonly TaskCompletionSource _checkTcs = new TaskCompletionSource();

        public Task Task => _checkTcs.Task;
        
        public MyDummyWebSocket(Predicate<string> assert, CancellationToken token)
        {
            token.Register(() =>
            {
                _checkTcs.TrySetCanceled();
                Close();
            }, false);
            _assert = assert;
        }
        
        public string LogsReceived { get; private set; } = "";
        public void Close() => _tcs.SetCanceled();
        public override void Abort() { }
        public override Task CloseAsync(WebSocketCloseStatus closeStatus, string statusDescription, CancellationToken cancellationToken)
            => Task.CompletedTask;
        public override Task CloseOutputAsync(WebSocketCloseStatus closeStatus, string statusDescription, CancellationToken cancellationToken)
            => Task.CompletedTask;
        public override void Dispose() { }
        public override Task<WebSocketReceiveResult> ReceiveAsync(ArraySegment<byte> buffer, CancellationToken cancellationToken) => _tcs.Task;
        public override async Task SendAsync(ArraySegment<byte> buffer, WebSocketMessageType messageType, bool endOfMessage, CancellationToken cancellationToken)
        {
            if (_tcs.Task.IsCompleted)
                await _tcs.Task;
            
            var log = Encodings.Utf8.GetString(buffer.ToArray());
            try
            {
                if(_checkTcs.Task.IsCompleted == false && _assert.Invoke(log))
                    _checkTcs.TrySetResult();
            }
            catch (Exception e)
            {
                _checkTcs.SetException(e);
            }
        }
        public override WebSocketCloseStatus? CloseStatus => default;
        public override string CloseStatusDescription => default;
        public override WebSocketState State => default;
        public override string SubProtocol => default;
    }
    
    [InterpolatedStringHandler]
    private ref struct ConditionalInterpolatedStringHandler
    {
        private DefaultInterpolatedStringHandler _inner;
        public ConditionalInterpolatedStringHandler(int literalLength, int formattedCount, bool condition, out bool shouldInterpolate)
        {
            if (condition)
                shouldInterpolate = false;

            shouldInterpolate = true;
            _inner = new DefaultInterpolatedStringHandler(literalLength, formattedCount);
        }
        public void AppendLiteral(string value) => _inner.AppendLiteral(value);
        public void AppendFormatted<T>(T value) => _inner.AppendFormatted(value);
        public string ToStringAndClear() => _inner.ToStringAndClear();
    }

    private void AssertTrue(bool condition, [InterpolatedStringHandlerArgument("condition")]ConditionalInterpolatedStringHandler userMessage, [CallerArgumentExpression("condition")] string strExpression = "")
    {
        if (condition)
            return;
        
        userMessage.AppendLiteral(". Condition expression:");
        userMessage.AppendLiteral(strExpression);
        
        Assert.True(false, userMessage.ToStringAndClear());
    }
    
    private async Task<string> CreateConfigurationFileAsync(string configurationContent)
    {
        string configurationFile = GetTempFileName();
        await File.WriteAllTextAsync(configurationFile, configurationContent);
        return configurationFile;
    }

}
