using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.ServerWide.Operations.Logs;
using Sparrow;
using Sparrow.Logging;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_11440 : RavenTestBase
    {
        public RavenDB_11440(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task CanGetLogsConfigurationAndChangeLogMode()
        {
            UseNewLocalServer();

            using (var store = GetDocumentStore())
            {
                using (var cts = new CancellationTokenSource(TimeSpan.FromSeconds(15)))
                {
                    var configuration1 = await store.Maintenance.Server.SendAsync(new GetLogsConfigurationOperation(), cts.Token);

                    LogLevel newMinLevel = configuration1.Logs.CurrentMinLevel switch
                    {
                        LogLevel.Trace => LogLevel.Debug,
                        LogLevel.Debug => LogLevel.Trace,
                        LogLevel.Info => LogLevel.Debug,
                        LogLevel.Warn => LogLevel.Debug,
                        LogLevel.Error => LogLevel.Debug,
                        LogLevel.Fatal => LogLevel.Debug,
                        LogLevel.Off => LogLevel.Debug,
                        _ => throw new ArgumentOutOfRangeException()
                    };

                    LogLevel newMaxLevel = configuration1.Logs.CurrentMaxLevel switch
                    {
                        LogLevel.Trace => LogLevel.Debug,
                        LogLevel.Debug => LogLevel.Trace,
                        LogLevel.Info => LogLevel.Debug,
                        LogLevel.Warn => LogLevel.Debug,
                        LogLevel.Error => LogLevel.Debug,
                        LogLevel.Fatal => LogLevel.Debug,
                        LogLevel.Off => LogLevel.Debug,
                        _ => throw new ArgumentOutOfRangeException()
                    };

                    try
                    {
                        await store.Maintenance.Server.SendAsync(new SetLogsConfigurationOperation(
                            new SetLogsConfigurationOperation.LogsConfiguration(minLevel: newMinLevel, maxLevel: newMaxLevel)), cts.Token);

                        var configuration2 = await store.Maintenance.Server.SendAsync(new GetLogsConfigurationOperation(), cts.Token);

                        Assert.Equal(newMinLevel, configuration2.Logs.CurrentMinLevel);
                        Assert.Equal(newMaxLevel, configuration2.Logs.CurrentMaxLevel);

                        Assert.Equal(configuration1.Logs.MinLevel, configuration2.Logs.MinLevel);
                        Assert.Equal(configuration1.Logs.MaxLevel, configuration2.Logs.MaxLevel);
                        Assert.Equal(configuration1.Logs.ArchiveAboveSizeInMb, configuration2.Logs.ArchiveAboveSizeInMb);
                        Assert.Equal(configuration1.Logs.EnableArchiveFileCompression, configuration2.Logs.EnableArchiveFileCompression);
                        Assert.Equal(configuration1.Logs.MaxArchiveDays, configuration2.Logs.MaxArchiveDays);
                        Assert.Equal(configuration1.Logs.MaxArchiveFiles, configuration2.Logs.MaxArchiveFiles);
                        Assert.Equal(configuration1.Logs.Path, configuration2.Logs.Path);
                    }
                    finally
                    {
                        await store.Maintenance.Server.SendAsync(new SetLogsConfigurationOperation(
                            new SetLogsConfigurationOperation.LogsConfiguration(minLevel: configuration1.Logs.CurrentMinLevel,
                                maxLevel: configuration1.Logs.CurrentMaxLevel)), cts.Token);
                    }
                }
            }
        }
    }
}
