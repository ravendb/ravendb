using System;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.ServerWide.Operations.Logs;
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
        public async Task CanGetLogsConfigurationAndChangeMode()
        {
            UseNewLocalServer();

            using (var store = GetDocumentStore())
            {
                using (var cts = new CancellationTokenSource(TimeSpan.FromSeconds(15)))
                {
                    var configuration = await store.Maintenance.Server.SendAsync(new GetLogsConfigurationOperation(), cts.Token);

                    LogMode modeToSet;
                    var time = TimeSpan.MaxValue;
                    switch (configuration.CurrentMode)
                    {
                        case LogMode.None:
                            modeToSet = LogMode.Information;
                            break;
                        case LogMode.Operations:
                            modeToSet = LogMode.Information;
                            break;
                        case LogMode.Information:
                            modeToSet = LogMode.None;
                            break;
                        default:
                            throw new ArgumentOutOfRangeException();
                    }

                    try
                    {
                        await store.Maintenance.Server.SendAsync(new SetLogsConfigurationOperation(new SetLogsConfigurationOperation.Parameters
                        {
                            Mode = modeToSet,
                            RetentionTime = time
                        }), cts.Token);

                        var configuration2 = await store.Maintenance.Server.SendAsync(new GetLogsConfigurationOperation(), cts.Token);

                        Assert.Equal(modeToSet, configuration2.CurrentMode);
                        Assert.Equal(time, configuration2.RetentionTime);
                        Assert.Equal(configuration.Mode, configuration2.Mode);
                        Assert.Equal(configuration.Path, configuration2.Path);
                        Assert.Equal(configuration.UseUtcTime, configuration2.UseUtcTime);
                    }
                    finally
                    {
                        await store.Maintenance.Server.SendAsync(new SetLogsConfigurationOperation(new SetLogsConfigurationOperation.Parameters
                        {
                            Mode = configuration.CurrentMode,
                            RetentionTime = configuration.RetentionTime
                        }), cts.Token);
                    }
                }
            }
        }
    }
}
