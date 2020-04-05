using System;
using System.Diagnostics;
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
        public async Task CanGetLogsConfigurationAndChangeLogMode()
        {
            UseNewLocalServer();

            using (var store = GetDocumentStore())
            {
                using (var cts = new CancellationTokenSource(TimeSpan.FromSeconds(15)))
                {
                    var configuration1 = await store.Maintenance.Server.SendAsync(new GetLogsConfigurationOperation(), cts.Token);
                    
                    LogMode newLogMode;
                    switch (configuration1.CurrentMode)
                    {
                        case LogMode.None:
                            newLogMode = LogMode.Information;
                            break;
                        case LogMode.Operations:
                            newLogMode = LogMode.Information;
                            break;
                        case LogMode.Information:
                            newLogMode = LogMode.None;
                            break;
                        default:
                            throw new ArgumentOutOfRangeException();
                    }

                    try
                    {
                        var newParams = new SetLogsConfigurationOperation.Parameters(configuration1)
                        {
                            Mode = newLogMode
                        };
                        
                        await store.Maintenance.Server.SendAsync(new SetLogsConfigurationOperation(newParams), cts.Token);

                        var configuration2 = await store.Maintenance.Server.SendAsync(new GetLogsConfigurationOperation(), cts.Token);

                        Assert.Equal(newLogMode, configuration2.CurrentMode);
                        
                        Assert.Equal(configuration1.Mode, configuration2.Mode);
                        Assert.Equal(configuration1.Path, configuration2.Path);
                        Assert.Equal(configuration1.UseUtcTime, configuration2.UseUtcTime);
                        Assert.Equal(configuration1.Compress, configuration2.Compress);
                        Assert.Equal(configuration1.RetentionTime, configuration2.RetentionTime);
                        //Assert.Equal(configuration1.RetentionSize, configuration2.RetentionSize); // waiting for issue RavenDB-14841
                    }
                    finally
                    {
                        await store.Maintenance.Server.SendAsync(new SetLogsConfigurationOperation(new SetLogsConfigurationOperation.Parameters(configuration1)), cts.Token);
                    }
                }
            }
        }
        
        [Fact]
        public async Task CanGetLogsConfigurationAndChangeRetentionTimeAndCompress()
        {
            UseNewLocalServer();

            using (var store = GetDocumentStore())
            {
                using (var cts = new CancellationTokenSource(TimeSpan.FromSeconds(Debugger.IsAttached ? 1000 : 15)))
                {
                    var configuration1 = await store.Maintenance.Server.SendAsync(new GetLogsConfigurationOperation(), cts.Token);

                    var newCompress = !configuration1.Compress;
                    var newTime = configuration1.RetentionTime == null ? new TimeSpan(9, 9, 9) :
                                                                         configuration1.RetentionTime.Add(TimeSpan.FromHours(1));
                    try
                    {
                        var newParams = new SetLogsConfigurationOperation.Parameters(configuration1)
                        {
                            Mode = LogMode.Information,
                            Compress = newCompress,
                            RetentionTime = newTime
                        };

                        await store.Maintenance.Server.SendAsync(new SetLogsConfigurationOperation(newParams), cts.Token);

                        var configuration2 = await store.Maintenance.Server.SendAsync(new GetLogsConfigurationOperation(), cts.Token);

                        Assert.Equal(newCompress, configuration2.Compress);
                        Assert.Equal(newTime, configuration2.RetentionTime);
                        Assert.Equal(LogMode.Information, configuration2.CurrentMode);
                        
                        Assert.Equal(configuration1.Mode, configuration2.Mode);
                        Assert.Equal(configuration1.Path, configuration2.Path);
                        Assert.Equal(configuration1.UseUtcTime, configuration2.UseUtcTime);
                        //Assert.Equal(configuration1.RetentionSize, configuration2.RetentionSize); //  waiting for issue RavenDB-14841
                    }
                    finally
                    {
                        await store.Maintenance.Server.SendAsync(new SetLogsConfigurationOperation(new SetLogsConfigurationOperation.Parameters(configuration1)), cts.Token);
                    }
                }
            }
        }
    }
}
