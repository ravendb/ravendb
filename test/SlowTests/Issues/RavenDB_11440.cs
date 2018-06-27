using System;
using FastTests;
using Raven.Client.ServerWide.Operations.Logs;
using Sparrow.Logging;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_11440 : RavenTestBase
    {
        [Fact]
        public void CanGetLogsConfigurationAndChangeMode()
        {
            using (var store = GetDocumentStore())
            {
                var configuration1 = store.Maintenance.Server.Send(new GetLogsConfigurationOperation());

                LogMode modeToSet;
                switch (configuration1.CurrentMode)
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
                    store.Maintenance.Server.Send(new SetLogsConfigurationOperation(new SetLogsConfigurationOperation.Parameters
                    {
                        Mode = modeToSet
                    }));

                    var configuration2 = store.Maintenance.Server.Send(new GetLogsConfigurationOperation());

                    Assert.Equal(modeToSet, configuration2.CurrentMode);
                    Assert.Equal(configuration1.Mode, configuration2.Mode);
                    Assert.Equal(configuration1.Path, configuration2.Path);
                    Assert.Equal(configuration1.UseUtcTime, configuration2.UseUtcTime);
                }
                finally
                {
                    store.Maintenance.Server.Send(new SetLogsConfigurationOperation(new SetLogsConfigurationOperation.Parameters
                    {
                        Mode = configuration1.CurrentMode
                    }));
                }
            }
        }
    }
}
