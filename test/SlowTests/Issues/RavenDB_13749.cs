using System;
using System.Collections.Generic;
using FastTests;
using Raven.Server.Config;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_13749 : RavenTestBase
    {
        [Fact]
        public void CanSetLogsRetentionTime()
        {
            using (var server = GetNewServer(new ServerCreationOptions{RunInMemory = true, CustomSettings = new Dictionary<string, string>
            {
                [RavenConfiguration.GetKey(x => x.Logs.RetentionTime)] = "100"
            }}))
            {
                Assert.Equal(new TimeSpan(100, 0, 0), server.Configuration.Logs.RetentionTime.Value.AsTimeSpan);
            }
        }

        [Fact]
        public void SettingLogsRetentionTimeToVeryLowValueWillSetMinValueAnyway()
        {
            using (var server = GetNewServer(new ServerCreationOptions{ RunInMemory = true, CustomSettings = new Dictionary<string, string>
            {
                [RavenConfiguration.GetKey(x => x.Logs.RetentionTime)] = "1"
            }}))
            {
                Assert.Equal(new TimeSpan(24, 0, 0), server.Configuration.Logs.RetentionTime.Value.AsTimeSpan);
            }
        }
    }
}
