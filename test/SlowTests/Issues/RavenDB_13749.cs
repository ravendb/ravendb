using System;
using System.Collections.Generic;
using FastTests;
using Raven.Server.Config;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_13749 : RavenTestBase
    {
        public RavenDB_13749(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.Configuration)]
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

        [RavenFact(RavenTestCategory.Configuration)]
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
