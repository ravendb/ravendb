using System;
using Raven.Server.Config;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Issues
{
    public class RavenDB_7260 : NoDisposalNeeded
    {
        public RavenDB_7260(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void ShouldWork()
        {
            var server = RavenConfiguration.CreateForServer(null);
            server.SetSetting(RavenConfiguration.GetKey(x => x.Storage.ForceUsing32BitsPager), "true");
            server.Initialize(); // should not throw

            Assert.True(server.Storage.ForceUsing32BitsPager);

            var database = RavenConfiguration.CreateForDatabase(server, "dbName");
            database.SetSetting(RavenConfiguration.GetKey(x => x.Storage.ForceUsing32BitsPager), "true");

            var e = Assert.Throws<InvalidOperationException>(() => database.Initialize());
            Assert.Equal($"Configuration '{RavenConfiguration.GetKey(x => x.Storage.ForceUsing32BitsPager)}' can only be set at server level.", e.Message);
        }
    }
}
