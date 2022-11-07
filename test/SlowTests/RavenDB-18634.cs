using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using CsvHelper.Configuration;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.Configuration;
using Raven.Client.Http;
using Raven.Client.ServerWide.Operations;
using Raven.Client.ServerWide.Operations.Configuration;
using Raven.Tests.Core.Utils.Entities;
using SlowTests.Core.AdminConsole;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests
{
    public class RavenDB_18634 : ClusterTestBase
    {
        public RavenDB_18634(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task DisableTcpCompressionIn1ServerOutOf2InCluster()
        {
            var (nodes, leader) = await CreateRaftCluster(2);

            // modify configuration
            AdminJsConsoleTests.ExecuteScript(leader, database: null, "server.Configuration.Server.DisableTcpCompression = true;");
            Assert.True(leader.Configuration.Server.DisableTcpCompression);

            using var store = GetDocumentStore(new Options { Server = leader, ReplicationFactor = 2});

            var db0 = await nodes[0].ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database);
            var db1 = await nodes[1].ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database);
            Assert.NotNull(db0);
            Assert.NotNull(db1);
        }

    }
}
