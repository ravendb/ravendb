using System;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Configuration;
using Raven.Client.ServerWide.Operations;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_15772 : RavenTestBase
    {
        public RavenDB_15772(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task ShouldntThrowConcurrencyException()
        {
            using var store = GetDocumentStore();
            var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
            var fixedOrder = record.Topology.AllNodes.ToList();

            var updateClientConfig = store.Maintenance.SendAsync(
                new PutClientConfigurationOperation(
                    new ClientConfiguration
                    {
                        MaxNumberOfRequestsPerSession = 30
                    }));

            //In the origin case the update topology trigger by the watcher
            var updateTopology = store.Maintenance.Server.SendAsync(new ReorderDatabaseMembersOperation(store.Database, fixedOrder, true));

            await Task.WhenAll(updateClientConfig, updateTopology);
        }
    }
}
