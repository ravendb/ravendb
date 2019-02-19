using System.Linq;
using System.Threading.Tasks;
using FastTests.Server.Replication;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.Replication;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Server.Documents.Replication
{
    public class ReplicateLargeDatabase : ReplicationTestBase
    {
        [Fact]
        public async Task AutomaticResolveWithIdenticalContent()
        {
            DocumentStore store1;
            DocumentStore store2;

            CreateSampleDatabase(out store1);
            CreateSampleDatabase(out store2);

            await SetupReplicationAsync(store1, store2);
            Assert.Equal(1, WaitForValue(() => store2.Maintenance.Send(new GetReplicationPerformanceStatisticsOperation()).Incoming.Length, 1));
            var stats = store2.Maintenance.Send(new GetReplicationPerformanceStatisticsOperation());
            var errors = stats.Incoming
                .SelectMany(x => x.Performance.Where(y => y.Errors != null).SelectMany(z => z.Errors)).ToList();
            Assert.Empty(errors);
        }

        public void CreateSampleDatabase(out DocumentStore store)
        {
            store = GetDocumentStore();
            CallCreateSampleDatabaseEndpoint(store);
        }

        public void CallCreateSampleDatabaseEndpoint(DocumentStore store)
        {
            store.Maintenance.Send(new CreateSampleDataOperation());
        }
    }
}
