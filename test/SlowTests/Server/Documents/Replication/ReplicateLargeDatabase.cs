using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using FastTests.Server.Replication;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Server.Documents.Replication
{
    public class ReplicateLargeDatabase : ReplicationTestsBase
    {
        [Fact]
        public async Task AutomaticResolveWithIdenticalContent()
        {
            DocumentStore store1;
            DocumentStore store2;

            CreateSampleDatabase(out store1);
            CreateSampleDatabase(out store2);

            await SetupReplicationAsync(store1, store2);
            Assert.Equal(1, WaitForValue(() => store2.Admin.Send(new GetReplicationPerformanceStatisticsOperation()).Incoming.Length, 1));
            var stats = store2.Admin.Send(new GetReplicationPerformanceStatisticsOperation());
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
            store.Admin.Send(new CreateSampleDataOperation());
        }
    }
}
