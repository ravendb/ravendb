using System.IO;
using System.Linq;
using FastTests.Server.Replication;
using Raven.Client.Documents;
using Raven.Server.Documents.Replication;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Server.Documents.Replication
{
    public class ReplicateLargeDatabase : ReplicationTestsBase
    {
        [Fact]
        public void AutomaticResolveWithIdenticalContent()
        {
            DocumentStore store1;
            DocumentStore store2;

            CreateSampleDatabase(out store1);
            CreateSampleDatabase(out store2);

            SetupReplication(store1, store2);
            Assert.Equal(1, WaitForValue(() => GetReplicationStats(store2).IncomingStats.Count, 1));
            var stats = GetReplicationStats(store2);
            Assert.True(stats.IncomingStats.Any(o =>
            {
                var stat = (ReplicationStatistics.IncomingBatchStats)o;
                if (stat.Exception != null)
                {
                    throw new InvalidDataException(stat.Exception);
                }
                return true;
            }));
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
