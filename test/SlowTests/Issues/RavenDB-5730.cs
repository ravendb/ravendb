using System.Linq;
using System.Threading.Tasks;
using FastTests.Server.Replication;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.Replication;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_5730 : ReplicationTestBase
    {
        private class User
        {
            public string Name { get; set; }
        }

        [Fact]
        public async Task Whitespace_at_the_beginning_of_replication_destination_url_should_not_cause_issues()
        {
            using (var storeA = GetDocumentStore())
            using (var storeB = GetDocumentStore())
            {
                var url = " " + storeB.Urls.First();
                await DoReplicationTest(storeA, storeB, url);
            }
        }

        [Fact]
        public async Task Whitespace_at_the_end_of_replication_destination_url_should_not_cause_issues()
        {
            using (var storeA = GetDocumentStore())
            using (var storeB = GetDocumentStore())
            {
                var url = storeB.Urls.First() + " ";
                await DoReplicationTest(storeA, storeB, url);
            }
        }

        [Fact]
        public async Task Whitespace_at_the_beginning_and_end_of_replication_destination_url_should_not_cause_issues()
        {
            using (var storeA = GetDocumentStore())
            using (var storeB = GetDocumentStore())
            {
                var url = " " + storeB.Urls.First() + " ";
                await DoReplicationTest(storeA, storeB, url);
            }
        }       

        private async Task DoReplicationTest(DocumentStore storeA, DocumentStore storeB, string url)
        {
            var watcher = new ExternalReplication(storeB.Database, "Connection");

             await AddWatcherToReplicationTopology(storeA, watcher, new[] { url }).ConfigureAwait(false);
            
            using (var session = storeA.OpenSession())
            {
                session.Store(new User {Name = "foo/bar"}, "foo-id");
                session.SaveChanges();
            }

            Assert.True(WaitForDocument(storeB, "foo-id"));
        }
    }
}
