using System.Collections.Generic;
using System.Linq;
using FastTests.Server.Replication;
using Raven.Client;
using Raven.Client.Documents;
using Raven.Client.Documents.Replication;
using Raven.Client.Server;
using Xunit;

namespace FastTests.Issues
{
    public class RavenDB_5730 : ReplicationTestsBase
    {
        private class User
        {
            public string Name { get; set; }
        }

        [Fact]
        public void Whispace_at_the_beginning_of_replication_destination_url_should_not_cause_issues()
        {
            using (var storeA = GetDocumentStore())
            using (var storeB = GetDocumentStore())
            {
                var url = " " + storeB.Urls.First();
                DoReplicationTest(storeA, storeB, url);
            }
        }

        [Fact]
        public void Whispace_at_the_end_of_replication_destination_url_should_not_cause_issues()
        {
            using (var storeA = GetDocumentStore())
            using (var storeB = GetDocumentStore())
            {
                var url = storeB.Urls.First() + " ";
                DoReplicationTest(storeA, storeB, url);
            }
        }

        [Fact]
        public void Whispace_at_the_beginning_and_end_of_replication_destination_url_should_not_cause_issues()
        {
            using (var storeA = GetDocumentStore())
            using (var storeB = GetDocumentStore())
            {
                var url = storeB.Urls.First() + " ";
                DoReplicationTest(storeA, storeB, url);
            }
        }       

        private void DoReplicationTest(DocumentStore storeA, DocumentStore storeB, string url)
        {

            var watchers = new List<DatabaseWatcher>
            {
                new DatabaseWatcher
                {
                    Database = storeB.Database,
                    Url = url,
                }
            };

            UpdateReplicationTopology(storeA, watchers).ConfigureAwait(false);
            
            using (var session = storeA.OpenSession())
            {
                session.Store(new User {Name = "foo/bar"}, "foo-id");
                session.SaveChanges();
            }

            Assert.True(WaitForDocument(storeB, "foo-id"));
        }
    }
}
