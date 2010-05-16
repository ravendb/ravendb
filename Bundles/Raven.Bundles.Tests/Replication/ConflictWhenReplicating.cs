using System.Net;
using System.Threading;
using Xunit;

namespace Raven.Bundles.Tests.Replication
{
    public class ConflictWhenReplicating : ReplicationBase
    {
        [Fact]
        public void When_replicating_and_a_document_is_already_there_will_result_in_conflict()
        {
            var store1 = CreateStore();
            var store2 = CreateStore();
            using(var session = store1.OpenSession())
            {
                session.Store(new Company());
                session.SaveChanges();
            }

            using (var session = store2.OpenSession())
            {
                session.Store(new Company());
                session.SaveChanges();
            }

            TellFirstInstanceToReplicateToSecondInstance();

            var webException = Assert.Throws<WebException>(() =>
            {
                for (int i = 0; i < RetriesCount; i++)
                {
                    using (var session = store2.OpenSession())
                    {
                        session.Load<Company>("companies/1");
                        Thread.Sleep(100);
                    }
                }
            });

            Assert.Equal(HttpStatusCode.Conflict,((HttpWebResponse)webException.Response).StatusCode);
        }

        [Fact]
        public void When_replicating_from_two_different_source_different_documents()
        {
            var store1 = CreateStore();
            var store2 = CreateStore();
            var store3 = CreateStore();
            using (var session = store1.OpenSession())
            {
                session.Store(new Company());
                session.SaveChanges();
            }

            using (var session = store2.OpenSession())
            {
                session.Store(new Company());
                session.SaveChanges();
            }

            TellInstanceToReplicateToAnotherInstance(0,2);

            for (int i = 0; i < RetriesCount; i++) // wait for it to show up in the 3rd server
            {
                using (var session = store3.OpenSession())
                {
                    if (session.Load<Company>("companies/1") != null)
                        break;
                    Thread.Sleep(100);
                }
            }

            TellInstanceToReplicateToAnotherInstance(1, 2);

            var webException = Assert.Throws<WebException>(() =>
            {
                for (int i = 0; i < RetriesCount; i++)
                {
                    using (var session = store3.OpenSession())
                    {
                        session.Load<Company>("companies/1");
                        Thread.Sleep(100);
                    }
                }
            });

            Assert.Equal(HttpStatusCode.Conflict, ((HttpWebResponse)webException.Response).StatusCode);
        }
    }
}