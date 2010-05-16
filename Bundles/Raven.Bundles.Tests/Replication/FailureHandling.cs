using System.Net;
using System.Threading;
using Xunit;

namespace Raven.Bundles.Tests.Replication
{
    public class FailureHandling : ReplicationBase
    {
        [Fact]
        public void When_replicating_from_two_different_source_different_documents_at_the_same_time()
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

            TellInstanceToReplicateToAnotherInstance(0, 2);

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