using System.Transactions;
using Raven.Client;
using Raven.Client.Document;
using Raven.Server;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Bugs.DTC
{
    public class UsingDtcForCreate : RavenTest
    {
        [Fact]
        public void ShouldWork()
        {
            using (RavenDbServer server = GetNewServer(requestedStorage: "esent"))
            {
                EnsureDtcIsSupported(server);

                using (IDocumentStore store = new DocumentStore
                {
                    Url = "http://localhost:8079",
                    Conventions =
                    {
                        ShouldCacheRequest = s => false
                    }
                }.Initialize())
                {
                    using (var tx = new TransactionScope())
                    {
                        using (IDocumentSession s = store.OpenSession())
                        {
                            s.Store(new Tester {Id = "tester123", Name = "Blah"});
                            s.SaveChanges();
                        }

                        tx.Complete();
                    }

                    using (IDocumentSession s = store.OpenSession())
                    {
                        s.Store(new Tester {Id = "tester1234", Name = "Blah"});
                        s.SaveChanges();
                    }

                    using (IDocumentSession s = store.OpenSession())
                    {
                        s.Advanced.AllowNonAuthoritativeInformation = false;
                        Assert.NotNull(s.Load<Tester>("tester1234"));
                        Assert.NotNull(s.Load<Tester>("tester123"));
                    }
                }
            }
        }

        public class Tester
        {
            public string Id { get; set; }
            public string Name { get; set; }
        }
    }
}