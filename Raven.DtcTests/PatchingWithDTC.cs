using System.Transactions;
using Raven.Abstractions.Commands;
using Raven.Abstractions.Data;
using Raven.Client;
using Raven.Client.Document;
using Raven.Server;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.MailingList
{
    public class PatchingWithDTC : RavenTest
    {
        [Fact]
        public void ShouldWork()
        {
            using (RavenDbServer server = GetNewServer(requestedStorage: "esent"))
            {
                EnsureDtcIsSupported(server);

                using (IDocumentStore store = new DocumentStore
                {
                    Url = "http://localhost:8079"
                }.Initialize())
                {
                    using (IDocumentSession session = store.OpenSession())
                    {
                        session.Store(new Item {Name = "milk"});
                        session.SaveChanges();
                    }

                    using (var tx = new TransactionScope())
                    using (IDocumentSession session = store.OpenSession())
                    {
                        session.Advanced.Defer(new PatchCommandData
                        {
                            Key = "items/1",
                            Patches = new[]
                            {
                                new PatchRequest
                                {
                                    Type = PatchCommandType.Set,
                                    Name = "Name",
                                    Value = "Bread"
                                }
                            }
                        });
                        session.SaveChanges();
                        tx.Complete();
                    }
                    using (IDocumentSession session = store.OpenSession())
                    {
                        session.Advanced.AllowNonAuthoritativeInformation = false;
                        Assert.Equal("Bread", session.Load<Item>(1).Name);
                    }
                }
            }
        }

        public class Item
        {
            public string Name;
        }
    }
}