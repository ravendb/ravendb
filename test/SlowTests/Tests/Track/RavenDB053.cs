using System;
using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Xunit;

namespace SlowTests.Tests.Track
{
    public class LinqTest : RavenTestBase
    {
        private class JoinedChildTransport
        {
            public string ChildId { get; set; }
            public string TransportId { get; set; }
            public string Name { get; set; }

            public override string ToString()
            {
                return string.Format("ChildId: {0}, TransportId: {1}, Name: {2}", ChildId, TransportId, Name);
            }
        }

        private class Child
        {
            public string Id { get; set; }
            public string Name { get; set; }
        }

        private class Transport
        {
            public string Id { get; set; }
            public string ChildId { get; set; }
        }

        private class TransportsIndex : AbstractMultiMapIndexCreationTask<JoinedChildTransport>
        {
            public TransportsIndex()
            {
                AddMap<Child>(childList => from child in childList
                                           select new
                                           {
                                               ChildId = child.Id,
                                               TransportId = (dynamic)null,
                                               child.Name,
                                           });

                AddMap<Transport>(transportList => from transport in transportList
                                                   select new
                                                   {
                                                       transport.ChildId,
                                                       TransportId = transport.Id,
                                                       Name = (dynamic)null,
                                                   });

                Reduce = results => from result in results
                                    group result by result.ChildId
                                    into g
                                    from transport in g.Where(transport => transport.TransportId != null).DefaultIfEmpty()
                                    from child in g.Where(barn => barn.Name != null).DefaultIfEmpty()
                                    select new { ChildId = g.Key, transport.TransportId, child.Name };

                Store(x => x.ChildId, FieldStorage.Yes);
                Store(x => x.TransportId, FieldStorage.Yes);
                Store(x => x.Name, FieldStorage.Yes);
            }
        }

        [Fact]
        public void ChildrenHasMultipleTransports_Raven()
        {
            using (var store = GetDocumentStore())
            {
                // Create Index
                new TransportsIndex().Execute(store);

                using (var session = store.OpenSession())
                {
                    // Store two children
                    session.Store(new Child { Id = "B1", Name = "Thor Arne" });
                    session.Store(new Child { Id = "B2", Name = "Ståle" });

                    // Store four Transports
                    session.Store(new Transport { Id = "A1", ChildId = "B1" });
                    session.Store(new Transport { Id = "A2", ChildId = "B1" });
                    session.Store(new Transport { Id = "A3", ChildId = "B2" });
                    session.Store(new Transport { Id = "A4", ChildId = "B2" });

                    session.SaveChanges();

                    var transports = session.Query<JoinedChildTransport, TransportsIndex>()
                        .Customize(x => x.WaitForNonStaleResults(TimeSpan.FromMinutes(100)))
                        .OrderBy(x => x.TransportId)
                        .OrderBy(x => x.ChildId)
                        .ProjectInto<JoinedChildTransport>()
                        .ToList();

                    RavenTestHelper.AssertNoIndexErrors(store);

                    Assert.Equal(4, transports.Count);

                    // skyssavtaler for B1
                    Assert.Equal("A1", transports[0].TransportId);
                    Assert.Equal("B1", transports[0].ChildId);
                    Assert.Equal("Thor Arne", transports[0].Name);

                    Assert.Equal("A2", transports[1].TransportId);
                    Assert.Equal("B1", transports[1].ChildId);
                    Assert.Equal("Thor Arne", transports[0].Name);

                    // skyssavtaler for B2
                    Assert.Equal("A3", transports[2].TransportId);
                    Assert.Equal("B2", transports[2].ChildId);
                    Assert.Equal("Ståle", transports[2].Name);

                    Assert.Equal("A4", transports[3].TransportId);
                    Assert.Equal("B2", transports[3].ChildId);
                    Assert.Equal("Ståle", transports[3].Name);
                }
            }
        }
    }
}