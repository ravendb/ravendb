using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Xunit;

namespace SlowTests.MailingList.Thor
{
    public class LinqTest : RavenTestBase
    {
        private class Child
        {
            public string Id { get; set; }
            public string Name { get; set; }
        }

        private class JoinedChildTransport
        {
            public string ChildId { get; set; }
            public string TransportId { get; set; }
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
                                               TransportId = (string)null,
                                               Name = child.Name,
                                           });

                AddMap<Transport>(transportList => from transport in transportList
                                                   select new
                                                   {
                                                       ChildId = transport.ChildId,
                                                       TransportId = transport.Id,
                                                       Name = (string)null,
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
        public void SingleTransportWithoutChildren_Raven()
        {
            using (var store = GetDocumentStore())
            {
                // Create Index
                new TransportsIndex().Execute(store);

                using (var session = store.OpenSession())
                {
                    // Store a single Transport
                    session.Store(new Transport { Id = "A1", ChildId = "Test" });

                    session.SaveChanges();

                    var query = session.Query<JoinedChildTransport, TransportsIndex>().Customize(x => x.WaitForNonStaleResults())
                        //                 .AsProjection<JoinedChildTransport>()
                        ;

                    var transports = query.ToList();
                    Assert.Equal(1, transports.Count);

                    Assert.Equal("A1", transports[0].TransportId);
                    Assert.Equal("Test", transports[0].ChildId);
                    Assert.Null(transports[0].Name);
                }
            }
        }

        [Fact]
        public void SingleChildAndNoTransport_Raven()
        {
            using (var store = GetDocumentStore())
            {
                // Create Index
                new TransportsIndex().Execute(store);

                using (var session = store.OpenSession())
                {
                    // Store a single Transport
                    session.Store(new Child { Id = "B1", Name = "Thor Arne" });

                    session.SaveChanges();

                    var query = session.Query<JoinedChildTransport, TransportsIndex>().Customize(x => x.WaitForNonStaleResults())
                        //                 .AsProjection<JoinedChildTransport>()
                        ;

                    var transports = query.ToList();
                    Assert.Equal(1, transports.Count);  // Also check for indexing error
                }
            }
        }

        [Fact]
        public void MultipleChildrenWithMultipleTransports_Raven()
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

                    var query = session.Query<JoinedChildTransport, TransportsIndex>()
                        .Customize(x => x.WaitForNonStaleResults())
                        .OrderBy(x => x.TransportId)
                        //                 .AsProjection<JoinedChildTransport>()
                        ;

                    var transports = query.ToList();
                    Assert.Equal(4, transports.Count);

                    // The test below may have to change to account for unpredictable order, but we never even get the correct number of hits

                    // transports for B1
                    Assert.Equal("A1", transports[0].TransportId);
                    Assert.Equal("B1", transports[0].ChildId);
                    Assert.Equal("Thor Arne", transports[0].Name);

                    Assert.Equal("A2", transports[1].TransportId);
                    Assert.Equal("B1", transports[1].ChildId);
                    Assert.Equal("Thor Arne", transports[1].Name);

                    // transports for B2
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
