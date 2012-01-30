using System;
using System.Linq;
using Raven.Client;
using Raven.Client.Document;
using Raven.Client.Linq;
using Raven.Server;
using Xunit;

namespace Raven.Tests.Queries
{
    public class Includes : RemoteClientTest, IDisposable
    {
        private readonly IDocumentStore store;
        private readonly RavenDbServer server;

        public Includes()
        {
            server = GetNewServer(8079, GetPath(DbName));

            store = new DocumentStore
            {
                Url = "http://localhost:8079"
            }.Initialize();
        }

        [Fact]
        public void Can_use_includes_within_multi_load()
        {
            using (var session = store.OpenSession())
            {
                session.Store(new Customer { Id = "users/1", Name = "Daniel Lang"});
                session.Store(new Customer { Id = "users/2", Name = "Oren Eini"});

                session.Store(new Order { CustomerId = "users/1", Number = "1"});
                session.Store(new Order { CustomerId = "users/1", Number = "2"});
                session.Store(new Order { CustomerId = "users/2", Number = "3"});

                session.SaveChanges();
            }

            using (var session = store.OpenSession())
            {
                var orders = session.Query<Order>()
                    .Customize(x => x.WaitForNonStaleResultsAsOfLastWrite())
                    .Include(x => x.CustomerId)
                    .ToList();

                Assert.Equal(3, orders.Count);
                Assert.Equal(1, session.Advanced.NumberOfRequests);

                var customers = session.Load<Customer>(orders.Select(x => x.CustomerId));
                Assert.Equal(2, customers.Length);
                Assert.Equal(1, session.Advanced.NumberOfRequests);
            }
        }

        public void Dispose()
        {
            store.Dispose();
            server.Dispose();
            ClearDatabaseDirectory();
        }

        public class Customer
        {
            public string Id { get; set; }
            public string Name { get; set; }
        }

        public class Order
        {
            public string Id { get; set; }
            public string CustomerId { get; set; }
            public string Number { get; set; }
        }
    }
}