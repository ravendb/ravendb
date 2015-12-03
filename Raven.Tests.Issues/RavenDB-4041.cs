using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Client;
using Raven.Client.Indexes;
using Raven.Tests.Helpers;
using Xunit;

namespace Raven.Tests.Issues
{
    public class RavenDB_4041 : RavenTestBase
    {
        [Fact]
        public void streaming_returns_metadata()
        {
            using (var store = NewRemoteDocumentStore(fiddler: true))
            {
                var index = new Customers_ByName();
                index.Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new Customer { Name = "John", Address = "Tel Aviv" });
                    session.SaveChanges();
                }

                WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var enumerator = session.Advanced.Stream<Customer>("customers/");

                    while (enumerator.MoveNext())
                    {
                        Assert.NotNull(enumerator.Current.Document.Id);
                        Assert.Equal("John", enumerator.Current.Document.Name);
                        Assert.Equal("Tel Aviv", enumerator.Current.Document.Address);

                        Assert.NotNull(enumerator.Current.Key);
                        Assert.NotNull(enumerator.Current.Etag);
                        Assert.NotNull(enumerator.Current.Metadata.Value<string>(Constants.RavenClrType));
                        Assert.NotNull(enumerator.Current.Metadata.Value<string>(Constants.RavenEntityName));
                        Assert.NotNull(enumerator.Current.Metadata.Value<string>(Constants.LastModified));
                        Assert.NotNull(enumerator.Current.Metadata.Value<string>(Constants.RavenLastModified));
                    }
                }
            }
        }

        [Fact]
        public async Task streaming_returns_metadata_async()
        {
            using (var store = NewDocumentStore())
            {
                var index = new Customers_ByName();
                index.Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new Customer { Name = "John", Address = "Tel Aviv" });
                    session.SaveChanges();
                }

                WaitForIndexing(store);

                using (var session = store.OpenAsyncSession())
                {
                    var enumerator = await session.Advanced.StreamAsync<Customer>("customers/");

                    while (await enumerator.MoveNextAsync())
                    {
                        Assert.NotNull(enumerator.Current.Document.Id);
                        Assert.Equal("John", enumerator.Current.Document.Name);
                        Assert.Equal("Tel Aviv", enumerator.Current.Document.Address);

                        Assert.NotNull(enumerator.Current.Key);
                        Assert.NotNull(enumerator.Current.Etag);
                        Assert.NotNull(enumerator.Current.Metadata.Value<string>(Constants.RavenClrType));
                        Assert.NotNull(enumerator.Current.Metadata.Value<string>(Constants.RavenEntityName));
                        Assert.NotNull(enumerator.Current.Metadata.Value<string>(Constants.LastModified));
                        Assert.NotNull(enumerator.Current.Metadata.Value<string>(Constants.RavenLastModified));
                    }
                }
            }
        }

        [Fact]
        public void streaming_query_returns_metadata()
        {
            using (var store = NewDocumentStore())
            {
                var index = new Customers_ByName();
                index.Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new Customer { Name = "John", Address = "Tel Aviv" });
                    session.SaveChanges();
                }

                WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var query = session.Query<Customer>(index.IndexName);
                    var enumerator = session.Advanced.Stream(query);

                    while (enumerator.MoveNext())
                    {
                        Assert.NotNull(enumerator.Current.Document.Id);
                        Assert.Equal("John", enumerator.Current.Document.Name);
                        Assert.Equal("Tel Aviv", enumerator.Current.Document.Address);

                        Assert.NotNull(enumerator.Current.Key);
                        Assert.NotNull(enumerator.Current.Etag);
                        Assert.NotNull(enumerator.Current.Metadata.Value<string>(Constants.RavenClrType));
                        Assert.NotNull(enumerator.Current.Metadata.Value<string>(Constants.RavenEntityName));
                        Assert.NotNull(enumerator.Current.Metadata.Value<string>(Constants.TemporaryScoreValue));
                        Assert.NotNull(enumerator.Current.Metadata.Value<string>(Constants.LastModified));
                        Assert.NotNull(enumerator.Current.Metadata.Value<string>(Constants.RavenLastModified));
                    }
                }
            }
        }

        [Fact]
        public async Task streaming_query_returns_metadata_async()
        {
            using (var store = NewDocumentStore())
            {
                var index = new Customers_ByName();
                index.Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new Customer { Name = "John", Address = "Tel Aviv" });
                    session.SaveChanges();
                }

                WaitForIndexing(store);

                using (var session = store.OpenAsyncSession())
                {
                    var query = session.Query<Customer>(index.IndexName);
                    var enumerator = await session.Advanced.StreamAsync(query);

                    while (await enumerator.MoveNextAsync())
                    {
                        Assert.NotNull(enumerator.Current.Document.Id);
                        Assert.Equal("John", enumerator.Current.Document.Name);
                        Assert.Equal("Tel Aviv", enumerator.Current.Document.Address);

                        Assert.NotNull(enumerator.Current.Key);
                        Assert.NotNull(enumerator.Current.Etag);
                        Assert.NotNull(enumerator.Current.Metadata.Value<string>(Constants.RavenClrType));
                        Assert.NotNull(enumerator.Current.Metadata.Value<string>(Constants.RavenEntityName));
                        Assert.NotNull(enumerator.Current.Metadata.Value<string>(Constants.TemporaryScoreValue));
                        Assert.NotNull(enumerator.Current.Metadata.Value<string>(Constants.LastModified));
                        Assert.NotNull(enumerator.Current.Metadata.Value<string>(Constants.RavenLastModified));
                    }
                }
            }
        }

        [Fact]
        public void returns_metadata()
        {
            using (var store = NewDocumentStore())
            {
                var index = new Customers_ByName();
                index.Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new Customer { Name = "John", Address = "Tel Aviv" });
                    session.SaveChanges();
                }

                WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var customer = session.Query<Customer>().FirstOrDefault();
                    Assert.NotNull(customer);
                    Assert.NotNull(customer.Id);
                    Assert.Equal(customer.Name, "John");
                    Assert.Equal(customer.Address, "Tel Aviv");

                    var metadata = session.Advanced.GetMetadataFor(customer);
                    Assert.NotNull(metadata.Value<string>("@etag"));
                    Assert.NotNull(metadata.Value<string>(Constants.RavenClrType));
                    Assert.NotNull(metadata.Value<string>(Constants.RavenEntityName));
                    Assert.NotNull(metadata.Value<string>(Constants.LastModified));
                    Assert.NotNull(metadata.Value<string>(Constants.RavenLastModified));
                }
            }
        }

        [Fact]
        public void returns_metadata_async()
        {
            using (var store = NewDocumentStore())
            {
                var index = new Customers_ByName();
                index.Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new Customer { Name = "John", Address = "Tel Aviv" });
                    session.SaveChanges();
                }

                WaitForIndexing(store);

                using (var session = store.OpenAsyncSession())
                {
                    var customerAsync = session.Query<Customer>().FirstOrDefaultAsync();
                    Assert.NotNull(customerAsync);
                    var customer = customerAsync.Result;
                    Assert.NotNull(customer.Id);
                    Assert.Equal(customer.Name, "John");
                    Assert.Equal(customer.Address, "Tel Aviv");

                    var metadata = session.Advanced.GetMetadataFor(customer);
                    Assert.NotNull(metadata.Value<string>("@etag"));
                    Assert.NotNull(metadata.Value<string>(Constants.RavenClrType));
                    Assert.NotNull(metadata.Value<string>(Constants.RavenEntityName));
                    Assert.NotNull(metadata.Value<string>(Constants.LastModified));
                    Assert.NotNull(metadata.Value<string>(Constants.RavenLastModified));
                }
            }
        }

        [Fact]
        public void load_returns_metadata()
        {
            using (var store = NewDocumentStore())
            {
                var index = new Customers_ByName();
                index.Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new Customer { Name = "John", Address = "Tel Aviv" });
                    session.SaveChanges();
                }

                WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var customer = session.Load<Customer>("customers/1");
                    Assert.NotNull(customer);
                    Assert.NotNull(customer.Id);
                    Assert.Equal(customer.Name, "John");
                    Assert.Equal(customer.Address, "Tel Aviv");

                    var metadata = session.Advanced.GetMetadataFor(customer);
                    Assert.NotNull(metadata.Value<string>("@etag"));
                    Assert.NotNull(metadata.Value<string>(Constants.RavenClrType));
                    Assert.NotNull(metadata.Value<string>(Constants.RavenEntityName));
                    Assert.NotNull(metadata.Value<string>(Constants.LastModified));
                    Assert.NotNull(metadata.Value<string>(Constants.RavenLastModified));
                }
            }
        }

        [Fact]
        public async Task load_returns_metadata_async()
        {
            using (var store = NewDocumentStore())
            {
                var index = new Customers_ByName();
                index.Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new Customer { Name = "John", Address = "Tel Aviv" });
                    session.SaveChanges();
                }

                WaitForIndexing(store);

                using (var session = store.OpenAsyncSession())
                {
                    var customer = await session.LoadAsync<Customer>("customers/1");
                    Assert.NotNull(customer);
                    Assert.NotNull(customer.Id);
                    Assert.Equal(customer.Name, "John");
                    Assert.Equal(customer.Address, "Tel Aviv");

                    var metadata = session.Advanced.GetMetadataFor(customer);
                    Assert.NotNull(metadata.Value<string>("@etag"));
                    Assert.NotNull(metadata.Value<string>(Constants.RavenClrType));
                    Assert.NotNull(metadata.Value<string>(Constants.RavenEntityName));
                    Assert.NotNull(metadata.Value<string>(Constants.LastModified));
                    Assert.NotNull(metadata.Value<string>(Constants.RavenLastModified));
                }
            }
        }

        [Fact]
        public void load_with_big_key_returns_metadata()
        {
            using (var store = NewDocumentStore())
            {
                var index = new Customers_ByName();
                index.Execute(store);
                var id = new string('a', 130);

                using (var session = store.OpenSession())
                {
                    session.Store(new Customer { Name = "John", Address = "Tel Aviv" }, id);
                    session.SaveChanges();
                }

                WaitForIndexing(store);

                var customer = store.DatabaseCommands.Get(id);
                Assert.NotNull(customer);
                Assert.NotNull(customer.Key);
                Assert.Equal(customer.DataAsJson.Value<string>("Name"), "John");
                Assert.Equal(customer.DataAsJson.Value<string>("Address"), "Tel Aviv");

                Assert.NotNull(customer.Etag);
                Assert.NotNull(customer.LastModified);
                Assert.NotNull(customer.Metadata.Value<string>(Constants.RavenClrType));
                Assert.NotNull(customer.Metadata.Value<string>(Constants.RavenEntityName));
                Assert.NotNull(customer.Metadata.Value<string>(Constants.LastModified));
                Assert.NotNull(customer.Metadata.Value<string>(Constants.RavenLastModified));
            }
        }

        [Fact]
        public async Task load_with_big_key_returns_metadata_async()
        {
            using (var store = NewDocumentStore())
            {
                var index = new Customers_ByName();
                index.Execute(store);
                var id = new string('a', 130);

                using (var session = store.OpenSession())
                {
                    session.Store(new Customer { Name = "John", Address = "Tel Aviv" }, id);
                    session.SaveChanges();
                }

                WaitForIndexing(store);

                var customer = await store.AsyncDatabaseCommands.GetAsync(id);
                Assert.NotNull(customer);
                Assert.NotNull(customer.Key);
                Assert.Equal(customer.DataAsJson.Value<string>("Name"), "John");
                Assert.Equal(customer.DataAsJson.Value<string>("Address"), "Tel Aviv");

                Assert.NotNull(customer.Etag);
                Assert.NotNull(customer.LastModified);
                Assert.NotNull(customer.Metadata.Value<string>(Constants.RavenClrType));
                Assert.NotNull(customer.Metadata.Value<string>(Constants.RavenEntityName));
                Assert.NotNull(customer.Metadata.Value<string>(Constants.LastModified));
                Assert.NotNull(customer.Metadata.Value<string>(Constants.RavenLastModified));
            }
        }

        [Fact]
        public void multi_load_returns_metadata()
        {
            using (var store = NewDocumentStore())
            {
                var index = new Customers_ByName();
                index.Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new Customer { Name = "John", Address = "Tel Aviv" });
                    session.Store(new Customer { Name = "John", Address = "Tel Aviv" });
                    session.SaveChanges();
                }

                WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var customers = session.Load<Customer>(new List<string> { "customers/1", "customers/2" });

                    foreach (var customer in customers)
                    {
                        Assert.NotNull(customer);
                        Assert.NotNull(customer.Id);
                        Assert.Equal(customer.Name, "John");
                        Assert.Equal(customer.Address, "Tel Aviv");

                        var metadata = session.Advanced.GetMetadataFor(customer);
                        Assert.NotNull(metadata.Value<string>("@etag"));
                        Assert.NotNull(metadata.Value<string>(Constants.RavenClrType));
                        Assert.NotNull(metadata.Value<string>(Constants.RavenEntityName));
                        Assert.NotNull(metadata.Value<string>(Constants.LastModified));
                        Assert.NotNull(metadata.Value<string>(Constants.RavenLastModified));
                    }
                }
            }
        }

        [Fact]
        public void load_lazily_returns_metadata()
        {
            using (var store = NewDocumentStore())
            {
                var index = new Customers_ByName();
                index.Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new Customer { Name = "John", Address = "Tel Aviv" });
                    session.SaveChanges();
                }

                WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var customerLazy = session.Advanced.Lazily.Load<Customer>("customers/1");
                    var customer = customerLazy.Value;
                    Assert.NotNull(customer);
                    Assert.NotNull(customer.Id);
                    Assert.Equal(customer.Name, "John");
                    Assert.Equal(customer.Address, "Tel Aviv");

                    var metadata = session.Advanced.GetMetadataFor(customer);
                    Assert.NotNull(metadata.Value<string>("@etag"));
                    Assert.NotNull(metadata.Value<string>(Constants.RavenClrType));
                    Assert.NotNull(metadata.Value<string>(Constants.RavenEntityName));
                    Assert.NotNull(metadata.Value<string>(Constants.LastModified));
                    Assert.NotNull(metadata.Value<string>(Constants.RavenLastModified));
                }
            }
        }

        [Fact]
        public void load_lazily_returns_metadata_async()
        {
            using (var store = NewDocumentStore())
            {
                var index = new Customers_ByName();
                index.Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new Customer { Name = "John", Address = "Tel Aviv" });
                    session.SaveChanges();
                }

                WaitForIndexing(store);

                using (var session = store.OpenAsyncSession())
                {
                    var customerLazy = session.Advanced.Lazily.LoadAsync<Customer>("customers/1");
                    var customer = customerLazy.Value.Result;
                    Assert.NotNull(customer);
                    Assert.NotNull(customer.Id);
                    Assert.Equal(customer.Name, "John");
                    Assert.Equal(customer.Address, "Tel Aviv");

                    var metadata = session.Advanced.GetMetadataFor(customer);
                    Assert.NotNull(metadata.Value<string>("@etag"));
                    Assert.NotNull(metadata.Value<string>(Constants.RavenClrType));
                    Assert.NotNull(metadata.Value<string>(Constants.RavenEntityName));
                    Assert.NotNull(metadata.Value<string>(Constants.LastModified));
                    Assert.NotNull(metadata.Value<string>(Constants.RavenLastModified));
                }
            }
        }

        [Fact]
        public void get_returns_metadata()
        {
            using (var store = NewDocumentStore())
            {
                var index = new Customers_ByName();
                index.Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new Customer { Name = "John", Address = "Tel Aviv" });
                    session.SaveChanges();
                }

                WaitForIndexing(store);

                var customer = store.DatabaseCommands.Get("customers/1");
                Assert.NotNull(customer);
                Assert.NotNull(customer.Key);
                Assert.Equal(customer.DataAsJson.Value<string>("Name"), "John");
                Assert.Equal(customer.DataAsJson.Value<string>("Address"), "Tel Aviv");

                Assert.NotNull(customer.Etag);
                Assert.NotNull(customer.LastModified);
                Assert.NotNull(customer.Metadata.Value<string>(Constants.RavenClrType));
                Assert.NotNull(customer.Metadata.Value<string>(Constants.RavenEntityName));
                Assert.NotNull(customer.Metadata.Value<string>(Constants.LastModified));
                Assert.NotNull(customer.Metadata.Value<string>(Constants.RavenLastModified));
            }
        }

        [Fact]
        public async Task get_returns_metadata_async()
        {
            using (var store = NewDocumentStore())
            {
                var index = new Customers_ByName();
                index.Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new Customer { Name = "John", Address = "Tel Aviv" });
                    session.SaveChanges();
                }

                WaitForIndexing(store);

                var customer = await store.AsyncDatabaseCommands.GetAsync("customers/1");
                Assert.NotNull(customer);
                Assert.NotNull(customer.Key);
                Assert.Equal(customer.DataAsJson.Value<string>("Name"), "John");
                Assert.Equal(customer.DataAsJson.Value<string>("Address"), "Tel Aviv");

                Assert.NotNull(customer.Etag);
                Assert.NotNull(customer.LastModified);
                Assert.NotNull(customer.Metadata.Value<string>(Constants.RavenClrType));
                Assert.NotNull(customer.Metadata.Value<string>(Constants.RavenEntityName));
                Assert.NotNull(customer.Metadata.Value<string>(Constants.LastModified));
                Assert.NotNull(customer.Metadata.Value<string>(Constants.RavenLastModified));
            }
        }

        public class Customer
        {
            public string Id { get; set; }
            public string Name { get; set; }
            public string Address { get; set; }
        }

        public class Customers_ByName : AbstractIndexCreationTask<Customer>
        {
            public Customers_ByName()
            {
                Map = customers => from customer in customers
                    select new
                    {
                        customer.Name
                    };
            }
        }
    }
}
 

