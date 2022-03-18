using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Extensions;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_4041 : RavenTestBase
    {
        public RavenDB_4041(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void streaming_returns_metadata()
        {
            using (var store = GetDocumentStore())
            {
                var index = new Customers_ByName();
                index.Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new Customer { Name = "John", Address = "Tel Aviv" });
                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var enumerator = session.Advanced.Stream<Customer>(startsWith:"customers/");

                    while (enumerator.MoveNext())
                    {
                        Assert.NotNull(enumerator.Current.Document.Id);
                        Assert.Equal("John", enumerator.Current.Document.Name);
                        Assert.Equal("Tel Aviv", enumerator.Current.Document.Address);

                        Assert.NotNull(enumerator.Current.Id);
                        Assert.NotNull(enumerator.Current.ChangeVector);
                        Assert.NotNull(enumerator.Current.Metadata[Constants.Documents.Metadata.RavenClrType]);
                        Assert.NotNull(enumerator.Current.Metadata[Constants.Documents.Metadata.Collection]);
                        Assert.NotNull(enumerator.Current.Metadata[Constants.Documents.Metadata.LastModified]);
                    }
                }
            }
        }

        [Fact]
        public async Task streaming_returns_metadata_async()
        {
            using (var store = GetDocumentStore())
            {
                var index = new Customers_ByName();
                index.Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new Customer { Name = "John", Address = "Tel Aviv" });
                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);

                using (var session = store.OpenAsyncSession())
                {
                    var enumerator = await session.Advanced.StreamAsync<Customer>(startsWith:"customers/");

                    while (await enumerator.MoveNextAsync())
                    {
                        Assert.NotNull(enumerator.Current.Document.Id);
                        Assert.Equal("John", enumerator.Current.Document.Name);
                        Assert.Equal("Tel Aviv", enumerator.Current.Document.Address);

                        Assert.NotNull(enumerator.Current.Id);
                        Assert.NotNull(enumerator.Current.ChangeVector);
                        Assert.NotNull(enumerator.Current.Metadata[Constants.Documents.Metadata.RavenClrType]);
                        Assert.NotNull(enumerator.Current.Metadata[Constants.Documents.Metadata.Collection]);
                        Assert.NotNull(enumerator.Current.Metadata[Constants.Documents.Metadata.LastModified]);
                    }
                }
            }
        }

        [Fact]
        public void streaming_query_returns_metadata()
        {
            using (var store = GetDocumentStore())
            {
                var index = new Customers_ByName();
                index.Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new Customer { Name = "John", Address = "Tel Aviv" });
                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var query = session.Query<Customer>(index.IndexName);
                    var enumerator = session.Advanced.Stream(query);

                    while (enumerator.MoveNext())
                    {
                        Assert.NotNull(enumerator.Current.Document.Id);
                        Assert.Equal("John", enumerator.Current.Document.Name);
                        Assert.Equal("Tel Aviv", enumerator.Current.Document.Address);

                        Assert.NotNull(enumerator.Current.Id);
                        Assert.NotNull(enumerator.Current.ChangeVector);
                        Assert.NotNull(enumerator.Current.Metadata[Constants.Documents.Metadata.RavenClrType]);
                        Assert.NotNull(enumerator.Current.Metadata[Constants.Documents.Metadata.Collection]);
                        Assert.NotNull(enumerator.Current.Metadata[Constants.Documents.Metadata.IndexScore]);
                        Assert.NotNull(enumerator.Current.Metadata[Constants.Documents.Metadata.LastModified]); ;
                    }
                }
            }
        }

        [Fact]
        public async Task streaming_query_returns_metadata_async()
        {
            using (var store = GetDocumentStore())
            {
                var index = new Customers_ByName();
                index.Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new Customer { Name = "John", Address = "Tel Aviv" });
                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);

                using (var session = store.OpenAsyncSession())
                {
                    var query = session.Query<Customer>(index.IndexName);
                    var enumerator = await session.Advanced.StreamAsync(query);

                    while (await enumerator.MoveNextAsync())
                    {
                        Assert.NotNull(enumerator.Current.Document.Id);
                        Assert.Equal("John", enumerator.Current.Document.Name);
                        Assert.Equal("Tel Aviv", enumerator.Current.Document.Address);

                        Assert.NotNull(enumerator.Current.Id);
                        Assert.NotNull(enumerator.Current.ChangeVector);
                        Assert.NotNull(enumerator.Current.Metadata[Constants.Documents.Metadata.RavenClrType]);
                        Assert.NotNull(enumerator.Current.Metadata[Constants.Documents.Metadata.Collection]);
                        Assert.NotNull(enumerator.Current.Metadata[Constants.Documents.Metadata.IndexScore]);
                        Assert.NotNull(enumerator.Current.Metadata[Constants.Documents.Metadata.LastModified]);
                    }
                }
            }
        }

        [Fact]
        public void returns_metadata()
        {
            using (var store = GetDocumentStore())
            {
                var index = new Customers_ByName();
                index.Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new Customer { Name = "John", Address = "Tel Aviv" });
                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var customer = session.Query<Customer>().FirstOrDefault();
                    Assert.NotNull(customer);
                    Assert.NotNull(customer.Id);
                    Assert.Equal(customer.Name, "John");
                    Assert.Equal(customer.Address, "Tel Aviv");

                    var metadata = session.Advanced.GetMetadataFor(customer);
                    Assert.NotNull(metadata[Constants.Documents.Metadata.ChangeVector]);
                    Assert.NotNull(metadata[Constants.Documents.Metadata.RavenClrType]);
                    Assert.NotNull(metadata[Constants.Documents.Metadata.Collection]);
                    Assert.NotNull(metadata[Constants.Documents.Metadata.LastModified]);
                }
            }
        }

        [Fact]
        public void returns_metadata_async()
        {
            using (var store = GetDocumentStore())
            {
                var index = new Customers_ByName();
                index.Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new Customer { Name = "John", Address = "Tel Aviv" });
                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);

                using (var session = store.OpenAsyncSession())
                {
                    var customerAsync = session.Query<Customer>().FirstOrDefaultAsync();
                    Assert.NotNull(customerAsync);
                    var customer = customerAsync.Result;
                    Assert.NotNull(customer.Id);
                    Assert.Equal(customer.Name, "John");
                    Assert.Equal(customer.Address, "Tel Aviv");

                    var metadata = session.Advanced.GetMetadataFor(customer);
                    Assert.NotNull(metadata[Constants.Documents.Metadata.ChangeVector]);
                    Assert.NotNull(metadata[Constants.Documents.Metadata.RavenClrType]);
                    Assert.NotNull(metadata[Constants.Documents.Metadata.Collection]);
                    Assert.NotNull(metadata[Constants.Documents.Metadata.LastModified]);
                }
            }
        }

        [Fact]
        public void load_returns_metadata()
        {
            using (var store = GetDocumentStore())
            {
                var index = new Customers_ByName();
                index.Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new Customer { Name = "John", Address = "Tel Aviv" });
                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var customer = session.Load<Customer>("customers/1-A");
                    Assert.NotNull(customer);
                    Assert.NotNull(customer.Id);
                    Assert.Equal(customer.Name, "John");
                    Assert.Equal(customer.Address, "Tel Aviv");

                    var metadata = session.Advanced.GetMetadataFor(customer);
                    Assert.NotNull(metadata[Constants.Documents.Metadata.ChangeVector]);
                    Assert.NotNull(metadata[Constants.Documents.Metadata.RavenClrType]);
                    Assert.NotNull(metadata[Constants.Documents.Metadata.Collection]);
                    Assert.NotNull(metadata[Constants.Documents.Metadata.LastModified]);
                }
            }
        }

        [Fact]
        public async Task load_returns_metadata_async()
        {
            using (var store = GetDocumentStore())
            {
                var index = new Customers_ByName();
                index.Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new Customer { Name = "John", Address = "Tel Aviv" });
                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);

                using (var session = store.OpenAsyncSession())
                {
                    var customer = await session.LoadAsync<Customer>("customers/1-A");
                    Assert.NotNull(customer);
                    Assert.NotNull(customer.Id);
                    Assert.Equal(customer.Name, "John");
                    Assert.Equal(customer.Address, "Tel Aviv");

                    var metadata = session.Advanced.GetMetadataFor(customer);
                    Assert.NotNull(metadata[Constants.Documents.Metadata.ChangeVector]);
                    Assert.NotNull(metadata[Constants.Documents.Metadata.RavenClrType]);
                    Assert.NotNull(metadata[Constants.Documents.Metadata.Collection]);
                    Assert.NotNull(metadata[Constants.Documents.Metadata.LastModified]);
                }
            }
        }

        [Fact]
        public void load_with_big_key_returns_metadata()
        {
            using (var store = GetDocumentStore())
            {
                var index = new Customers_ByName();
                index.Execute(store);
                var id = new string('a', 130);

                using (var session = store.OpenSession())
                {
                    session.Store(new Customer { Name = "John", Address = "Tel Aviv" }, id);
                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);

                using (var commands = store.Commands())
                {
                    var customer = commands.Get(id);

                    Assert.NotNull(customer);

                    var metadata = customer.BlittableJson.GetMetadata();
                    var key = metadata.GetId();
                    Assert.NotNull(key);

                    dynamic customerDynamic = customer;

                    Assert.Equal(customerDynamic.Name.ToString(), "John");
                    Assert.Equal(customerDynamic.Address.ToString(), "Tel Aviv");

                    Assert.NotNull(metadata.GetChangeVector());
                    Assert.NotNull(metadata.GetLastModified());

                    object _;
                    Assert.NotNull(metadata.TryGetMember(Constants.Documents.Metadata.RavenClrType, out _));
                    Assert.NotNull(metadata.TryGetMember(Constants.Documents.Metadata.Collection, out _));
                    Assert.NotNull(metadata.TryGetMember(Constants.Documents.Metadata.LastModified, out _));
                }
            }
        }

        [Fact]
        public async Task load_with_big_key_returns_metadata_async()
        {
            using (var store = GetDocumentStore())
            {
                var index = new Customers_ByName();
                index.Execute(store);
                var id = new string('a', 130);

                using (var session = store.OpenSession())
                {
                    session.Store(new Customer { Name = "John", Address = "Tel Aviv" }, id);
                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);

                using (var commands = store.Commands())
                {
                    var customer = await commands.GetAsync(id);

                    Assert.NotNull(customer);

                    var metadata = customer.BlittableJson.GetMetadata();
                    var key = metadata.GetId();
                    Assert.NotNull(key);

                    dynamic customerDynamic = customer;

                    Assert.Equal(customerDynamic.Name.ToString(), "John");
                    Assert.Equal(customerDynamic.Address.ToString(), "Tel Aviv");

                    Assert.NotNull(metadata.GetChangeVector());
                    Assert.NotNull(metadata.GetLastModified());

                    object _;
                    Assert.NotNull(metadata.TryGetMember(Constants.Documents.Metadata.RavenClrType, out _));
                    Assert.NotNull(metadata.TryGetMember(Constants.Documents.Metadata.Collection, out _));
                    Assert.NotNull(metadata.TryGetMember(Constants.Documents.Metadata.LastModified, out _));
                }
            }
        }

        [Fact]
        public void multi_load_returns_metadata()
        {
            using (var store = GetDocumentStore())
            {
                var index = new Customers_ByName();
                index.Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new Customer { Name = "John", Address = "Tel Aviv" });
                    session.Store(new Customer { Name = "John", Address = "Tel Aviv" });
                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var customers = session.Load<Customer>(new List<string> { "customers/1-A", "customers/2-A" });

                    foreach (var customer in customers.Values)
                    {
                        Assert.NotNull(customer);
                        Assert.NotNull(customer.Id);
                        Assert.Equal(customer.Name, "John");
                        Assert.Equal(customer.Address, "Tel Aviv");

                        var metadata = session.Advanced.GetMetadataFor(customer);
                        Assert.NotNull(metadata[Constants.Documents.Metadata.ChangeVector]);
                        Assert.NotNull(metadata[Constants.Documents.Metadata.RavenClrType]);
                        Assert.NotNull(metadata[Constants.Documents.Metadata.Collection]);
                        Assert.NotNull(metadata[Constants.Documents.Metadata.LastModified]);
                    }
                }
            }
        }

        [Fact]
        public void load_lazily_returns_metadata()
        {
            using (var store = GetDocumentStore())
            {
                var index = new Customers_ByName();
                index.Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new Customer { Name = "John", Address = "Tel Aviv" });
                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var customerLazy = session.Advanced.Lazily.Load<Customer>("customers/1-A");
                    var customer = customerLazy.Value;
                    Assert.NotNull(customer);
                    Assert.NotNull(customer.Id);
                    Assert.Equal(customer.Name, "John");
                    Assert.Equal(customer.Address, "Tel Aviv");

                    var metadata = session.Advanced.GetMetadataFor(customer);
                    Assert.NotNull(metadata[Constants.Documents.Metadata.ChangeVector]);
                    Assert.NotNull(metadata[Constants.Documents.Metadata.RavenClrType]);
                    Assert.NotNull(metadata[Constants.Documents.Metadata.Collection]);
                    Assert.NotNull(metadata[Constants.Documents.Metadata.LastModified]);
                }
            }
        }

        [Fact]
        public void load_lazily_returns_metadata_async()
        {
            using (var store = GetDocumentStore())
            {
                var index = new Customers_ByName();
                index.Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new Customer { Name = "John", Address = "Tel Aviv" });
                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);

                using (var session = store.OpenAsyncSession())
                {
                    var customerLazy = session.Advanced.Lazily.LoadAsync<Customer>("customers/1-A");
                    var customer = customerLazy.Value.Result;
                    Assert.NotNull(customer);
                    Assert.NotNull(customer.Id);
                    Assert.Equal(customer.Name, "John");
                    Assert.Equal(customer.Address, "Tel Aviv");

                    var metadata = session.Advanced.GetMetadataFor(customer);
                    Assert.NotNull(metadata[Constants.Documents.Metadata.ChangeVector]);
                    Assert.NotNull(metadata[Constants.Documents.Metadata.RavenClrType]);
                    Assert.NotNull(metadata[Constants.Documents.Metadata.Collection]);
                    Assert.NotNull(metadata[Constants.Documents.Metadata.LastModified]);
                }
            }
        }

        [Fact]
        public void get_returns_metadata()
        {
            using (var store = GetDocumentStore())
            {
                var index = new Customers_ByName();
                index.Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new Customer { Name = "John", Address = "Tel Aviv" });
                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);

                using (var commands = store.Commands())
                {
                    var customer = commands.Get("customers/1-A");

                    Assert.NotNull(customer);

                    var metadata = customer.BlittableJson.GetMetadata();
                    var id = metadata.GetId();
                    Assert.NotNull(id);

                    dynamic customerDynamic = customer;

                    Assert.Equal(customerDynamic.Name.ToString(), "John");
                    Assert.Equal(customerDynamic.Address.ToString(), "Tel Aviv");

                    Assert.NotNull(metadata.GetChangeVector());
                    Assert.NotNull(metadata.GetLastModified());

                    object _;
                    Assert.NotNull(metadata.TryGetMember(Constants.Documents.Metadata.RavenClrType, out _));
                    Assert.NotNull(metadata.TryGetMember(Constants.Documents.Metadata.Collection, out _));
                    Assert.NotNull(metadata.TryGetMember(Constants.Documents.Metadata.LastModified, out _));
                }
            }
        }

        [Fact]
        public async Task get_returns_metadata_async()
        {
            using (var store = GetDocumentStore())
            {
                var index = new Customers_ByName();
                index.Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new Customer { Name = "John", Address = "Tel Aviv" });
                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);

                using (var commands = store.Commands())
                {
                    var customer = await commands.GetAsync("customers/1-A");

                    Assert.NotNull(customer);

                    var metadata = customer.BlittableJson.GetMetadata();
                    var id = metadata.GetId();
                    Assert.NotNull(id);

                    dynamic customerDynamic = customer;

                    Assert.Equal(customerDynamic.Name.ToString(), "John");
                    Assert.Equal(customerDynamic.Address.ToString(), "Tel Aviv");

                    Assert.NotNull(metadata.GetChangeVector());
                    Assert.NotNull(metadata.GetLastModified());

                    object _;
                    Assert.NotNull(metadata.TryGetMember(Constants.Documents.Metadata.RavenClrType, out _));
                    Assert.NotNull(metadata.TryGetMember(Constants.Documents.Metadata.Collection, out _));
                    Assert.NotNull(metadata.TryGetMember(Constants.Documents.Metadata.LastModified, out _));
                }
            }
        }

        private class Customer
        {
            public string Id { get; set; }
            public string Name { get; set; }
            public string Address { get; set; }
        }

        private class Customers_ByName : AbstractIndexCreationTask<Customer>
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


