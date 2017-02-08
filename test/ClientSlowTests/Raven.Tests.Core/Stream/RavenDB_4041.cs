// -----------------------------------------------------------------------
//  <copyright file="QueryResultsStreaming.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using Raven.NewClient.Client.Indexes;
using Xunit;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.NewClient.Abstractions.Data;
using Tests.Infrastructure;

namespace NewClientTests.NewClient.Raven.Tests.Core.Stream
{
    public class RavenDB_4041 : RavenNewTestBase
    {
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
                        Assert.NotNull(enumerator.Current.Metadata[Constants.Metadata.Collection]);
                        Assert.NotNull(enumerator.Current.Metadata[Constants.Metadata.LastModified]);
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
                        Assert.NotNull(enumerator.Current.Metadata[Constants.Metadata.Collection]);
                        Assert.NotNull(enumerator.Current.Metadata[Constants.Metadata.LastModified]);
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
                        Assert.NotNull(enumerator.Current.Metadata[Constants.Metadata.Collection]);
                        Assert.NotNull(enumerator.Current.Metadata[Constants.Metadata.LastModified]);
                        Assert.NotNull(enumerator.Current.Metadata[Constants.Metadata.IndexScore]);
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
                        Assert.NotNull(enumerator.Current.Metadata[Constants.Metadata.Collection]);
                        Assert.NotNull(enumerator.Current.Metadata[Constants.Metadata.LastModified]);
                        Assert.NotNull(enumerator.Current.Metadata[Constants.Metadata.IndexScore]);
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
                    Assert.NotNull(metadata["@etag"]);
                    Assert.NotNull(metadata[Constants.Metadata.Collection]);
                    Assert.NotNull(metadata[Constants.Metadata.LastModified]);
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
                    Assert.NotNull(metadata["@etag"]);
                    Assert.NotNull(metadata[Constants.Metadata.Collection]);
                    Assert.NotNull(metadata[Constants.Metadata.LastModified]);
                }
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
