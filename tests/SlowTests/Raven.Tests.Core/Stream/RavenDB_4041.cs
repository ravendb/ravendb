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
using Raven.NewClient.Abstractions.Data;

namespace NewClientTests.NewClient.Raven.Tests.Core.Stream
{
    public class RavenDB_4041 : RavenTestBase
    {
        [Fact(Skip = "stream start with Not Implemented")]
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
                        Assert.NotNull(enumerator.Current.Metadata[Constants.Headers.RavenClrType]);
                        Assert.NotNull(enumerator.Current.Metadata[Constants.Headers.RavenEntityName]);
                        Assert.NotNull(enumerator.Current.Metadata[Constants.Headers.RavenLastModified]);
                        Assert.NotNull(enumerator.Current.Metadata[Constants.Headers.LastModified]);
                    }
                }
            }
        }

        [Fact(Skip = "StreamAsync Not Implemented")]
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
                        /*Assert.NotNull(enumerator.Current.Document.Id);
                        Assert.Equal("John", enumerator.Current.Document.Name);
                        Assert.Equal("Tel Aviv", enumerator.Current.Document.Address);

                        Assert.NotNull(enumerator.Current.Key);
                        Assert.NotNull(enumerator.Current.Etag);
                        Assert.NotNull(enumerator.Current.Metadata[Constants.Headers.RavenClrType]);
                        Assert.NotNull(enumerator.Current.Metadata[Constants.Headers.RavenEntityName]);
                        Assert.NotNull(enumerator.Current.Metadata[Constants.Headers.RavenLastModified]);
                        Assert.NotNull(enumerator.Current.Metadata[Constants.Headers.LastModified]);*/
                    }
                }
            }
        }

        [Fact(Skip = "stream start with Not Implemented")]
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
                        Assert.NotNull(enumerator.Current.Metadata[Constants.Headers.RavenClrType]);
                        Assert.NotNull(enumerator.Current.Metadata[Constants.Headers.RavenEntityName]);
                        Assert.NotNull(enumerator.Current.Metadata[Constants.Headers.RavenLastModified]);
                        Assert.NotNull(enumerator.Current.Metadata[Constants.Headers.LastModified]);
                        Assert.NotNull(enumerator.Current.Metadata[Constants.Metadata.IndexScore]);
                    }
                }
            }
        }

        [Fact(Skip = "StreamAsync Not Implemented")]
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
                       /* Assert.NotNull(enumerator.Current.Document.Id);
                        Assert.Equal("John", enumerator.Current.Document.Name);
                        Assert.Equal("Tel Aviv", enumerator.Current.Document.Address);

                        Assert.NotNull(enumerator.Current.Key);
                        Assert.NotNull(enumerator.Current.Etag);
                        Assert.NotNull(enumerator.Current.Metadata.Value<string>(Constants.RavenClrType));
                        Assert.NotNull(enumerator.Current.Metadata.Value<string>(Constants.RavenEntityName));
                        Assert.NotNull(enumerator.Current.Metadata.Value<string>(Constants.TemporaryScoreValue));
                        Assert.NotNull(enumerator.Current.Metadata.Value<string>(Constants.LastModified));
                        Assert.NotNull(enumerator.Current.Metadata.Value<string>(Constants.RavenLastModified));*/
                    }
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
