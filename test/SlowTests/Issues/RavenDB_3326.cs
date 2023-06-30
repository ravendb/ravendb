// -----------------------------------------------------------------------
//  <copyright file="QueryResultsStreaming.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Server.Config;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_3326 : RavenTestBase
    {
        public RavenDB_3326(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Querying)]
        [RavenData(DatabaseMode = RavenDatabaseMode.Single, SearchEngineMode = RavenSearchEngineMode.All)]
        public void streaming_and_projections_with_property_rename(Options options)
        {
            IncludeDistancesAndScore(options);

            using (var store = GetDocumentStore(options))
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
                    var query = session.Query<Customer>(index.IndexName)
                        .OrderByScore()
                    .Select(r => new
                    {
                        Name = r.Name,
                        OtherThanName = r.Address,
                    });

                    var enumerator = session.Advanced.Stream(query);
                    while (enumerator.MoveNext())
                    {
                        Assert.Equal("John", enumerator.Current.Document.Name);
                        Assert.Equal("Tel Aviv", enumerator.Current.Document.OtherThanName);

                        Assert.NotNull(enumerator.Current.Id);
                        Assert.NotNull(enumerator.Current.Metadata[Constants.Documents.Metadata.IndexScore]);
                    }
                }
            }
        }

        [RavenTheory(RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.Single)]
         public async Task streaming_and_projections_with_property_rename_Async(Options options)
        {
            IncludeDistancesAndScore(options);
            using (var store = GetDocumentStore(options))
            {
                var index = new Customers_ByName();
                await index.ExecuteAsync(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new Customer { Name = "John", Address = "Tel Aviv" });
                    session.SaveChanges();
                }

                await Indexes.WaitForIndexingAsync(store);

                using (var session = store.OpenAsyncSession())
                {

                    var query = options.SearchEngineMode is RavenSearchEngineMode.Lucene
                        ? session.Query<Customer>(index.IndexName).Select(r => new {Name = r.Name, OtherThanName = r.Address,})
                        : session.Query<Customer>(index.IndexName).OrderByScore().Select(r => new {Name = r.Name, OtherThanName = r.Address,});
                        

                    await using var enumerator = await session.Advanced.StreamAsync(query);

                    while (await enumerator.MoveNextAsync())
                    {
                        Assert.Equal("John", enumerator.Current.Document.Name);
                        Assert.Equal("Tel Aviv", enumerator.Current.Document.OtherThanName);

                        Assert.NotNull(enumerator.Current.Id);
                        Assert.NotNull(enumerator.Current.Metadata[Constants.Documents.Metadata.IndexScore]);
                    }
                }
            }
        }

        public class Customer
        {
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
        
        private void IncludeDistancesAndScore(Options options)
        {
            if (options.SearchEngineMode is RavenSearchEngineMode.Corax)
            {
                options.ModifyDatabaseRecord += record =>
                {
                    record.Settings[RavenConfiguration.GetKey(x => x.Indexing.CoraxIncludeSpatialDistance)] = true.ToString();
                    record.Settings[RavenConfiguration.GetKey(x => x.Indexing.CoraxIncludeDocumentScore)] = true.ToString();
                };
            }
        }
    }
}
