using System;
using System.Linq;
using FastTests;
using Orders;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Documents.Session;
using Tests.Infrastructure.Operations;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_13644 : RavenTestBase
    {
        public RavenDB_13644(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void CanLoadCompareExchangeInIndexes()
        {
            using (var store = GetDocumentStore())
            {
                var index = new Index_With_CompareExchange();
                var indexName = index.IndexName;
                index.Execute(store);

                WaitForIndexing(store);

                RavenTestHelper.AssertNoIndexErrors(store);

                store.Maintenance.Send(new StopIndexingOperation());

                var staleness = store.Maintenance.Send(new GetIndexStalenessOperation(indexName));
                Assert.False(staleness.IsStale);

                var terms = store.Maintenance.Send(new GetTermsOperation(indexName, "City", null));
                Assert.Equal(0, terms.Length);

                // add doc
                using (var session = store.OpenSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                {
                    session.Store(new Company { Name = "CF", ExternalId = "companies/cf" });

                    session.SaveChanges();
                }

                staleness = store.Maintenance.Send(new GetIndexStalenessOperation(indexName));
                Assert.True(staleness.IsStale);
                Assert.Equal(1, staleness.StalenessReasons.Count);
                Assert.Contains("There are still some documents to process from collection", staleness.StalenessReasons[0]);

                store.Maintenance.Send(new StartIndexingOperation());

                WaitForIndexing(store);

                RavenTestHelper.AssertNoIndexErrors(store);

                staleness = store.Maintenance.Send(new GetIndexStalenessOperation(indexName));
                Assert.False(staleness.IsStale);

                terms = store.Maintenance.Send(new GetTermsOperation(indexName, "City", null));
                Assert.Equal(0, terms.Length);

                store.Maintenance.Send(new StopIndexingOperation());

                // add compare
                using (var session = store.OpenSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                {
                    session.Advanced.ClusterTransaction.CreateCompareExchangeValue("companies/cf", new Address { City = "Torun" });

                    session.SaveChanges();
                }

                staleness = store.Maintenance.Send(new GetIndexStalenessOperation(indexName));
                Assert.True(staleness.IsStale);
                Assert.Equal(1, staleness.StalenessReasons.Count);
                Assert.Contains("There are still some compare exchange references to process for collection", staleness.StalenessReasons[0]);

                store.Maintenance.Send(new StartIndexingOperation());

                WaitForIndexing(store);

                RavenTestHelper.AssertNoIndexErrors(store);

                staleness = store.Maintenance.Send(new GetIndexStalenessOperation(indexName));
                Assert.False(staleness.IsStale);

                terms = store.Maintenance.Send(new GetTermsOperation(indexName, "City", null));
                Assert.Equal(1, terms.Length);
                Assert.Contains("torun", terms);

                store.Maintenance.Send(new StopIndexingOperation());

                // add doc and compare
                using (var session = store.OpenSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                {
                    session.Store(new Company { Name = "HR", ExternalId = "companies/hr" });
                    session.Advanced.ClusterTransaction.CreateCompareExchangeValue("companies/hr", new Address { City = "Cesarea" });

                    session.SaveChanges();
                }

                staleness = store.Maintenance.Send(new GetIndexStalenessOperation(indexName));
                Assert.True(staleness.IsStale);
                Assert.Equal(2, staleness.StalenessReasons.Count);
                Assert.Contains("There are still some documents to process from collection", staleness.StalenessReasons[0]);
                Assert.Contains("There are still some compare exchange references to process for collection", staleness.StalenessReasons[1]);

                store.Maintenance.Send(new StartIndexingOperation());

                WaitForIndexing(store);

                RavenTestHelper.AssertNoIndexErrors(store);

                staleness = store.Maintenance.Send(new GetIndexStalenessOperation(indexName));
                Assert.False(staleness.IsStale);

                terms = store.Maintenance.Send(new GetTermsOperation(indexName, "City", null));
                Assert.Equal(2, terms.Length);
                Assert.Contains("torun", terms);
                Assert.Contains("cesarea", terms);

                store.Maintenance.Send(new StopIndexingOperation());

                // update
                using (var session = store.OpenSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                {
                    var value = session.Advanced.ClusterTransaction.GetCompareExchangeValue<Address>("companies/hr");
                    value.Value.City = "Hadera";

                    session.Advanced.ClusterTransaction.UpdateCompareExchangeValue(value);

                    session.SaveChanges();
                }

                staleness = store.Maintenance.Send(new GetIndexStalenessOperation(indexName));
                Assert.True(staleness.IsStale);
                Assert.Equal(1, staleness.StalenessReasons.Count);
                Assert.Contains("There are still some compare exchange references to process for collection", staleness.StalenessReasons[0]);

                store.Maintenance.Send(new StartIndexingOperation());

                WaitForIndexing(store);

                RavenTestHelper.AssertNoIndexErrors(store);

                staleness = store.Maintenance.Send(new GetIndexStalenessOperation(indexName));
                Assert.False(staleness.IsStale);

                terms = store.Maintenance.Send(new GetTermsOperation(indexName, "City", null));
                Assert.Equal(2, terms.Length);
                Assert.Contains("torun", terms);
                Assert.Contains("hadera", terms);

                store.Maintenance.Send(new StopIndexingOperation());

                // delete
                using (var session = store.OpenSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                {
                    var value = session.Advanced.ClusterTransaction.GetCompareExchangeValue<Address>("companies/hr");
                    session.Advanced.ClusterTransaction.DeleteCompareExchangeValue(value);

                    session.SaveChanges();
                }

                staleness = store.Maintenance.Send(new GetIndexStalenessOperation(indexName));
                Assert.True(staleness.IsStale);
                Assert.Equal(1, staleness.StalenessReasons.Count);
                Assert.Contains("There are still some compare exchange tombstone references to process for collection", staleness.StalenessReasons[0]);

                store.Maintenance.Send(new StartIndexingOperation());

                WaitForIndexing(store);

                RavenTestHelper.AssertNoIndexErrors(store);

                staleness = store.Maintenance.Send(new GetIndexStalenessOperation(indexName));
                Assert.False(staleness.IsStale);

                terms = store.Maintenance.Send(new GetTermsOperation(indexName, "City", null));
                Assert.Equal(1, terms.Length);
                Assert.Contains("torun", terms);

                // live add compare without stopping indexing
                using (var session = store.OpenSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                {
                    session.Advanced.ClusterTransaction.CreateCompareExchangeValue("companies/hr", new Address { City = "Tel Aviv" });

                    session.SaveChanges();
                }

                WaitForIndexing(store, timeout: TimeSpan.FromSeconds(5));

                RavenTestHelper.AssertNoIndexErrors(store);

                staleness = store.Maintenance.Send(new GetIndexStalenessOperation(indexName));
                Assert.False(staleness.IsStale);

                terms = store.Maintenance.Send(new GetTermsOperation(indexName, "City", null));
                Assert.Equal(2, terms.Length);
                Assert.Contains("torun", terms);
                Assert.Contains("tel aviv", terms);
            }
        }

        [Fact]
        public void CanLoadCompareExchangeInIndexes_Query()
        {
            using (var store = GetDocumentStore())
            {
                var index = new Index_With_CompareExchange();
                var indexName = index.IndexName;
                index.Execute(store);

                WaitForIndexing(store);

                RavenTestHelper.AssertNoIndexErrors(store);

                store.Maintenance.Send(new StopIndexingOperation());

                long? previousResultEtag = 0L;
                using (var session = store.OpenSession())
                {
                    var terms = session.Query<Index_With_CompareExchange.Result, Index_With_CompareExchange>()
                        .Statistics(out var statistics)
                        .Select(x => x.City)
                        .ToArray();

                    Assert.False(statistics.IsStale);
                    Assert.True(statistics.DurationInMs >= 0); // not from cache
                    Assert.NotEqual(previousResultEtag, statistics.ResultEtag);
                    Assert.Equal(0, terms.Length);

                    previousResultEtag = statistics.ResultEtag;
                }

                // add doc
                using (var session = store.OpenSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                {
                    session.Store(new Company { Name = "CF", ExternalId = "companies/cf" });

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var terms = session.Query<Index_With_CompareExchange.Result, Index_With_CompareExchange>()
                        .Statistics(out var statistics)
                        .Select(x => x.City)
                        .ToArray();

                    Assert.True(statistics.IsStale);
                    Assert.True(statistics.DurationInMs >= 0); // not from cache
                    Assert.NotEqual(previousResultEtag, statistics.ResultEtag);
                    Assert.Equal(0, terms.Length);

                    previousResultEtag = statistics.ResultEtag;
                }

                store.Maintenance.Send(new StartIndexingOperation());

                WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var terms = session.Query<Index_With_CompareExchange.Result, Index_With_CompareExchange>()
                        .Statistics(out var statistics)
                        .Select(x => x.City)
                        .ToArray();

                    Assert.False(statistics.IsStale);
                    Assert.True(statistics.DurationInMs >= 0); // not from cache
                    Assert.NotEqual(previousResultEtag, statistics.ResultEtag);
                    Assert.Equal(0, terms.Length);

                    previousResultEtag = statistics.ResultEtag;
                }

                store.Maintenance.Send(new StopIndexingOperation());

                // add compare
                using (var session = store.OpenSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                {
                    session.Advanced.ClusterTransaction.CreateCompareExchangeValue("companies/cf", new Address { City = "Torun" });

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var terms = session.Query<Index_With_CompareExchange.Result, Index_With_CompareExchange>()
                        .Statistics(out var statistics)
                        .Select(x => x.City)
                        .ToArray();

                    Assert.True(statistics.IsStale);
                    Assert.True(statistics.DurationInMs >= 0); // not from cache
                    Assert.NotEqual(previousResultEtag, statistics.ResultEtag);
                    Assert.Equal(0, terms.Length);

                    previousResultEtag = statistics.ResultEtag;
                }

                store.Maintenance.Send(new StartIndexingOperation());

                WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var terms = session.Query<Index_With_CompareExchange.Result, Index_With_CompareExchange>()
                        .Statistics(out var statistics)
                        .Select(x => x.City)
                        .ToArray();

                    Assert.False(statistics.IsStale);
                    Assert.True(statistics.DurationInMs >= 0); // not from cache
                    Assert.NotEqual(previousResultEtag, statistics.ResultEtag);
                    Assert.Equal(1, terms.Length);
                    Assert.Contains("Torun", terms);

                    previousResultEtag = statistics.ResultEtag;
                }

                store.Maintenance.Send(new StopIndexingOperation());

                // add doc and compare
                using (var session = store.OpenSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                {
                    session.Store(new Company { Name = "HR", ExternalId = "companies/hr" });
                    session.Advanced.ClusterTransaction.CreateCompareExchangeValue("companies/hr", new Address { City = "Cesarea" });

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var terms = session.Query<Index_With_CompareExchange.Result, Index_With_CompareExchange>()
                        .Statistics(out var statistics)
                        .Select(x => x.City)
                        .ToArray();

                    Assert.True(statistics.IsStale);
                    Assert.True(statistics.DurationInMs >= 0); // not from cache
                    Assert.NotEqual(previousResultEtag, statistics.ResultEtag);
                    Assert.Equal(1, terms.Length);
                    Assert.Contains("Torun", terms);

                    previousResultEtag = statistics.ResultEtag;
                }

                store.Maintenance.Send(new StartIndexingOperation());

                WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var terms = session.Query<Index_With_CompareExchange.Result, Index_With_CompareExchange>()
                        .Statistics(out var statistics)
                        .Select(x => x.City)
                        .ToArray();

                    Assert.False(statistics.IsStale);
                    Assert.True(statistics.DurationInMs >= 0); // not from cache
                    Assert.NotEqual(previousResultEtag, statistics.ResultEtag);
                    Assert.Equal(2, terms.Length);
                    Assert.Contains("Torun", terms);
                    Assert.Contains("Cesarea", terms);

                    previousResultEtag = statistics.ResultEtag;
                }

                store.Maintenance.Send(new StopIndexingOperation());

                // update
                using (var session = store.OpenSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                {
                    var value = session.Advanced.ClusterTransaction.GetCompareExchangeValue<Address>("companies/hr");
                    value.Value.City = "Hadera";

                    session.Advanced.ClusterTransaction.UpdateCompareExchangeValue(value);

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var terms = session.Query<Index_With_CompareExchange.Result, Index_With_CompareExchange>()
                        .Statistics(out var statistics)
                        .Select(x => x.City)
                        .ToArray();

                    Assert.True(statistics.IsStale);
                    Assert.True(statistics.DurationInMs >= 0); // not from cache
                    Assert.NotEqual(previousResultEtag, statistics.ResultEtag);
                    Assert.Equal(2, terms.Length);
                    Assert.Contains("Torun", terms);
                    Assert.Contains("Cesarea", terms);

                    previousResultEtag = statistics.ResultEtag;
                }

                store.Maintenance.Send(new StartIndexingOperation());

                WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var terms = session.Query<Index_With_CompareExchange.Result, Index_With_CompareExchange>()
                        .Statistics(out var statistics)
                        .Select(x => x.City)
                        .ToArray();

                    Assert.False(statistics.IsStale);
                    Assert.True(statistics.DurationInMs >= 0); // not from cache
                    Assert.NotEqual(previousResultEtag, statistics.ResultEtag);
                    Assert.Equal(2, terms.Length);
                    Assert.Contains("Torun", terms);
                    Assert.Contains("Hadera", terms);

                    previousResultEtag = statistics.ResultEtag;
                }

                store.Maintenance.Send(new StopIndexingOperation());

                // delete
                using (var session = store.OpenSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                {
                    var value = session.Advanced.ClusterTransaction.GetCompareExchangeValue<Address>("companies/hr");
                    session.Advanced.ClusterTransaction.DeleteCompareExchangeValue(value);

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var terms = session.Query<Index_With_CompareExchange.Result, Index_With_CompareExchange>()
                        .Statistics(out var statistics)
                        .Select(x => x.City)
                        .ToArray();

                    Assert.True(statistics.IsStale);
                    Assert.True(statistics.DurationInMs >= 0); // not from cache
                    Assert.NotEqual(previousResultEtag, statistics.ResultEtag);
                    Assert.Equal(2, terms.Length);
                    Assert.Contains("Torun", terms);
                    Assert.Contains("Hadera", terms);

                    previousResultEtag = statistics.ResultEtag;
                }

                store.Maintenance.Send(new StartIndexingOperation());

                WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var terms = session.Query<Index_With_CompareExchange.Result, Index_With_CompareExchange>()
                        .Statistics(out var statistics)
                        .Select(x => x.City)
                        .ToArray();

                    Assert.False(statistics.IsStale);
                    Assert.True(statistics.DurationInMs >= 0); // not from cache
                    Assert.NotEqual(previousResultEtag, statistics.ResultEtag);
                    Assert.Equal(1, terms.Length);
                    Assert.Contains("Torun", terms);

                    previousResultEtag = statistics.ResultEtag;
                }

                // live add compare without stopping indexing
                using (var session = store.OpenSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                {
                    session.Advanced.ClusterTransaction.CreateCompareExchangeValue("companies/hr", new Address { City = "Tel Aviv" });

                    session.SaveChanges();
                }

                WaitForIndexing(store, timeout: TimeSpan.FromSeconds(5));

                using (var session = store.OpenSession())
                {
                    var terms = session.Query<Index_With_CompareExchange.Result, Index_With_CompareExchange>()
                        .Statistics(out var statistics)
                        .Select(x => x.City)
                        .ToArray();

                    Assert.False(statistics.IsStale);
                    Assert.True(statistics.DurationInMs >= 0); // not from cache
                    Assert.NotEqual(previousResultEtag, statistics.ResultEtag);
                    Assert.Equal(2, terms.Length);
                    Assert.Contains("Torun", terms);
                    Assert.Contains("Tel Aviv", terms);

                    previousResultEtag = statistics.ResultEtag;
                }
            }
        }

        private class Index_With_CompareExchange : AbstractIndexCreationTask<Company>
        {
            public class Result
            {
                public string City { get; set; }
            }

            public Index_With_CompareExchange()
            {
                Map = companies => from c in companies
                                   let address = LoadCompareExchangeValue<Address>(c.ExternalId)
                                   select new
                                   {
                                       address.City
                                   };

                StoreAllFields(FieldStorage.Yes);
            }
        }
    }
}
