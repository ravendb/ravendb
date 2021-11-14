using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using FastTests.Server.JavaScript;
using Orders;
using Raven.Client;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Indexes.Counters;
using Raven.Client.Documents.Indexes.TimeSeries;
using Raven.Client.Documents.Operations.CompareExchange;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Documents.Session;
using Raven.Server.ServerWide.Context;
using Tests.Infrastructure;
using Tests.Infrastructure.Operations;
using Xunit;
using Xunit.Abstractions;
using IndexingFields = Raven.Client.Constants.Documents.Indexing.Fields;

namespace SlowTests.Issues
{
    public class RavenDB_13644 : RavenTestBase
    {
        public RavenDB_13644(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void CanLoadCompareExchangeInIndexes(Options options)
        {
            CanLoadCompareExchangeInIndexes<Index_With_CompareExchange>(options);
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void CanLoadCompareExchangeInIndexes_JavaScript(Options options)
        {
            switch (jsEngineType)
            {
                case "Jint":
                    CanLoadCompareExchangeInIndexes<Index_With_CompareExchange_JsJint>(jsEngineType);
                    break;
                case "V8":
                    CanLoadCompareExchangeInIndexes<Index_With_CompareExchange_JsV8>(jsEngineType);
                    break;
                default:
                    throw new NotSupportedException($"Not supported JS engine kind '{jsEngineType}'.");
            }
        }

        private void CanLoadCompareExchangeInIndexes<TIndex>(string jsEngineType = null)
            where TIndex : AbstractIndexCreationTask, new()
        {
            using (var store = jsEngineType == null ? GetDocumentStore() : GetDocumentStore(Options.ForJavaScriptEngine(jsEngineType)))
            {
                var termsCountNull = jsEngineType is null or "Jint" ? 0 : 1;

                var index = new TIndex();
                var indexName = index.IndexName;
                index.Execute(store);

                Indexes.WaitForIndexing(store);

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

                Indexes.WaitForIndexing(store);

                RavenTestHelper.AssertNoIndexErrors(store);

                staleness = store.Maintenance.Send(new GetIndexStalenessOperation(indexName));
                Assert.False(staleness.IsStale);

                terms = store.Maintenance.Send(new GetTermsOperation(indexName, "City", null));
                Assert.Equal(termsCountNull, terms.Length);
                if (termsCountNull > 0)
                    Assert.Contains(IndexingFields.NullValue, terms);

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

                Indexes.WaitForIndexing(store);

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

                Indexes.WaitForIndexing(store);

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

                    session.SaveChanges();
                }

                staleness = store.Maintenance.Send(new GetIndexStalenessOperation(indexName));
                Assert.True(staleness.IsStale);
                Assert.Equal(1, staleness.StalenessReasons.Count);
                Assert.Contains("There are still some compare exchange references to process for collection", staleness.StalenessReasons[0]);

                store.Maintenance.Send(new StartIndexingOperation());

                Indexes.WaitForIndexing(store);

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

                Indexes.WaitForIndexing(store);

                RavenTestHelper.AssertNoIndexErrors(store);

                staleness = store.Maintenance.Send(new GetIndexStalenessOperation(indexName));
                Assert.False(staleness.IsStale);

                terms = store.Maintenance.Send(new GetTermsOperation(indexName, "City", null));
                Assert.Equal(1 + termsCountNull, terms.Length);
                if (termsCountNull > 0)
                    Assert.Contains(IndexingFields.NullValue, terms);
                Assert.Contains("torun", terms);

                // live add compare without stopping indexing
                using (var session = store.OpenSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                {
                    session.Advanced.ClusterTransaction.CreateCompareExchangeValue("companies/hr", new Address { City = "Tel Aviv" });

                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store, timeout: TimeSpan.FromSeconds(5));

                RavenTestHelper.AssertNoIndexErrors(store);

                staleness = store.Maintenance.Send(new GetIndexStalenessOperation(indexName));
                Assert.False(staleness.IsStale);

                terms = store.Maintenance.Send(new GetTermsOperation(indexName, "City", null));
                Assert.Equal(2, terms.Length);
                Assert.Contains("torun", terms);
                Assert.Contains("tel aviv", terms);
            }
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void CanLoadCompareExchangeInIndexes_Simple(Options options)
        {
            CanLoadCompareExchangeInIndexes_Simple<Index_With_CompareExchange_Simple>(options);
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void CanLoadCompareExchangeInIndexes_Simple_JavaScript(Options options)
        {
            switch (jsEngineType)
            {
                case "Jint":
                    CanLoadCompareExchangeInIndexes_Simple<Index_With_CompareExchange_Simple_JsJint>(jsEngineType);
                    break;
                case "V8":
                    CanLoadCompareExchangeInIndexes_Simple<Index_With_CompareExchange_Simple_JsV8>(jsEngineType);
                    break;
                default:
                    throw new NotSupportedException($"Not supported JS engine kind '{jsEngineType}'.");
            }
        }

        private void CanLoadCompareExchangeInIndexes_Simple<TIndex>(string jsEngineType = null)
            where TIndex : AbstractIndexCreationTask, new()
        {
            using (var store = jsEngineType == null ? GetDocumentStore() : GetDocumentStore(Options.ForJavaScriptEngine(jsEngineType)))
            {
                var termsCountNull = jsEngineType is null or "Jint" ? 0 : 1;

                var index = new TIndex();
                var indexName = index.IndexName;
                index.Execute(store);

                Indexes.WaitForIndexing(store);

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

                Indexes.WaitForIndexing(store);

                RavenTestHelper.AssertNoIndexErrors(store);

                staleness = store.Maintenance.Send(new GetIndexStalenessOperation(indexName));
                Assert.False(staleness.IsStale);

                terms = store.Maintenance.Send(new GetTermsOperation(indexName, "City", null));
                Assert.Equal(termsCountNull, terms.Length);
                if (termsCountNull > 0)
                    Assert.Contains(IndexingFields.NullValue, terms);

                store.Maintenance.Send(new StopIndexingOperation());

                // add compare
                using (var session = store.OpenSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                {
                    session.Advanced.ClusterTransaction.CreateCompareExchangeValue("companies/cf", "Torun");

                    session.SaveChanges();
                }

                staleness = store.Maintenance.Send(new GetIndexStalenessOperation(indexName));
                Assert.True(staleness.IsStale);
                Assert.Equal(1, staleness.StalenessReasons.Count);
                Assert.Contains("There are still some compare exchange references to process for collection", staleness.StalenessReasons[0]);

                store.Maintenance.Send(new StartIndexingOperation());

                Indexes.WaitForIndexing(store);

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
                    session.Advanced.ClusterTransaction.CreateCompareExchangeValue("companies/hr", "Cesarea");

                    session.SaveChanges();
                }

                staleness = store.Maintenance.Send(new GetIndexStalenessOperation(indexName));
                Assert.True(staleness.IsStale);
                Assert.Equal(2, staleness.StalenessReasons.Count);
                Assert.Contains("There are still some documents to process from collection", staleness.StalenessReasons[0]);
                Assert.Contains("There are still some compare exchange references to process for collection", staleness.StalenessReasons[1]);

                store.Maintenance.Send(new StartIndexingOperation());

                Indexes.WaitForIndexing(store);

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
                    var value = session.Advanced.ClusterTransaction.GetCompareExchangeValue<string>("companies/hr");
                    value.Value = "Hadera";

                    session.SaveChanges();
                }

                staleness = store.Maintenance.Send(new GetIndexStalenessOperation(indexName));
                Assert.True(staleness.IsStale);
                Assert.Equal(1, staleness.StalenessReasons.Count);
                Assert.Contains("There are still some compare exchange references to process for collection", staleness.StalenessReasons[0]);

                store.Maintenance.Send(new StartIndexingOperation());

                Indexes.WaitForIndexing(store);

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
                    var value = session.Advanced.ClusterTransaction.GetCompareExchangeValue<string>("companies/hr");
                    session.Advanced.ClusterTransaction.DeleteCompareExchangeValue(value);

                    session.SaveChanges();
                }

                staleness = store.Maintenance.Send(new GetIndexStalenessOperation(indexName));
                Assert.True(staleness.IsStale);
                Assert.Equal(1, staleness.StalenessReasons.Count);
                Assert.Contains("There are still some compare exchange tombstone references to process for collection", staleness.StalenessReasons[0]);

                store.Maintenance.Send(new StartIndexingOperation());

                Indexes.WaitForIndexing(store);

                RavenTestHelper.AssertNoIndexErrors(store);

                staleness = store.Maintenance.Send(new GetIndexStalenessOperation(indexName));
                Assert.False(staleness.IsStale);

                terms = store.Maintenance.Send(new GetTermsOperation(indexName, "City", null));
                Assert.Equal(1 + termsCountNull, terms.Length);
                if (termsCountNull > 0)
                    Assert.Contains(IndexingFields.NullValue, terms);
                Assert.Contains("torun", terms);

                // live add compare without stopping indexing
                using (var session = store.OpenSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                {
                    session.Advanced.ClusterTransaction.CreateCompareExchangeValue("companies/hr", "Tel Aviv");

                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store, timeout: TimeSpan.FromSeconds(5));

                RavenTestHelper.AssertNoIndexErrors(store);

                staleness = store.Maintenance.Send(new GetIndexStalenessOperation(indexName));
                Assert.False(staleness.IsStale);

                terms = store.Maintenance.Send(new GetTermsOperation(indexName, "City", null));
                Assert.Equal(2, terms.Length);
                Assert.Contains("torun", terms);
                Assert.Contains("tel aviv", terms);
            }
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void CanLoadCompareExchangeInIndexes_Query(Options options)
        {
            CanLoadCompareExchangeInIndexes_Query<Index_With_CompareExchange>(options);
        }

        [Theory]
        [JavaScriptEngineClassData]
        public void anLoadCompareExchangeInIndexes_Query_JavaScript(string jsEngineType)
        {
            switch (jsEngineType)
            {
                case "Jint":
                    CanLoadCompareExchangeInIndexes_Query<Index_With_CompareExchange_JsJint>(jsEngineType);
                    break;
                case "V8":
                    CanLoadCompareExchangeInIndexes_Query<Index_With_CompareExchange_JsV8>(jsEngineType);
                    break;
                default:
                    throw new NotSupportedException($"Not supported JS engine kind '{jsEngineType}'.");
            }
        }

        private void CanLoadCompareExchangeInIndexes_Query<TIndex>(Options options)
            where TIndex : AbstractIndexCreationTask, new()
        {
            using (var store = GetDocumentStore(options))
            {
                var termsCountNull = jsEngineType is null or "Jint" ? 0 : 1;

                var index = new TIndex();
                var indexName = index.IndexName;
                index.Execute(store);

                Indexes.WaitForIndexing(store);

                RavenTestHelper.AssertNoIndexErrors(store);

                store.Maintenance.Send(new StopIndexingOperation());

                long? previousResultEtag = 0L;
                using (var session = store.OpenSession())
                {
                    var terms = session.Query<MapIndexResult, TIndex>()
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
                    var terms = session.Query<MapIndexResult, TIndex>()
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

                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var terms = session.Query<MapIndexResult, TIndex>()
                        .Statistics(out var statistics)
                        .Select(x => x.City)
                        .ToArray();

                    Assert.False(statistics.IsStale);
                    Assert.True(statistics.DurationInMs >= 0); // not from cache
                    Assert.NotEqual(previousResultEtag, statistics.ResultEtag);
                    Assert.Equal(termsCountNull, terms.Length);
                    if (termsCountNull > 0)
                        Assert.Contains(null, terms);

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
                    var terms = session.Query<MapIndexResult, TIndex>()
                        .Statistics(out var statistics)
                        .Select(x => x.City)
                        .ToArray();

                    Assert.True(statistics.IsStale);
                    Assert.True(statistics.DurationInMs >= 0); // not from cache
                    Assert.NotEqual(previousResultEtag, statistics.ResultEtag);
                    Assert.Equal(termsCountNull, terms.Length);
                    if (termsCountNull > 0)
                        Assert.Contains(null, terms);

                    previousResultEtag = statistics.ResultEtag;
                }

                store.Maintenance.Send(new StartIndexingOperation());

                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var terms = session.Query<MapIndexResult, TIndex>()
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
                    var terms = session.Query<MapIndexResult, TIndex>()
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

                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var terms = session.Query<MapIndexResult, TIndex>()
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

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var terms = session.Query<MapIndexResult, TIndex>()
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

                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var terms = session.Query<MapIndexResult, TIndex>()
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
                    var terms = session.Query<MapIndexResult, TIndex>()
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

                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var terms = session.Query<MapIndexResult, TIndex>()
                        .Statistics(out var statistics)
                        .Select(x => x.City)
                        .ToArray();

                    Assert.False(statistics.IsStale);
                    Assert.True(statistics.DurationInMs >= 0); // not from cache
                    Assert.NotEqual(previousResultEtag, statistics.ResultEtag);
                    Assert.Equal(1 + termsCountNull, terms.Length);
                    if (termsCountNull > 0)
                        Assert.Contains(null, terms);
                    Assert.Contains("Torun", terms);

                    previousResultEtag = statistics.ResultEtag;
                }

                // live add compare without stopping indexing
                using (var session = store.OpenSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                {
                    session.Advanced.ClusterTransaction.CreateCompareExchangeValue("companies/hr", new Address { City = "Tel Aviv" });

                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store, timeout: TimeSpan.FromSeconds(5));

                using (var session = store.OpenSession())
                {
                    var terms = session.Query<MapIndexResult, TIndex>()
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

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.Lucene)]
        public async Task CanLoadCompareExchangeInIndexes_TimeSeries(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                var index = new TimeSeries_Index_With_CompareExchange();
                var indexName = index.IndexName;
                index.Execute(store);

                Indexes.WaitForIndexing(store);

                RavenTestHelper.AssertNoIndexErrors(store);

                store.Maintenance.Send(new StopIndexingOperation());

                var staleness = store.Maintenance.Send(new GetIndexStalenessOperation(indexName));
                Assert.False(staleness.IsStale);

                var terms = store.Maintenance.Send(new GetTermsOperation(indexName, "City", null));
                Assert.Equal(0, terms.Length);

                // add doc
                using (var session = store.OpenSession())
                {
                    var company = new Company { Name = "CF", ExternalId = "companies/cf" };
                    session.Store(company, "companies/1");
                    session.TimeSeriesFor(company, "Heartrate")
                        .Append(DateTime.Now, new double[] { 3 }, company.ExternalId);

                    session.SaveChanges();
                }

                staleness = store.Maintenance.Send(new GetIndexStalenessOperation(indexName));
                Assert.True(staleness.IsStale);
                Assert.Equal(1, staleness.StalenessReasons.Count);
                Assert.Contains("There are still some time series items to process from collection", staleness.StalenessReasons[0]);

                store.Maintenance.Send(new StartIndexingOperation());

                Indexes.WaitForIndexing(store);

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

                Indexes.WaitForIndexing(store);

                RavenTestHelper.AssertNoIndexErrors(store);

                staleness = store.Maintenance.Send(new GetIndexStalenessOperation(indexName));
                Assert.False(staleness.IsStale);

                terms = store.Maintenance.Send(new GetTermsOperation(indexName, "City", null));
                Assert.Equal(1, terms.Length);
                Assert.Contains("torun", terms);

                store.Maintenance.Send(new StopIndexingOperation());

                // add doc and compare
                using (var session = store.OpenSession())
                {
                    var company = new Company { Name = "HR", ExternalId = "companies/hr" };
                    session.Store(company);
                    session.TimeSeriesFor(company, "HeartRate")
                        .Append(DateTime.Now, new double[] { 5 }, company.ExternalId);

                    session.SaveChanges();
                }

                using (var session = store.OpenSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                {
                    session.Advanced.ClusterTransaction.CreateCompareExchangeValue("companies/hr", new Address { City = "Cesarea" });

                    session.SaveChanges();
                }

                staleness = store.Maintenance.Send(new GetIndexStalenessOperation(indexName));
                Assert.True(staleness.IsStale);
                Assert.Equal(2, staleness.StalenessReasons.Count);
                Assert.Contains("There are still some time series items to process from collection", staleness.StalenessReasons[0]);
                Assert.Contains("There are still some compare exchange references to process for collection", staleness.StalenessReasons[1]);

                store.Maintenance.Send(new StartIndexingOperation());

                Indexes.WaitForIndexing(store);

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

                    session.SaveChanges();
                }

                staleness = store.Maintenance.Send(new GetIndexStalenessOperation(indexName));
                Assert.True(staleness.IsStale);
                Assert.Equal(1, staleness.StalenessReasons.Count);
                Assert.Contains("There are still some compare exchange references to process for collection", staleness.StalenessReasons[0]);

                store.Maintenance.Send(new StartIndexingOperation());

                Indexes.WaitForIndexing(store);

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

                Indexes.WaitForIndexing(store);

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

                Indexes.WaitForIndexing(store, timeout: TimeSpan.FromSeconds(5));

                RavenTestHelper.AssertNoIndexErrors(store);

                staleness = store.Maintenance.Send(new GetIndexStalenessOperation(indexName));
                Assert.False(staleness.IsStale);

                terms = store.Maintenance.Send(new GetTermsOperation(indexName, "City", null));
                Assert.Equal(2, terms.Length);
                Assert.Contains("torun", terms);
                Assert.Contains("tel aviv", terms);

                var database = await Databases.GetDocumentDatabaseInstanceFor(store);
                var indexInstance = database.IndexStore.GetIndex(indexName);

                using (indexInstance._contextPool.AllocateOperationContext(out TransactionOperationContext context))
                using (var tx = context.OpenReadTransaction())
                {
                    var counts = indexInstance._indexStorage.ReferencesForCompareExchange.GetReferenceTablesCount("Companies", tx);

                    Assert.Equal(2, counts.ReferenceTableCount);
                    Assert.Equal(2, counts.CollectionTableCount);
                }

                using (var session = store.OpenSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                {
                    session.Delete("companies/1");

                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store, timeout: TimeSpan.FromSeconds(5));

                using (indexInstance._contextPool.AllocateOperationContext(out TransactionOperationContext context))
                using (var tx = context.OpenReadTransaction())
                {
                    var counts = indexInstance._indexStorage.ReferencesForCompareExchange.GetReferenceTablesCount("Companies", tx);

                    Assert.Equal(1, counts.ReferenceTableCount);
                    Assert.Equal(1, counts.CollectionTableCount);
                }
            }
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.Lucene)]
        public void CanLoadCompareExchangeInIndexes_TimeSeries_Query(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                var index = new TimeSeries_Index_With_CompareExchange();
                var indexName = index.IndexName;
                index.Execute(store);

                Indexes.WaitForIndexing(store);

                RavenTestHelper.AssertNoIndexErrors(store);

                store.Maintenance.Send(new StopIndexingOperation());

                long? previousResultEtag = 0L;
                using (var session = store.OpenSession())
                {
                    var terms = session.Query<MapIndexResult, TimeSeries_Index_With_CompareExchange>()
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
                using (var session = store.OpenSession())
                {
                    var company = new Company { Name = "CF", ExternalId = "companies/cf" };
                    session.Store(company);
                    session.TimeSeriesFor(company, "HeartRate")
                        .Append(DateTime.Now, new double[] { 3 }, company.ExternalId);

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var terms = session.Query<MapIndexResult, TimeSeries_Index_With_CompareExchange>()
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

                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var terms = session.Query<MapIndexResult, TimeSeries_Index_With_CompareExchange>()
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
                    var terms = session.Query<MapIndexResult, TimeSeries_Index_With_CompareExchange>()
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

                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var terms = session.Query<MapIndexResult, TimeSeries_Index_With_CompareExchange>()
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
                using (var session = store.OpenSession())
                {
                    var company = new Company { Name = "HR", ExternalId = "companies/hr" };
                    session.Store(company);
                    session.TimeSeriesFor(company, "HeartRate").Append(DateTime.Now, 5, company.ExternalId);

                    session.SaveChanges();
                }

                using (var session = store.OpenSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                {
                    session.Advanced.ClusterTransaction.CreateCompareExchangeValue("companies/hr", new Address { City = "Cesarea" });

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var terms = session.Query<MapIndexResult, TimeSeries_Index_With_CompareExchange>()
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

                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var terms = session.Query<MapIndexResult, TimeSeries_Index_With_CompareExchange>()
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

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var terms = session.Query<MapIndexResult, TimeSeries_Index_With_CompareExchange>()
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

                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var terms = session.Query<MapIndexResult, TimeSeries_Index_With_CompareExchange>()
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
                    var terms = session.Query<MapIndexResult, TimeSeries_Index_With_CompareExchange>()
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

                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var terms = session.Query<MapIndexResult, TimeSeries_Index_With_CompareExchange>()
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

                Indexes.WaitForIndexing(store, timeout: TimeSpan.FromSeconds(5));

                using (var session = store.OpenSession())
                {
                    var terms = session.Query<MapIndexResult, TimeSeries_Index_With_CompareExchange>()
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

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.Lucene)]
        public async Task CanLoadCompareExchangeInIndexes_Counters(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                var index = new Counters_Index_With_CompareExchange();
                var indexName = index.IndexName;
                index.Execute(store);

                Indexes.WaitForIndexing(store);

                RavenTestHelper.AssertNoIndexErrors(store);

                store.Maintenance.Send(new StopIndexingOperation());

                var staleness = store.Maintenance.Send(new GetIndexStalenessOperation(indexName));
                Assert.False(staleness.IsStale);

                var terms = store.Maintenance.Send(new GetTermsOperation(indexName, "City", null));
                Assert.Equal(0, terms.Length);

                // add doc
                using (var session = store.OpenSession())
                {
                    var company = new Company { Name = "CF", ExternalId = "companies/cf" };
                    session.Store(company, "companies/cf");
                    session.CountersFor(company).Increment("HeartRate", 3);

                    session.SaveChanges();
                }

                staleness = store.Maintenance.Send(new GetIndexStalenessOperation(indexName));
                Assert.True(staleness.IsStale);
                Assert.Equal(1, staleness.StalenessReasons.Count);
                Assert.Contains("There are still some counters to process from collection", staleness.StalenessReasons[0]);

                store.Maintenance.Send(new StartIndexingOperation());

                Indexes.WaitForIndexing(store);

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

                Indexes.WaitForIndexing(store);

                RavenTestHelper.AssertNoIndexErrors(store);

                staleness = store.Maintenance.Send(new GetIndexStalenessOperation(indexName));
                Assert.False(staleness.IsStale);

                terms = store.Maintenance.Send(new GetTermsOperation(indexName, "City", null));
                Assert.Equal(1, terms.Length);
                Assert.Contains("torun", terms);

                store.Maintenance.Send(new StopIndexingOperation());

                // add doc and compare
                using (var session = store.OpenSession())
                {
                    var company = new Company { Name = "HR", ExternalId = "companies/hr" };
                    session.Store(company, "companies/hr");
                    session.CountersFor(company).Increment("HeartRate", 5);

                    session.SaveChanges();
                }

                using (var session = store.OpenSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                {
                    session.Advanced.ClusterTransaction.CreateCompareExchangeValue("companies/hr", new Address { City = "Cesarea" });

                    session.SaveChanges();
                }

                staleness = store.Maintenance.Send(new GetIndexStalenessOperation(indexName));
                Assert.True(staleness.IsStale);
                Assert.Equal(2, staleness.StalenessReasons.Count);
                Assert.Contains("There are still some counters to process from collection", staleness.StalenessReasons[0]);
                Assert.Contains("There are still some compare exchange references to process for collection", staleness.StalenessReasons[1]);

                store.Maintenance.Send(new StartIndexingOperation());

                Indexes.WaitForIndexing(store);

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

                    session.SaveChanges();
                }

                staleness = store.Maintenance.Send(new GetIndexStalenessOperation(indexName));
                Assert.True(staleness.IsStale);
                Assert.Equal(1, staleness.StalenessReasons.Count);
                Assert.Contains("There are still some compare exchange references to process for collection", staleness.StalenessReasons[0]);

                store.Maintenance.Send(new StartIndexingOperation());

                Indexes.WaitForIndexing(store);

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

                Indexes.WaitForIndexing(store);

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

                Indexes.WaitForIndexing(store, timeout: TimeSpan.FromSeconds(5));

                RavenTestHelper.AssertNoIndexErrors(store);

                staleness = store.Maintenance.Send(new GetIndexStalenessOperation(indexName));
                Assert.False(staleness.IsStale);

                terms = store.Maintenance.Send(new GetTermsOperation(indexName, "City", null));
                Assert.Equal(2, terms.Length);
                Assert.Contains("torun", terms);
                Assert.Contains("tel aviv", terms);

                var database = await Databases.GetDocumentDatabaseInstanceFor(store);
                var indexInstance = database.IndexStore.GetIndex(indexName);

                using (indexInstance._contextPool.AllocateOperationContext(out TransactionOperationContext context))
                using (var tx = context.OpenReadTransaction())
                {
                    var counts = indexInstance._indexStorage.ReferencesForCompareExchange.GetReferenceTablesCount("Companies", tx);

                    Assert.Equal(2, counts.ReferenceTableCount);
                    Assert.Equal(2, counts.CollectionTableCount);
                }

                using (var session = store.OpenSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                {
                    session.Delete("companies/hr");

                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store, timeout: TimeSpan.FromSeconds(5));

                using (indexInstance._contextPool.AllocateOperationContext(out TransactionOperationContext context))
                using (var tx = context.OpenReadTransaction())
                {
                    var counts = indexInstance._indexStorage.ReferencesForCompareExchange.GetReferenceTablesCount("Companies", tx);

                    Assert.Equal(1, counts.ReferenceTableCount);
                    Assert.Equal(1, counts.CollectionTableCount);
                }
            }
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.Lucene)]
        public void CanLoadCompareExchangeInIndexes_Counters_Query(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                var index = new Counters_Index_With_CompareExchange();
                var indexName = index.IndexName;
                index.Execute(store);

                Indexes.WaitForIndexing(store);

                RavenTestHelper.AssertNoIndexErrors(store);

                store.Maintenance.Send(new StopIndexingOperation());

                long? previousResultEtag = 0L;
                using (var session = store.OpenSession())
                {
                    var terms = session.Query<MapIndexResult, Counters_Index_With_CompareExchange>()
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
                using (var session = store.OpenSession())
                {
                    var company = new Company { Name = "CF", ExternalId = "companies/cf" };
                    session.Store(company, "companies/cf");
                    session.CountersFor(company).Increment("HeartRate", 3);

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var terms = session.Query<MapIndexResult, Counters_Index_With_CompareExchange>()
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

                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var terms = session.Query<MapIndexResult, Counters_Index_With_CompareExchange>()
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
                    var terms = session.Query<MapIndexResult, Counters_Index_With_CompareExchange>()
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

                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var terms = session.Query<MapIndexResult, Counters_Index_With_CompareExchange>()
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
                using (var session = store.OpenSession())
                {
                    var company = new Company { Name = "HR", ExternalId = "companies/hr" };
                    session.Store(company, "companies/hr");
                    session.CountersFor(company).Increment("HeartRate", 5);

                    session.SaveChanges();
                }

                using (var session = store.OpenSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                {
                    session.Advanced.ClusterTransaction.CreateCompareExchangeValue("companies/hr", new Address { City = "Cesarea" });

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var terms = session.Query<MapIndexResult, Counters_Index_With_CompareExchange>()
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

                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var terms = session.Query<MapIndexResult, Counters_Index_With_CompareExchange>()
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

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var terms = session.Query<MapIndexResult, Counters_Index_With_CompareExchange>()
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

                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var terms = session.Query<MapIndexResult, Counters_Index_With_CompareExchange>()
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
                    var terms = session.Query<MapIndexResult, Counters_Index_With_CompareExchange>()
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

                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var terms = session.Query<MapIndexResult, Counters_Index_With_CompareExchange>()
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

                Indexes.WaitForIndexing(store, timeout: TimeSpan.FromSeconds(5));

                using (var session = store.OpenSession())
                {
                    var terms = session.Query<MapIndexResult, Counters_Index_With_CompareExchange>()
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

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void CanLoadCompareExchangeInIndexes_MapReduce_Query(Options options)
        {
            CanLoadCompareExchangeInIndexes_MapReduce_Query<Index_With_CompareExchange_MapReduce>(options);
        }

        private void CanLoadCompareExchangeInIndexes_MapReduce_Query<TIndex>(Options options)
            where TIndex : AbstractIndexCreationTask, new()
        {
            using (var store = GetDocumentStore(options))
            {
                var index = new TIndex();
                var indexName = index.IndexName;
                index.Execute(store);

                Indexes.WaitForIndexing(store);

                RavenTestHelper.AssertNoIndexErrors(store);

                store.Maintenance.Send(new StopIndexingOperation());

                long? previousResultEtag = 0L;
                using (var session = store.OpenSession())
                {
                    var terms = session.Query<MapReduceIndexResult, TIndex>()
                        .Statistics(out var statistics)
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
                    var terms = session.Query<MapReduceIndexResult, TIndex>()
                        .Statistics(out var statistics)
                        .ToArray();

                    Assert.True(statistics.IsStale);
                    Assert.True(statistics.DurationInMs >= 0); // not from cache
                    Assert.NotEqual(previousResultEtag, statistics.ResultEtag);
                    Assert.Equal(0, terms.Length);

                    previousResultEtag = statistics.ResultEtag;
                }

                store.Maintenance.Send(new StartIndexingOperation());

                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var terms = session.Query<MapReduceIndexResult, TIndex>()
                        .Statistics(out var statistics)
                        .ToArray();

                    Assert.False(statistics.IsStale);
                    Assert.True(statistics.DurationInMs >= 0); // not from cache
                    Assert.NotEqual(previousResultEtag, statistics.ResultEtag);
                    Assert.Equal(1, terms.Length);
                    Assert.Contains(null, terms.Select(x => x.City));

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
                    var terms = session.Query<MapReduceIndexResult, TIndex>()
                        .Statistics(out var statistics)
                        .ToArray();

                    Assert.True(statistics.IsStale);
                    Assert.True(statistics.DurationInMs >= 0); // not from cache
                    Assert.NotEqual(previousResultEtag, statistics.ResultEtag);
                    Assert.Equal(1, terms.Length);
                    Assert.Contains(null, terms.Select(x => x.City));

                    previousResultEtag = statistics.ResultEtag;
                }

                store.Maintenance.Send(new StartIndexingOperation());

                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var terms = session.Query<MapReduceIndexResult, TIndex>()
                        .Statistics(out var statistics)
                        .ToArray();

                    Assert.False(statistics.IsStale);
                    Assert.True(statistics.DurationInMs >= 0); // not from cache
                    Assert.NotEqual(previousResultEtag, statistics.ResultEtag);
                    Assert.Equal(1, terms.Length);
                    Assert.Contains("Torun", terms.Select(x => x.City));

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
                    var terms = session.Query<MapReduceIndexResult, TIndex>()
                        .Statistics(out var statistics)
                        .ToArray();

                    Assert.True(statistics.IsStale);
                    Assert.True(statistics.DurationInMs >= 0); // not from cache
                    Assert.NotEqual(previousResultEtag, statistics.ResultEtag);
                    Assert.Equal(1, terms.Length);
                    Assert.Contains("Torun", terms.Select(x => x.City));

                    previousResultEtag = statistics.ResultEtag;
                }

                store.Maintenance.Send(new StartIndexingOperation());

                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var terms = session.Query<MapReduceIndexResult, TIndex>()
                        .Statistics(out var statistics)
                        .ToArray();

                    Assert.False(statistics.IsStale);
                    Assert.True(statistics.DurationInMs >= 0); // not from cache
                    Assert.NotEqual(previousResultEtag, statistics.ResultEtag);
                    Assert.Equal(2, terms.Length);
                    Assert.Contains("Torun", terms.Select(x => x.City));
                    Assert.Contains("Cesarea", terms.Select(x => x.City));

                    previousResultEtag = statistics.ResultEtag;
                }

                store.Maintenance.Send(new StopIndexingOperation());

                // update
                using (var session = store.OpenSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                {
                    var value = session.Advanced.ClusterTransaction.GetCompareExchangeValue<Address>("companies/hr");
                    value.Value.City = "Hadera";

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var terms = session.Query<MapReduceIndexResult, TIndex>()
                        .Statistics(out var statistics)
                        .ToArray();

                    Assert.True(statistics.IsStale);
                    Assert.True(statistics.DurationInMs >= 0); // not from cache
                    Assert.NotEqual(previousResultEtag, statistics.ResultEtag);
                    Assert.Equal(2, terms.Length);
                    Assert.Contains("Torun", terms.Select(x => x.City));
                    Assert.Contains("Cesarea", terms.Select(x => x.City));

                    previousResultEtag = statistics.ResultEtag;
                }

                store.Maintenance.Send(new StartIndexingOperation());

                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var terms = session.Query<MapReduceIndexResult, TIndex>()
                        .Statistics(out var statistics)
                        .ToArray();

                    Assert.False(statistics.IsStale);
                    Assert.True(statistics.DurationInMs >= 0); // not from cache
                    Assert.NotEqual(previousResultEtag, statistics.ResultEtag);
                    Assert.Equal(2, terms.Length);
                    Assert.Contains("Torun", terms.Select(x => x.City));
                    Assert.Contains("Hadera", terms.Select(x => x.City));

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
                    var terms = session.Query<MapReduceIndexResult, TIndex>()
                        .Statistics(out var statistics)
                        .ToArray();

                    Assert.True(statistics.IsStale);
                    Assert.True(statistics.DurationInMs >= 0); // not from cache
                    Assert.NotEqual(previousResultEtag, statistics.ResultEtag);
                    Assert.Equal(2, terms.Length);
                    Assert.Contains("Torun", terms.Select(x => x.City));
                    Assert.Contains("Hadera", terms.Select(x => x.City));

                    previousResultEtag = statistics.ResultEtag;
                }

                store.Maintenance.Send(new StartIndexingOperation());

                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var terms = session.Query<MapReduceIndexResult, TIndex>()
                        .Statistics(out var statistics)
                        .ToArray();

                    Assert.False(statistics.IsStale);
                    Assert.True(statistics.DurationInMs >= 0); // not from cache
                    Assert.NotEqual(previousResultEtag, statistics.ResultEtag);
                    Assert.Equal(2, terms.Length);
                    Assert.Contains("Torun", terms.Select(x => x.City));
                    Assert.Contains(null, terms.Select(x => x.City));

                    previousResultEtag = statistics.ResultEtag;
                }

                // live add compare without stopping indexing
                using (var session = store.OpenSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                {
                    session.Advanced.ClusterTransaction.CreateCompareExchangeValue("companies/hr", new Address { City = "Tel Aviv" });

                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store, timeout: TimeSpan.FromSeconds(5));

                using (var session = store.OpenSession())
                {
                    var terms = session.Query<MapReduceIndexResult, TIndex>()
                        .Statistics(out var statistics)
                        .ToArray();

                    Assert.False(statistics.IsStale);
                    Assert.True(statistics.DurationInMs >= 0); // not from cache
                    Assert.NotEqual(previousResultEtag, statistics.ResultEtag);
                    Assert.Equal(2, terms.Length);
                    Assert.Contains("Torun", terms.Select(x => x.City));
                    Assert.Contains("Tel Aviv", terms.Select(x => x.City));

                    previousResultEtag = statistics.ResultEtag;
                }
            }
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.Lucene)]
        public void CanLoadCompareExchangeInIndexes_MapReduce_Counters_Query(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                var index = new Counters_Index_With_CompareExchange_MapReduce();
                var indexName = index.IndexName;
                index.Execute(store);

                Indexes.WaitForIndexing(store);

                RavenTestHelper.AssertNoIndexErrors(store);

                store.Maintenance.Send(new StopIndexingOperation());

                long? previousResultEtag = 0L;
                using (var session = store.OpenSession())
                {
                    var terms = session.Query<MapReduceIndexResult, Counters_Index_With_CompareExchange_MapReduce>()
                        .Statistics(out var statistics)
                        .ToArray();

                    Assert.False(statistics.IsStale);
                    Assert.True(statistics.DurationInMs >= 0); // not from cache
                    Assert.NotEqual(previousResultEtag, statistics.ResultEtag);
                    Assert.Equal(0, terms.Length);

                    previousResultEtag = statistics.ResultEtag;
                }

                // add doc
                using (var session = store.OpenSession())
                {
                    var company = new Company { Name = "CF", ExternalId = "companies/cf" };
                    session.Store(company, "companies/cf");
                    session.CountersFor(company).Increment("HeartRate", 3);

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var terms = session.Query<MapReduceIndexResult, Counters_Index_With_CompareExchange_MapReduce>()
                        .Statistics(out var statistics)
                        .ToArray();

                    Assert.True(statistics.IsStale);
                    Assert.True(statistics.DurationInMs >= 0); // not from cache
                    Assert.NotEqual(previousResultEtag, statistics.ResultEtag);
                    Assert.Equal(0, terms.Length);

                    previousResultEtag = statistics.ResultEtag;
                }

                store.Maintenance.Send(new StartIndexingOperation());

                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var terms = session.Query<MapReduceIndexResult, Counters_Index_With_CompareExchange_MapReduce>()
                        .Statistics(out var statistics)
                        .ToArray();

                    Assert.False(statistics.IsStale);
                    Assert.True(statistics.DurationInMs >= 0); // not from cache
                    Assert.NotEqual(previousResultEtag, statistics.ResultEtag);
                    Assert.Equal(1, terms.Length);
                    Assert.Contains(null, terms.Select(x => x.City));

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
                    var terms = session.Query<MapReduceIndexResult, Counters_Index_With_CompareExchange_MapReduce>()
                        .Statistics(out var statistics)
                        .ToArray();

                    Assert.True(statistics.IsStale);
                    Assert.True(statistics.DurationInMs >= 0); // not from cache
                    Assert.NotEqual(previousResultEtag, statistics.ResultEtag);
                    Assert.Equal(1, terms.Length);
                    Assert.Contains(null, terms.Select(x => x.City));

                    previousResultEtag = statistics.ResultEtag;
                }

                store.Maintenance.Send(new StartIndexingOperation());

                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var terms = session.Query<MapReduceIndexResult, Counters_Index_With_CompareExchange_MapReduce>()
                        .Statistics(out var statistics)
                        .ToArray();

                    Assert.False(statistics.IsStale);
                    Assert.True(statistics.DurationInMs >= 0); // not from cache
                    Assert.NotEqual(previousResultEtag, statistics.ResultEtag);
                    Assert.Equal(1, terms.Length);
                    Assert.Contains("Torun", terms.Select(x => x.City));

                    previousResultEtag = statistics.ResultEtag;
                }

                store.Maintenance.Send(new StopIndexingOperation());

                // add doc and compare
                using (var session = store.OpenSession())
                {
                    var company = new Company { Name = "HR", ExternalId = "companies/hr" };
                    session.Store(company, "companies/hr");
                    session.CountersFor(company).Increment("HeartRate", 5);

                    session.SaveChanges();
                }

                using (var session = store.OpenSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                {
                    session.Advanced.ClusterTransaction.CreateCompareExchangeValue("companies/hr", new Address { City = "Cesarea" });

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var terms = session.Query<MapReduceIndexResult, Counters_Index_With_CompareExchange_MapReduce>()
                        .Statistics(out var statistics)
                        .ToArray();

                    Assert.True(statistics.IsStale);
                    Assert.True(statistics.DurationInMs >= 0); // not from cache
                    Assert.NotEqual(previousResultEtag, statistics.ResultEtag);
                    Assert.Equal(1, terms.Length);
                    Assert.Contains("Torun", terms.Select(x => x.City));

                    previousResultEtag = statistics.ResultEtag;
                }

                store.Maintenance.Send(new StartIndexingOperation());

                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var terms = session.Query<MapReduceIndexResult, Counters_Index_With_CompareExchange_MapReduce>()
                        .Statistics(out var statistics)
                        .ToArray();

                    Assert.False(statistics.IsStale);
                    Assert.True(statistics.DurationInMs >= 0); // not from cache
                    Assert.NotEqual(previousResultEtag, statistics.ResultEtag);
                    Assert.Equal(2, terms.Length);
                    Assert.Contains("Torun", terms.Select(x => x.City));
                    Assert.Contains("Cesarea", terms.Select(x => x.City));

                    previousResultEtag = statistics.ResultEtag;
                }

                store.Maintenance.Send(new StopIndexingOperation());

                // update
                using (var session = store.OpenSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                {
                    var value = session.Advanced.ClusterTransaction.GetCompareExchangeValue<Address>("companies/hr");
                    value.Value.City = "Hadera";

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var terms = session.Query<MapReduceIndexResult, Counters_Index_With_CompareExchange_MapReduce>()
                        .Statistics(out var statistics)
                        .ToArray();

                    Assert.True(statistics.IsStale);
                    Assert.True(statistics.DurationInMs >= 0); // not from cache
                    Assert.NotEqual(previousResultEtag, statistics.ResultEtag);
                    Assert.Equal(2, terms.Length);
                    Assert.Contains("Torun", terms.Select(x => x.City));
                    Assert.Contains("Cesarea", terms.Select(x => x.City));

                    previousResultEtag = statistics.ResultEtag;
                }

                store.Maintenance.Send(new StartIndexingOperation());

                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var terms = session.Query<MapReduceIndexResult, Counters_Index_With_CompareExchange_MapReduce>()
                        .Statistics(out var statistics)
                        .ToArray();

                    Assert.False(statistics.IsStale);
                    Assert.True(statistics.DurationInMs >= 0); // not from cache
                    Assert.NotEqual(previousResultEtag, statistics.ResultEtag);
                    Assert.Equal(2, terms.Length);
                    Assert.Contains("Torun", terms.Select(x => x.City));
                    Assert.Contains("Hadera", terms.Select(x => x.City));

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
                    var terms = session.Query<MapReduceIndexResult, Counters_Index_With_CompareExchange_MapReduce>()
                        .Statistics(out var statistics)
                        .ToArray();

                    Assert.True(statistics.IsStale);
                    Assert.True(statistics.DurationInMs >= 0); // not from cache
                    Assert.NotEqual(previousResultEtag, statistics.ResultEtag);
                    Assert.Equal(2, terms.Length);
                    Assert.Contains("Torun", terms.Select(x => x.City));
                    Assert.Contains("Hadera", terms.Select(x => x.City));

                    previousResultEtag = statistics.ResultEtag;
                }

                store.Maintenance.Send(new StartIndexingOperation());

                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var terms = session.Query<MapReduceIndexResult, Counters_Index_With_CompareExchange_MapReduce>()
                        .Statistics(out var statistics)
                        .ToArray();

                    Assert.False(statistics.IsStale);
                    Assert.True(statistics.DurationInMs >= 0); // not from cache
                    Assert.NotEqual(previousResultEtag, statistics.ResultEtag);
                    Assert.Equal(2, terms.Length);
                    Assert.Contains("Torun", terms.Select(x => x.City));
                    Assert.Contains(null, terms.Select(x => x.City));

                    previousResultEtag = statistics.ResultEtag;
                }

                // live add compare without stopping indexing
                using (var session = store.OpenSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                {
                    session.Advanced.ClusterTransaction.CreateCompareExchangeValue("companies/hr", new Address { City = "Tel Aviv" });

                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store, timeout: TimeSpan.FromSeconds(5));

                using (var session = store.OpenSession())
                {
                    var terms = session.Query<MapReduceIndexResult, Counters_Index_With_CompareExchange_MapReduce>()
                        .Statistics(out var statistics)
                        .ToArray();

                    Assert.False(statistics.IsStale);
                    Assert.True(statistics.DurationInMs >= 0); // not from cache
                    Assert.NotEqual(previousResultEtag, statistics.ResultEtag);
                    Assert.Equal(2, terms.Length);
                    Assert.Contains("Torun", terms.Select(x => x.City));
                    Assert.Contains("Tel Aviv", terms.Select(x => x.City));

                    previousResultEtag = statistics.ResultEtag;
                }
            }
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.Lucene)]
        public void CanLoadCompareExchangeInIndexes_MapReduce_TimeSeries_Query(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                var index = new TimeSeries_Index_With_CompareExchange_MapReduce();
                var indexName = index.IndexName;
                index.Execute(store);

                Indexes.WaitForIndexing(store);

                RavenTestHelper.AssertNoIndexErrors(store);

                store.Maintenance.Send(new StopIndexingOperation());

                long? previousResultEtag = 0L;
                using (var session = store.OpenSession())
                {
                    var terms = session.Query<MapReduceIndexResult, TimeSeries_Index_With_CompareExchange_MapReduce>()
                        .Statistics(out var statistics)
                        .ToArray();

                    Assert.False(statistics.IsStale);
                    Assert.True(statistics.DurationInMs >= 0); // not from cache
                    Assert.NotEqual(previousResultEtag, statistics.ResultEtag);
                    Assert.Equal(0, terms.Length);

                    previousResultEtag = statistics.ResultEtag;
                }

                // add doc
                using (var session = store.OpenSession())
                {
                    var company = new Company { Name = "CF", ExternalId = "companies/cf" };
                    session.Store(company);
                    session.TimeSeriesFor(company, "HeartRate").Append(DateTime.Now, new double[] { 3 }, company.ExternalId);

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var terms = session.Query<MapReduceIndexResult, TimeSeries_Index_With_CompareExchange_MapReduce>()
                        .Statistics(out var statistics)
                        .ToArray();

                    Assert.True(statistics.IsStale);
                    Assert.True(statistics.DurationInMs >= 0); // not from cache
                    Assert.NotEqual(previousResultEtag, statistics.ResultEtag);
                    Assert.Equal(0, terms.Length);

                    previousResultEtag = statistics.ResultEtag;
                }

                store.Maintenance.Send(new StartIndexingOperation());

                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var terms = session.Query<MapReduceIndexResult, TimeSeries_Index_With_CompareExchange_MapReduce>()
                        .Statistics(out var statistics)
                        .ToArray();

                    Assert.False(statistics.IsStale);
                    Assert.True(statistics.DurationInMs >= 0); // not from cache
                    Assert.NotEqual(previousResultEtag, statistics.ResultEtag);
                    Assert.Equal(1, terms.Length);
                    Assert.Contains(null, terms.Select(x => x.City));

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
                    var terms = session.Query<MapReduceIndexResult, TimeSeries_Index_With_CompareExchange_MapReduce>()
                        .Statistics(out var statistics)
                        .ToArray();

                    Assert.True(statistics.IsStale);
                    Assert.True(statistics.DurationInMs >= 0); // not from cache
                    Assert.NotEqual(previousResultEtag, statistics.ResultEtag);
                    Assert.Equal(1, terms.Length);
                    Assert.Contains(null, terms.Select(x => x.City));

                    previousResultEtag = statistics.ResultEtag;
                }

                store.Maintenance.Send(new StartIndexingOperation());

                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var terms = session.Query<MapReduceIndexResult, TimeSeries_Index_With_CompareExchange_MapReduce>()
                        .Statistics(out var statistics)
                        .ToArray();

                    Assert.False(statistics.IsStale);
                    Assert.True(statistics.DurationInMs >= 0); // not from cache
                    Assert.NotEqual(previousResultEtag, statistics.ResultEtag);
                    Assert.Equal(1, terms.Length);
                    Assert.Contains("Torun", terms.Select(x => x.City));

                    previousResultEtag = statistics.ResultEtag;
                }

                store.Maintenance.Send(new StopIndexingOperation());

                // add doc and compare
                using (var session = store.OpenSession())
                {
                    var company = new Company { Name = "HR", ExternalId = "companies/hr" };
                    session.Store(company);
                    session.TimeSeriesFor(company, "HeartRate").Append(DateTime.Now, new double[] { 5 }, company.ExternalId);

                    session.SaveChanges();
                }

                using (var session = store.OpenSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                {
                    session.Advanced.ClusterTransaction.CreateCompareExchangeValue("companies/hr", new Address { City = "Cesarea" });

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var terms = session.Query<MapReduceIndexResult, TimeSeries_Index_With_CompareExchange_MapReduce>()
                        .Statistics(out var statistics)
                        .ToArray();

                    Assert.True(statistics.IsStale);
                    Assert.True(statistics.DurationInMs >= 0); // not from cache
                    Assert.NotEqual(previousResultEtag, statistics.ResultEtag);
                    Assert.Equal(1, terms.Length);
                    Assert.Contains("Torun", terms.Select(x => x.City));

                    previousResultEtag = statistics.ResultEtag;
                }

                store.Maintenance.Send(new StartIndexingOperation());

                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var terms = session.Query<MapReduceIndexResult, TimeSeries_Index_With_CompareExchange_MapReduce>()
                        .Statistics(out var statistics)
                        .ToArray();

                    Assert.False(statistics.IsStale);
                    Assert.True(statistics.DurationInMs >= 0); // not from cache
                    Assert.NotEqual(previousResultEtag, statistics.ResultEtag);
                    Assert.Equal(2, terms.Length);
                    Assert.Contains("Torun", terms.Select(x => x.City));
                    Assert.Contains("Cesarea", terms.Select(x => x.City));

                    previousResultEtag = statistics.ResultEtag;
                }

                store.Maintenance.Send(new StopIndexingOperation());

                // update
                using (var session = store.OpenSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                {
                    var value = session.Advanced.ClusterTransaction.GetCompareExchangeValue<Address>("companies/hr");
                    value.Value.City = "Hadera";

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var terms = session.Query<MapReduceIndexResult, TimeSeries_Index_With_CompareExchange_MapReduce>()
                        .Statistics(out var statistics)
                        .ToArray();

                    Assert.True(statistics.IsStale);
                    Assert.True(statistics.DurationInMs >= 0); // not from cache
                    Assert.NotEqual(previousResultEtag, statistics.ResultEtag);
                    Assert.Equal(2, terms.Length);
                    Assert.Contains("Torun", terms.Select(x => x.City));
                    Assert.Contains("Cesarea", terms.Select(x => x.City));

                    previousResultEtag = statistics.ResultEtag;
                }

                store.Maintenance.Send(new StartIndexingOperation());

                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var terms = session.Query<MapReduceIndexResult, TimeSeries_Index_With_CompareExchange_MapReduce>()
                        .Statistics(out var statistics)
                        .ToArray();

                    Assert.False(statistics.IsStale);
                    Assert.True(statistics.DurationInMs >= 0); // not from cache
                    Assert.NotEqual(previousResultEtag, statistics.ResultEtag);
                    Assert.Equal(2, terms.Length);
                    Assert.Contains("Torun", terms.Select(x => x.City));
                    Assert.Contains("Hadera", terms.Select(x => x.City));

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
                    var terms = session.Query<MapReduceIndexResult, TimeSeries_Index_With_CompareExchange_MapReduce>()
                        .Statistics(out var statistics)
                        .ToArray();

                    Assert.True(statistics.IsStale);
                    Assert.True(statistics.DurationInMs >= 0); // not from cache
                    Assert.NotEqual(previousResultEtag, statistics.ResultEtag);
                    Assert.Equal(2, terms.Length);
                    Assert.Contains("Torun", terms.Select(x => x.City));
                    Assert.Contains("Hadera", terms.Select(x => x.City));

                    previousResultEtag = statistics.ResultEtag;
                }

                store.Maintenance.Send(new StartIndexingOperation());

                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var terms = session.Query<MapReduceIndexResult, TimeSeries_Index_With_CompareExchange_MapReduce>()
                        .Statistics(out var statistics)
                        .ToArray();

                    Assert.False(statistics.IsStale);
                    Assert.True(statistics.DurationInMs >= 0); // not from cache
                    Assert.NotEqual(previousResultEtag, statistics.ResultEtag);
                    Assert.Equal(2, terms.Length);
                    Assert.Contains("Torun", terms.Select(x => x.City));
                    Assert.Contains(null, terms.Select(x => x.City));

                    previousResultEtag = statistics.ResultEtag;
                }

                // live add compare without stopping indexing
                using (var session = store.OpenSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                {
                    session.Advanced.ClusterTransaction.CreateCompareExchangeValue("companies/hr", new Address { City = "Tel Aviv" });

                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store, timeout: TimeSpan.FromSeconds(5));

                using (var session = store.OpenSession())
                {
                    var terms = session.Query<MapReduceIndexResult, TimeSeries_Index_With_CompareExchange_MapReduce>()
                        .Statistics(out var statistics)
                        .ToArray();

                    Assert.False(statistics.IsStale);
                    Assert.True(statistics.DurationInMs >= 0); // not from cache
                    Assert.NotEqual(previousResultEtag, statistics.ResultEtag);
                    Assert.Equal(2, terms.Length);
                    Assert.Contains("Torun", terms.Select(x => x.City));
                    Assert.Contains("Tel Aviv", terms.Select(x => x.City));

                    previousResultEtag = statistics.ResultEtag;
                }
            }
        }

        private class MapIndexResult
        {
            public string City { get; set; }
        }

        private class MapReduceIndexResult
        {
            public string City { get; set; }

            public int Count { get; set; }
        }

        private class Index_With_CompareExchange : AbstractIndexCreationTask<Company>
        {
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

        private class Index_With_CompareExchange_MapReduce : AbstractIndexCreationTask<Company, MapReduceIndexResult>
        {
            public Index_With_CompareExchange_MapReduce()
            {
                Map = companies => from c in companies
                                   let address = LoadCompareExchangeValue<Address>(c.ExternalId)
                                   select new
                                   {
                                       address.City,
                                       Count = 1
                                   };

                Reduce = results => from r in results
                                    group r by r.City into g
                                    select new
                                    {
                                        City = g.Key,
                                        Count = g.Sum(x => x.Count)
                                    };
            }
        }

        private class Index_With_CompareExchange_Simple : AbstractIndexCreationTask<Company>
        {
            public Index_With_CompareExchange_Simple()
            {
                Map = companies => from c in companies
                                   let city = LoadCompareExchangeValue<string>(c.ExternalId)
                                   select new
                                   {
                                       City = city
                                   };

                StoreAllFields(FieldStorage.Yes);
            }
        }

        private class Index_With_CompareExchange_Simple_JavaScript : AbstractJavaScriptIndexCreationTask
        {
            public Index_With_CompareExchange_Simple_JavaScript()
            {
                throw new NotSupportedException();
            }
                
            public Index_With_CompareExchange_Simple_JavaScript(string jsEngineType)
            {
                Maps = new HashSet<string>
                {
                    "map('Companies', function (c) { var city = cmpxchg(c.ExternalId); return { City: city };})",
                };

                Fields.Add(Constants.Documents.Indexing.Fields.AllFields, new IndexFieldOptions { Storage = FieldStorage.Yes });
            }
        }

        private class Index_With_CompareExchange_Simple_JsJint : Index_With_CompareExchange_Simple_JavaScript
        {
            public Index_With_CompareExchange_Simple_JsJint() : base("Jint")
            {
            }
        }

        private class Index_With_CompareExchange_Simple_JsV8 : Index_With_CompareExchange_Simple_JavaScript
        {
            public Index_With_CompareExchange_Simple_JsV8() : base("V8")
            {
            }
        }

        private class Index_With_CompareExchange_JavaScript : AbstractJavaScriptIndexCreationTask
        {
            public Index_With_CompareExchange_JavaScript()
            {
                throw new NotSupportedException();
            }
                
            public Index_With_CompareExchange_JavaScript(string jsEngineType)
            {
                var optChaining = jsEngineType == "Jint" ? "" : "?";
                Maps = new HashSet<string>
                {
                    $"map('Companies', function (c) {{ var address = cmpxchg(c.ExternalId); return {{ City: address{optChaining}.City }};}})",
                };

                Fields.Add(Constants.Documents.Indexing.Fields.AllFields, new IndexFieldOptions { Storage = FieldStorage.Yes });
            }
        }

        private class Index_With_CompareExchange_JsJint : Index_With_CompareExchange_JavaScript
        {
            public Index_With_CompareExchange_JsJint() : base("Jint")
            {
            }
        }

        private class Index_With_CompareExchange_JsV8 : Index_With_CompareExchange_JavaScript
        {
            public Index_With_CompareExchange_JsV8() : base("V8")
            {
            }
        }

        private class TimeSeries_Index_With_CompareExchange : AbstractTimeSeriesIndexCreationTask<Company>
        {
            public TimeSeries_Index_With_CompareExchange()
            {
                AddMap(
                    "HeartRate",
                    timeSeries => from ts in timeSeries
                                  from entry in ts.Entries
                                  let address = LoadCompareExchangeValue<Address>(entry.Tag)
                                  select new
                                  {
                                      address.City
                                  });
            }
        }

        private class Counters_Index_With_CompareExchange : AbstractCountersIndexCreationTask<Company>
        {
            public Counters_Index_With_CompareExchange()
            {
                AddMap(
                    "HeartRate",
                    counters => from counter in counters
                                let address = LoadCompareExchangeValue<Address>(counter.DocumentId)
                                select new
                                {
                                    address.City
                                });
            }
        }

        private class Counters_Index_With_CompareExchange_MapReduce : AbstractCountersIndexCreationTask<Company, MapReduceIndexResult>
        {
            public Counters_Index_With_CompareExchange_MapReduce()
            {
                AddMap(
                    "HeartRate",
                    counters => from counter in counters
                                let address = LoadCompareExchangeValue<Address>(counter.DocumentId)
                                select new
                                {
                                    address.City,
                                    Count = 1
                                });

                Reduce = results => from r in results
                                    group r by r.City into g
                                    select new
                                    {
                                        City = g.Key,
                                        Count = g.Sum(x => x.Count)
                                    };
            }
        }

        private class TimeSeries_Index_With_CompareExchange_MapReduce : AbstractTimeSeriesIndexCreationTask<Company, MapReduceIndexResult>
        {
            public TimeSeries_Index_With_CompareExchange_MapReduce()
            {
                AddMap(
                    "HeartRate",
                    timeSeries => from ts in timeSeries
                                  from entry in ts.Entries
                                  let address = LoadCompareExchangeValue<Address>(entry.Tag)
                                  select new
                                  {
                                      address.City,
                                      Count = 1
                                  });

                Reduce = results => from r in results
                                    group r by r.City into g
                                    select new
                                    {
                                        City = g.Key,
                                        Count = g.Sum(x => x.Count)
                                    };
            }
        }
    }
}
