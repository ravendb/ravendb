using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Indexes.TimeSeries;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Server.ServerWide.Context;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Tests.Infrastructure.Extensions;
using Tests.Infrastructure.Operations;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Client.Indexing.TimeSeries
{
    public class BasicTimeSeriesIndexes_JavaScript : RavenTestBase
    {
        public BasicTimeSeriesIndexes_JavaScript(ITestOutputHelper output) : base(output)
        {
        }

        private class MyTsIndex : AbstractJavaScriptTimeSeriesIndexCreationTask
        {
            public MyTsIndex()
            {
                Maps = new HashSet<string>
                {
                    @"timeSeries.map('Companies', 'HeartRate', function (ts) {
return ts.Entries.map(entry => ({
        HeartBeat: entry.Values[0],
        Date: new Date(entry.Timestamp.getFullYear(), entry.Timestamp.getMonth(), entry.Timestamp.getDate()),
        User: ts.DocumentId
    }));
})"
                };
            }
        }

        private class MyTsIndex_AllTimeSeries : AbstractJavaScriptTimeSeriesIndexCreationTask
        {
            public MyTsIndex_AllTimeSeries()
            {
                Maps = new HashSet<string>
                {
                    @"timeSeries.map('Companies', function (ts) {
return ts.Entries.map(entry => ({
        HeartBeat: entry.Values[0],
        Date: new Date(entry.Timestamp.getFullYear(), entry.Timestamp.getMonth(), entry.Timestamp.getDate()),
        User: ts.DocumentId
    }));
})"
                };
            }
        }

        private class MyTsIndex_AllDocs : AbstractJavaScriptTimeSeriesIndexCreationTask
        {
            public MyTsIndex_AllDocs()
            {
                Maps = new HashSet<string>
                {
                    @"timeSeries.map(function (ts) {
return ts.Entries.map(entry => ({
        HeartBeat: entry.Values[0],
        Name: ts.Name,
        Date: new Date(entry.Timestamp.getFullYear(), entry.Timestamp.getMonth(), entry.Timestamp.getDate()),
        User: ts.DocumentId
    }));
})"
                };
            }
        }

        private class MyTsIndex_Load : AbstractJavaScriptTimeSeriesIndexCreationTask
        {
            public MyTsIndex_Load()
            {
                Maps = new HashSet<string>
                {
                    @"timeSeries.map('Companies', 'HeartRate', function (ts) {
return ts.Entries.map(entry => ({
        HeartBeat: entry.Value,
        Date: new Date(entry.Timestamp.getFullYear(), entry.Timestamp.getMonth(), entry.Timestamp.getDate()),
        User: ts.DocumentId,
        Employee: load(entry.Tag, 'Employees').FirstName
    }));
})"
                };
            }
        }

        private class AverageHeartRateDaily_ByDateAndUser : AbstractJavaScriptTimeSeriesIndexCreationTask
        {
            public class Result
            {
                public double HeartBeat { get; set; }

                public DateTime Date { get; set; }

                public string User { get; set; }

                public long Count { get; set; }
            }

            public AverageHeartRateDaily_ByDateAndUser()
            {
                Maps = new HashSet<string>
                {
                    @"timeSeries.map('Users', 'HeartRate', function (ts) {
return ts.Entries.map(entry => ({
        HeartBeat: entry.Value,
        Date: new Date(entry.Timestamp.getFullYear(), entry.Timestamp.getMonth(), entry.Timestamp.getDate()),
        User: ts.DocumentId,
        Count: 1
    }));
})"
                };

                Reduce = @"groupBy(r => ({ Date: r.Date, User: r.User }))
                             .aggregate(g => ({
                                 HeartBeat: g.values.reduce((total, val) => val.HeartBeat + total, 0) / g.values.reduce((total, val) => val.Count + total, 0),
                                 Date: g.key.Date,
                                 User: g.key.User
                                 Count: g.values.reduce((total, val) => val.Count + total, 0)
                             }))";
            }
        }

        private class AverageHeartRateDaily_ByDateAndCity : AbstractJavaScriptTimeSeriesIndexCreationTask
        {
            public class Result
            {
                public double HeartBeat { get; set; }

                public DateTime Date { get; set; }

                public string City { get; set; }

                public long Count { get; set; }
            }

            public AverageHeartRateDaily_ByDateAndCity()
            {
                Maps = new HashSet<string>
                {
                    @"timeSeries.map('Users', 'HeartRate', function (ts) {
return ts.Entries.map(entry => ({
        HeartBeat: entry.Value,
        Date: new Date(entry.Timestamp.getFullYear(), entry.Timestamp.getMonth(), entry.Timestamp.getDate()),
        City: load(entry.Tag, 'Addresses').City,
        Count: 1
    }));
})"
                };

                Reduce = @"groupBy(r => ({ Date: r.Date, City: r.City }))
                             .aggregate(g => ({
                                 HeartBeat: g.values.reduce((total, val) => val.HeartBeat + total, 0) / g.values.reduce((total, val) => val.Count + total, 0),
                                 Date: g.key.Date,
                                 City: g.key.City
                                 Count: g.values.reduce((total, val) => val.Count + total, 0)
                             }))";
            }
        }

        private class MyMultiMapTsIndex : AbstractJavaScriptTimeSeriesIndexCreationTask
        {
            public class Result
            {
                public double HeartBeat { get; set; }

                public DateTime Date { get; set; }

                public string User { get; set; }
            }

            public MyMultiMapTsIndex()
            {
                Maps = new HashSet<string>
                {
                    @"timeSeries.map('Companies', 'HeartRate', function (ts) {
return ts.Entries.map(entry => ({
        HeartBeat: entry.Values[0],
        Date: new Date(entry.Timestamp.getFullYear(), entry.Timestamp.getMonth(), entry.Timestamp.getDate()),
        User: ts.DocumentId
    }));
})",
                    @"timeSeries.map('Companies', 'HeartRate2', function (ts) {
return ts.Entries.map(entry => ({
        HeartBeat: entry.Values[0],
        Date: new Date(entry.Timestamp.getFullYear(), entry.Timestamp.getMonth(), entry.Timestamp.getDate()),
        User: ts.DocumentId
    }));
})",
                    @"timeSeries.map('Users', 'HeartRate', function (ts) {
return ts.Entries.map(entry => ({
        HeartBeat: entry.Values[0],
        Date: new Date(entry.Timestamp.getFullYear(), entry.Timestamp.getMonth(), entry.Timestamp.getDate()),
        User: ts.DocumentId
    }));
})",
                };
            }
        }

        private class Companies_ByTimeSeriesNames : AbstractJavaScriptIndexCreationTask
        {
            public Companies_ByTimeSeriesNames()
            {
                Maps = new HashSet<string>
                {
                    @"map('Companies', function (company) {
return ({
    Names: timeSeriesNamesFor(company)
})
})"
                };
            }
        }

        [RavenTheory(RavenTestCategory.Indexes | RavenTestCategory.TimeSeries)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All)]
        public void BasicMapIndex(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                var now1 = RavenTestHelper.UtcToday;
                var now2 = now1.AddSeconds(1);

                using (var session = store.OpenSession())
                {
                    var company = new Company();
                    session.Store(company, "companies/1");
                    session.TimeSeriesFor(company, "HeartRate").Append(now1, new double[] { 7 }, "tag");

                    session.SaveChanges();
                }

                store.Maintenance.ForTesting(() => new StopIndexingOperation()).ExecuteOnAll();

                var timeSeriesIndex = new MyTsIndex();
                timeSeriesIndex.Execute(store);

                AssertIsStale(store, "MyTsIndex");

                store.Maintenance.ForTesting(() => new StartIndexingOperation()).ExecuteOnAll();

                Indexes.WaitForIndexing(store);

                AssertIsNotStale(store, "MyTsIndex");

                store.Maintenance.ForTesting(() => new StopIndexingOperation()).ExecuteOnAll();

                using (var session = store.OpenSession())
                {
                    var company = session.Load<Company>("companies/1");
                    session.TimeSeriesFor(company, "HeartRate").Append(now2, new double[] { 3 }, "tag");

                    session.SaveChanges();
                }

                AssertIsStale(store, "MyTsIndex");

                store.Maintenance.ForTesting(() => new StartIndexingOperation()).ExecuteOnAll();

                Indexes.WaitForIndexing(store);

                AssertIsNotStale(store, "MyTsIndex");

                store.Maintenance.ForTesting(() => new StopIndexingOperation()).ExecuteOnAll();

                if (options.DatabaseMode == RavenDatabaseMode.Single)
                {
                    Assert.Equal(2, WaitForValue(() => store.Maintenance.Send(new GetIndexStatisticsOperation("MyTsIndex")).EntriesCount, 2));
                }

                var terms = store.Maintenance.Send(new GetTermsOperation("MyTsIndex", "HeartBeat", null));
                Assert.Equal(2, terms.Length);
                Assert.Contains("7", terms);
                Assert.Contains("3", terms);

                terms = store.Maintenance.Send(new GetTermsOperation("MyTsIndex", "Date", null));
                Assert.Equal(1, terms.Length);
                Assert.Equal(now1.Date, DateTime.Parse(terms[0]), RavenTestHelper.DateTimeComparer.Instance);

                terms = store.Maintenance.Send(new GetTermsOperation("MyTsIndex", "User", null));
                Assert.Equal(1, terms.Length);
                Assert.Contains("companies/1", terms);
            
                // delete time series

                using (var session = store.OpenSession())
                {
                    var company = session.Load<Company>("companies/1");
                    session.TimeSeriesFor(company, "HeartRate").Delete(now2);

                    session.SaveChanges();
                }

                AssertIsStale(store, "MyTsIndex");

                store.Maintenance.ForTesting(() => new StartIndexingOperation()).ExecuteOnAll();

                Indexes.WaitForIndexing(store);

                AssertIsNotStale(store, "MyTsIndex");

                terms = store.Maintenance.Send(new GetTermsOperation("MyTsIndex", "HeartBeat", null));
                Assert.Equal(1, terms.Length);
                Assert.Contains("7", terms);
                // delete document

                store.Maintenance.ForTesting(() => new StopIndexingOperation()).ExecuteOnAll();

                using (var session = store.OpenSession())
                {
                    session.Delete("companies/1");
                    session.SaveChanges();
                }

                AssertIsStale(store, "MyTsIndex");

                store.Maintenance.ForTesting(() => new StartIndexingOperation()).ExecuteOnAll();

                Indexes.WaitForIndexing(store);

                AssertIsNotStale(store, "MyTsIndex");

                terms = store.Maintenance.Send(new GetTermsOperation("MyTsIndex", "HeartBeat", null));
                Assert.Equal(0, terms.Length);
                // delete document - this time don't stop indexing to make sure doc deletion will be noticed by the index

                using (var session = store.OpenSession())
                {
                    var company = new Company();
                    session.Store(company, "companies/2");
                    session.TimeSeriesFor(company, "HeartRate").Append(now1, new double[] { 9 }, "tag");

                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);

                terms = store.Maintenance.Send(new GetTermsOperation("MyTsIndex", "HeartBeat", null));
                Assert.Equal(1, terms.Length);
                Assert.Contains("9", terms);

                using (var session = store.OpenSession())
                {
                    session.Delete("companies/2");
                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);

                AssertIsNotStale(store, "MyTsIndex");

                terms = store.Maintenance.Send(new GetTermsOperation("MyTsIndex", "HeartBeat", null));
                Assert.Equal(0, terms.Length);
            }
        }

        [RavenTheory(RavenTestCategory.Indexes | RavenTestCategory.TimeSeries)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public async Task BasicMapIndexWithLoad(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                var now1 = DateTime.Now;
                var now2 = now1.AddSeconds(1);

                using (var session = store.OpenSession())
                {
                    var employee = new Employee
                    {
                        FirstName = "John"
                    };
                    session.Store(employee, "employees/1");

                    var company = new Company();
                    session.Store(company, "companies/1");

                    session.TimeSeriesFor(company, "HeartRate").Append(now1, new double[] { 7 }, employee.Id);

                    var company2 = new Company();
                    session.Store(company2, "companies/11");

                    session.TimeSeriesFor(company2, "HeartRate").Append(now1, new double[] { 11 }, employee.Id);

                    session.SaveChanges();
                }

                store.Maintenance.Send(new StopIndexingOperation());

                var timeSeriesIndex = new MyTsIndex_Load();
                var indexName = timeSeriesIndex.IndexName;
                var indexDefinition = timeSeriesIndex.CreateIndexDefinition();

                timeSeriesIndex.Execute(store);

                var staleness = store.Maintenance.Send(new GetIndexStalenessOperation(indexName));
                Assert.True(staleness.IsStale);
                Assert.Equal(1, staleness.StalenessReasons.Count);
                Assert.True(staleness.StalenessReasons.Any(x => x.Contains("There are still")));

                store.Maintenance.Send(new StartIndexingOperation());

                Indexes.WaitForIndexing(store);

                staleness = store.Maintenance.Send(new GetIndexStalenessOperation(indexName));
                Assert.False(staleness.IsStale);

                store.Maintenance.Send(new StopIndexingOperation());

                Assert.Equal(2, WaitForValue(() => store.Maintenance.Send(new GetIndexStatisticsOperation(indexName)).EntriesCount, 2));

                var terms = store.Maintenance.Send(new GetTermsOperation(indexName, "Employee", null));
                Assert.Equal(1, terms.Length);
                Assert.Contains("john", terms);

                using (var session = store.OpenSession())
                {
                    var employee = session.Load<Employee>("employees/1");
                    employee.FirstName = "Bob";

                    session.SaveChanges();
                }

                staleness = store.Maintenance.Send(new GetIndexStalenessOperation(indexName));
                Assert.True(staleness.IsStale);
                Assert.Equal(1, staleness.StalenessReasons.Count);
                Assert.True(staleness.StalenessReasons.Any(x => x.Contains("There are still")));

                store.Maintenance.Send(new StartIndexingOperation());

                Indexes.WaitForIndexing(store);

                staleness = store.Maintenance.Send(new GetIndexStalenessOperation(indexName));
                Assert.False(staleness.IsStale);

                store.Maintenance.Send(new StopIndexingOperation());

                Assert.Equal(2, WaitForValue(() => store.Maintenance.Send(new GetIndexStatisticsOperation(indexName)).EntriesCount, 2));

                terms = store.Maintenance.Send(new GetTermsOperation(indexName, "Employee", null));
                Assert.Equal(1, terms.Length);
                Assert.Contains("bob", terms);

                using (var session = store.OpenSession())
                {
                    session.Delete("employees/1");

                    session.SaveChanges();
                }

                staleness = store.Maintenance.Send(new GetIndexStalenessOperation(indexName));
                Assert.True(staleness.IsStale);
                Assert.Equal(1, staleness.StalenessReasons.Count);
                Assert.True(staleness.StalenessReasons.Any(x => x.Contains("There are still")));

                store.Maintenance.Send(new StartIndexingOperation());

                Indexes.WaitForIndexing(store);

                Assert.Equal(2, WaitForValue(() => store.Maintenance.Send(new GetIndexStatisticsOperation(indexName)).EntriesCount, 2));

                terms = store.Maintenance.Send(new GetTermsOperation(indexName, "Employee", null));
                Assert.Equal(0, terms.Length);

                // delete source document

                store.Maintenance.Send(new StopIndexingOperation());

                using (var session = store.OpenSession())
                {
                    session.Delete("companies/1");

                    session.SaveChanges();
                }

                staleness = store.Maintenance.Send(new GetIndexStalenessOperation(indexName));
                Assert.True(staleness.IsStale);
                Assert.Equal(1, staleness.StalenessReasons.Count);
                Assert.True(staleness.StalenessReasons.Any(x => x.Contains("There are still")));

                store.Maintenance.Send(new StartIndexingOperation());

                Indexes.WaitForIndexing(store);

                var database = await GetDatabase(store.Database);
                var index = database.IndexStore.GetIndex(indexName);

                using (index._contextPool.AllocateOperationContext(out TransactionOperationContext context))
                using (var tx = context.OpenReadTransaction())
                {
                    var counts = index._indexStorage.ReferencesForDocuments.GetReferenceTablesCount("Companies", tx);

                    Assert.Equal(1, counts.ReferenceTableCount);
                    Assert.Equal(1, counts.CollectionTableCount);
                }

                using (var session = store.OpenSession())
                {
                    session.Delete("companies/11");

                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);

                Assert.Equal(0, WaitForValue(() => store.Maintenance.Send(new GetIndexStatisticsOperation(indexName)).EntriesCount, 0));

                using (index._contextPool.AllocateOperationContext(out TransactionOperationContext context))
                using (var tx = context.OpenReadTransaction())
                {
                    var counts = index._indexStorage.ReferencesForDocuments.GetReferenceTablesCount("Companies", tx);

                    Assert.Equal(0, counts.ReferenceTableCount);
                    Assert.Equal(0, counts.CollectionTableCount);
                }
            }
        }

        [RavenTheory(RavenTestCategory.Indexes | RavenTestCategory.TimeSeries)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void BasicMapReduceIndex(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                var today = RavenTestHelper.UtcToday;
                var tomorrow = today.AddDays(1);

                using (var session = store.OpenSession())
                {
                    var user = new User();
                    session.Store(user, "users/1");

                    for (int i = 0; i < 10; i++)
                    {
                        session.TimeSeriesFor(user, "HeartRate").Append(today.AddHours(i), new double[] { 180 + i }, "abc");
                    }

                    session.SaveChanges();
                }

                store.Maintenance.Send(new StopIndexingOperation());

                var timeSeriesIndex = new AverageHeartRateDaily_ByDateAndUser();
                var indexName = timeSeriesIndex.IndexName;
                var indexDefinition = timeSeriesIndex.CreateIndexDefinition();

                timeSeriesIndex.Execute(store);

                var staleness = store.Maintenance.Send(new GetIndexStalenessOperation(indexName));
                Assert.True(staleness.IsStale);
                Assert.Equal(1, staleness.StalenessReasons.Count);
                Assert.True(staleness.StalenessReasons.Any(x => x.Contains("There are still")));

                store.Maintenance.Send(new StartIndexingOperation());

                Indexes.WaitForIndexing(store);

                staleness = store.Maintenance.Send(new GetIndexStalenessOperation(indexName));
                Assert.False(staleness.IsStale);

                var terms = store.Maintenance.Send(new GetTermsOperation(indexName, "HeartBeat", null));
                Assert.Equal(1, terms.Length);
                Assert.Contains("184.5", terms);

                terms = store.Maintenance.Send(new GetTermsOperation(indexName, "Date", null));
                Assert.Equal(1, terms.Length);
                Assert.Equal(today.Date, DateTime.Parse(terms[0]), RavenTestHelper.DateTimeComparer.Instance);

                terms = store.Maintenance.Send(new GetTermsOperation(indexName, "User", null));
                Assert.Equal(1, terms.Length);
                Assert.Contains("users/1", terms);

                terms = store.Maintenance.Send(new GetTermsOperation(indexName, "Count", null));
                Assert.Equal(1, terms.Length);
                Assert.Equal("10", terms[0]);

                store.Maintenance.Send(new StopIndexingOperation());

                // add more heart rates
                using (var session = store.OpenSession())
                {
                    var user = session.Load<User>("users/1");

                    for (int i = 0; i < 20; i++)
                    {
                        session.TimeSeriesFor(user, "HeartRate").Append(tomorrow.AddHours(i), new double[] { 200 + i }, "abc");
                    }

                    session.SaveChanges();
                }

                staleness = store.Maintenance.Send(new GetIndexStalenessOperation(indexName));
                Assert.True(staleness.IsStale);
                Assert.Equal(1, staleness.StalenessReasons.Count);
                Assert.True(staleness.StalenessReasons.Any(x => x.Contains("There are still")));

                store.Maintenance.Send(new StartIndexingOperation());

                Indexes.WaitForIndexing(store);

                staleness = store.Maintenance.Send(new GetIndexStalenessOperation(indexName));
                Assert.False(staleness.IsStale);

                Assert.Equal(2, WaitForValue(() => store.Maintenance.Send(new GetIndexStatisticsOperation(indexName)).EntriesCount, 2));

                terms = store.Maintenance.Send(new GetTermsOperation(indexName, "HeartBeat", null));
                Assert.Equal(2, terms.Length);
                Assert.Contains("184.5", terms);
                Assert.Contains("209.5", terms);

                terms = store.Maintenance.Send(new GetTermsOperation(indexName, "Date", null));
                Assert.Equal(2, terms.Length);
                Assert.Equal(today.Date, DateTime.Parse(terms[0]), RavenTestHelper.DateTimeComparer.Instance);
                Assert.Equal(tomorrow.Date, DateTime.Parse(terms[1]), RavenTestHelper.DateTimeComparer.Instance);

                terms = store.Maintenance.Send(new GetTermsOperation(indexName, "User", null));
                Assert.Equal(1, terms.Length);
                Assert.Contains("users/1", terms);

                terms = store.Maintenance.Send(new GetTermsOperation(indexName, "Count", null));
                Assert.Equal(2, terms.Length);
                Assert.Contains("10", terms);
                Assert.Contains("20", terms);

                store.Maintenance.Send(new StopIndexingOperation());

                //// delete some time series

                using (var session = store.OpenSession())
                {
                    var user = session.Load<User>("users/1");

                    for (int i = 0; i < 10; i++)
                    {
                        session.TimeSeriesFor(user, "HeartRate").Delete(today.AddHours(i));
                        session.TimeSeriesFor(user, "HeartRate").Delete(tomorrow.AddHours(i));
                    }

                    session.SaveChanges();
                }

                staleness = store.Maintenance.Send(new GetIndexStalenessOperation(indexName));
                Assert.True(staleness.IsStale);
                Assert.Equal(1, staleness.StalenessReasons.Count);
                Assert.True(staleness.StalenessReasons.Any(x => x.Contains("There are still")));

                store.Maintenance.Send(new StartIndexingOperation());

                Indexes.WaitForIndexing(store);

                staleness = store.Maintenance.Send(new GetIndexStalenessOperation(indexName));
                Assert.False(staleness.IsStale);

                terms = store.Maintenance.Send(new GetTermsOperation(indexName, "HeartBeat", null));
                Assert.Equal(1, terms.Length);
                Assert.Contains("214.5", terms);

                terms = store.Maintenance.Send(new GetTermsOperation(indexName, "Date", null));
                Assert.Equal(1, terms.Length);
                Assert.Equal(tomorrow.Date, DateTime.Parse(terms[0]), RavenTestHelper.DateTimeComparer.Instance);

                terms = store.Maintenance.Send(new GetTermsOperation(indexName, "User", null));
                Assert.Equal(1, terms.Length);
                Assert.Contains("users/1", terms);

                terms = store.Maintenance.Send(new GetTermsOperation(indexName, "Count", null));
                Assert.Equal(1, terms.Length);
                Assert.Equal("10", terms[0]);

                //// delete document

                store.Maintenance.Send(new StopIndexingOperation());

                using (var session = store.OpenSession())
                {
                    session.Delete("users/1");
                    session.SaveChanges();
                }

                staleness = store.Maintenance.Send(new GetIndexStalenessOperation(indexName));
                Assert.True(staleness.IsStale);
                Assert.Equal(1, staleness.StalenessReasons.Count);
                Assert.True(staleness.StalenessReasons.Any(x => x.Contains("There are still")));

                store.Maintenance.Send(new StartIndexingOperation());

                Indexes.WaitForIndexing(store);

                staleness = store.Maintenance.Send(new GetIndexStalenessOperation(indexName));
                Assert.False(staleness.IsStale);

                terms = store.Maintenance.Send(new GetTermsOperation(indexName, "HeartBeat", null));
                Assert.Equal(0, terms.Length);

                WaitForUserToContinueTheTest(store);
            }
        }

        [RavenTheory(RavenTestCategory.Indexes | RavenTestCategory.TimeSeries)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public async Task BasicMapReduceIndexWithLoad(Options options)
        {
            {
                using (var store = GetDocumentStore(options))
                {
                    var today = RavenTestHelper.UtcToday;

                    using (var session = store.OpenSession())
                    {
                        var address = new Address { City = "NY" };

                        session.Store(address, "addresses/1");

                        var user = new User();

                        user.AddressId = address.Id;

                        session.Store(user, "users/1");

                        for (int i = 0; i < 10; i++)
                        {
                            session.TimeSeriesFor(user, "HeartRate").Append(today.AddHours(i), new double[] { 180 + i }, address.Id);
                        }

                        session.SaveChanges();
                    }

                    store.Maintenance.Send(new StopIndexingOperation());

                    var timeSeriesIndex = new AverageHeartRateDaily_ByDateAndCity();
                    var indexName = timeSeriesIndex.IndexName;
                    var indexDefinition = timeSeriesIndex.CreateIndexDefinition();

                    timeSeriesIndex.Execute(store);

                    var staleness = store.Maintenance.Send(new GetIndexStalenessOperation(indexName));
                    Assert.True(staleness.IsStale);
                    Assert.Equal(1, staleness.StalenessReasons.Count);
                    Assert.True(staleness.StalenessReasons.Any(x => x.Contains("There are still")));

                    store.Maintenance.Send(new StartIndexingOperation());

                    Indexes.WaitForIndexing(store);

                    staleness = store.Maintenance.Send(new GetIndexStalenessOperation(indexName));
                    Assert.False(staleness.IsStale);

                    var terms = store.Maintenance.Send(new GetTermsOperation(indexName, "HeartBeat", null));
                    Assert.Equal(1, terms.Length);
                    Assert.Contains("184.5", terms);

                    terms = store.Maintenance.Send(new GetTermsOperation(indexName, "Date", null));
                    Assert.Equal(1, terms.Length);
                    Assert.Equal(today.Date, DateTime.Parse(terms[0]), RavenTestHelper.DateTimeComparer.Instance);

                    terms = store.Maintenance.Send(new GetTermsOperation(indexName, "City", null));
                    Assert.Equal(1, terms.Length);
                    Assert.Contains("ny", terms);

                    terms = store.Maintenance.Send(new GetTermsOperation(indexName, "Count", null));
                    Assert.Equal(1, terms.Length);
                    Assert.Equal("10", terms[0]);

                    store.Maintenance.Send(new StopIndexingOperation());

                    using (var session = store.OpenSession())
                    {
                        var address = session.Load<Address>("addresses/1");
                        address.City = "LA";

                        session.SaveChanges();
                    }

                    staleness = store.Maintenance.Send(new GetIndexStalenessOperation(indexName));
                    Assert.True(staleness.IsStale);
                    Assert.Equal(1, staleness.StalenessReasons.Count);
                    Assert.True(staleness.StalenessReasons.Any(x => x.Contains("There are still")));

                    store.Maintenance.Send(new StartIndexingOperation());

                    Indexes.WaitForIndexing(store);

                    staleness = store.Maintenance.Send(new GetIndexStalenessOperation(indexName));
                    Assert.False(staleness.IsStale);

                    terms = store.Maintenance.Send(new GetTermsOperation(indexName, "City", null));
                    Assert.Equal(1, terms.Length);
                    Assert.Contains("la", terms);

                    store.Maintenance.Send(new StopIndexingOperation());

                    using (var session = store.OpenSession())
                    {
                        session.Delete("addresses/1");

                        session.SaveChanges();
                    }

                    staleness = store.Maintenance.Send(new GetIndexStalenessOperation(indexName));
                    Assert.True(staleness.IsStale);
                    Assert.Equal(1, staleness.StalenessReasons.Count);
                    Assert.True(staleness.StalenessReasons.Any(x => x.Contains("There are still")));

                    store.Maintenance.Send(new StartIndexingOperation());

                    Indexes.WaitForIndexing(store);

                    terms = store.Maintenance.Send(new GetTermsOperation(indexName, "City", null));
                    Assert.Equal(1, terms.Length);
                    Assert.Equal("NULL_VALUE", terms[0]);

                    // delete source document

                    store.Maintenance.Send(new StopIndexingOperation());

                    using (var session = store.OpenSession())
                    {
                        session.Delete("users/1");

                        session.SaveChanges();
                    }

                    staleness = store.Maintenance.Send(new GetIndexStalenessOperation(indexName));
                    Assert.True(staleness.IsStale);
                    Assert.Equal(1, staleness.StalenessReasons.Count);
                    Assert.True(staleness.StalenessReasons.Any(x => x.Contains("There are still")));

                    store.Maintenance.Send(new StartIndexingOperation());

                    Indexes.WaitForIndexing(store);

                    var database = await GetDatabase(store.Database);
                    var index = database.IndexStore.GetIndex(indexName);

                    using (index._contextPool.AllocateOperationContext(out TransactionOperationContext context))
                    using (var tx = context.OpenReadTransaction())
                    {
                        var counts = index._indexStorage.ReferencesForDocuments.GetReferenceTablesCount("Companies", tx);

                        Assert.Equal(0, counts.ReferenceTableCount);
                        Assert.Equal(0, counts.CollectionTableCount);
                    }
                }
            }
        }

        [RavenTheory(RavenTestCategory.Indexes | RavenTestCategory.TimeSeries)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void CanMapAllTimeSeriesFromCollection(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                var now1 = DateTime.Now;
                var now2 = now1.AddSeconds(1);

                using (var session = store.OpenSession())
                {
                    var company = new Company();
                    session.Store(company, "companies/1");
                    session.TimeSeriesFor(company, "HeartRate").Append(now1, new double[] { 7 }, "tag1");
                    session.TimeSeriesFor(company, "Likes").Append(now1, new double[] { 3 }, "tag2");

                    session.SaveChanges();
                }

                store.Maintenance.Send(new StopIndexingOperation());

                new MyTsIndex_AllTimeSeries().Execute(store);

                var staleness = store.Maintenance.Send(new GetIndexStalenessOperation("MyTsIndex/AllTimeSeries"));
                Assert.True(staleness.IsStale);
                Assert.Equal(1, staleness.StalenessReasons.Count);
                Assert.True(staleness.StalenessReasons.Any(x => x.Contains("There are still")));

                store.Maintenance.Send(new StartIndexingOperation());

                Indexes.WaitForIndexing(store);

                staleness = store.Maintenance.Send(new GetIndexStalenessOperation("MyTsIndex/AllTimeSeries"));
                Assert.False(staleness.IsStale);

                var terms = store.Maintenance.Send(new GetTermsOperation("MyTsIndex/AllTimeSeries", "HeartBeat", null));
                Assert.Equal(2, terms.Length);
                Assert.Contains("7", terms);
                Assert.Contains("3", terms);

                store.Maintenance.Send(new StopIndexingOperation());

                using (var session = store.OpenSession())
                {
                    var company = session.Load<Company>("companies/1");
                    session.TimeSeriesFor(company, "HeartRate").Append(now2, new double[] { 2 }, "tag");

                    session.SaveChanges();
                }

                staleness = store.Maintenance.Send(new GetIndexStalenessOperation("MyTsIndex/AllTimeSeries"));
                Assert.True(staleness.IsStale);
                Assert.Equal(1, staleness.StalenessReasons.Count);
                Assert.True(staleness.StalenessReasons.Any(x => x.Contains("There are still")));

                store.Maintenance.Send(new StartIndexingOperation());

                Indexes.WaitForIndexing(store);

                staleness = store.Maintenance.Send(new GetIndexStalenessOperation("MyTsIndex/AllTimeSeries"));
                Assert.False(staleness.IsStale);

                terms = store.Maintenance.Send(new GetTermsOperation("MyTsIndex/AllTimeSeries", "HeartBeat", null));
                Assert.Equal(3, terms.Length);
                Assert.Contains("7", terms);
                Assert.Contains("3", terms);
                Assert.Contains("2", terms);

                store.Maintenance.Send(new StopIndexingOperation());

                using (var session = store.OpenSession())
                {
                    var company = session.Load<Company>("companies/1");
                    session.TimeSeriesFor(company, "HeartRate").Delete(now1);

                    session.SaveChanges();
                }

                staleness = store.Maintenance.Send(new GetIndexStalenessOperation("MyTsIndex/AllTimeSeries"));
                Assert.True(staleness.IsStale);
                Assert.Equal(2, staleness.StalenessReasons.Count); // one for time series update and one for time series deleted range
                Assert.True(staleness.StalenessReasons.Any(x => x.Contains("There are still")));

                store.Maintenance.Send(new StartIndexingOperation());

                Indexes.WaitForIndexing(store);

                staleness = store.Maintenance.Send(new GetIndexStalenessOperation("MyTsIndex/AllTimeSeries"));
                Assert.False(staleness.IsStale);

                terms = store.Maintenance.Send(new GetTermsOperation("MyTsIndex/AllTimeSeries", "HeartBeat", null));
                Assert.Equal(2, terms.Length);
                Assert.Contains("3", terms);
                Assert.Contains("2", terms);

                store.Maintenance.Send(new StopIndexingOperation());

                using (var session = store.OpenSession())
                {
                    var company = session.Load<Company>("companies/1");
                    session.TimeSeriesFor(company, "HeartRate").Delete(now2);

                    session.SaveChanges();
                }

                staleness = store.Maintenance.Send(new GetIndexStalenessOperation("MyTsIndex/AllTimeSeries"));
                Assert.True(staleness.IsStale);
                Assert.Equal(1, staleness.StalenessReasons.Count);
                Assert.True(staleness.StalenessReasons.Any(x => x.Contains("There are still")));

                store.Maintenance.Send(new StartIndexingOperation());

                Indexes.WaitForIndexing(store);

                staleness = store.Maintenance.Send(new GetIndexStalenessOperation("MyTsIndex/AllTimeSeries"));
                Assert.False(staleness.IsStale);

                terms = store.Maintenance.Send(new GetTermsOperation("MyTsIndex/AllTimeSeries", "HeartBeat", null));
                Assert.Equal(1, terms.Length);
                Assert.Contains("3", terms);

                store.Maintenance.Send(new StopIndexingOperation());

                using (var session = store.OpenSession())
                {
                    session.Delete("companies/1");

                    session.SaveChanges();
                }

                staleness = store.Maintenance.Send(new GetIndexStalenessOperation("MyTsIndex/AllTimeSeries"));
                Assert.True(staleness.IsStale);
                Assert.Equal(1, staleness.StalenessReasons.Count);
                Assert.True(staleness.StalenessReasons.Any(x => x.Contains("There are still")));

                store.Maintenance.Send(new StartIndexingOperation());

                Indexes.WaitForIndexing(store);

                staleness = store.Maintenance.Send(new GetIndexStalenessOperation("MyTsIndex/AllTimeSeries"));
                Assert.False(staleness.IsStale);

                terms = store.Maintenance.Send(new GetTermsOperation("MyTsIndex/AllTimeSeries", "HeartBeat", null));
                Assert.Equal(0, terms.Length);
            }
        }

        [RavenTheory(RavenTestCategory.Indexes | RavenTestCategory.TimeSeries)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All)]
        public void CanMapAllTimeSeries(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                var now1 = DateTime.Now;
                var now2 = now1.AddSeconds(1);

                using (var session = store.OpenSession())
                {
                    var company = new Company();
                    session.Store(company, "companies/1");
                    session.TimeSeriesFor(company, "HeartRate").Append(now1, new double[] { 7 }, "tag1");
                    session.TimeSeriesFor(company, "Likes").Append(now1, new double[] { 3 }, "tag2");

                    var employee = new Employee();
                    session.Store(employee, "employees/1");
                    session.TimeSeriesFor(employee, "Dislikes").Append(now1, new double[] { 1 }, "tag3");

                    session.SaveChanges();
                }

                store.Maintenance.ForTesting(() => new StopIndexingOperation()).ExecuteOnAll();

                new MyTsIndex_AllDocs().Execute(store);

                AssertIsStale(store, "MyTsIndex/AllDocs");

                store.Maintenance.ForTesting(() => new StartIndexingOperation()).ExecuteOnAll();

                Indexes.WaitForIndexing(store);

                AssertIsNotStale(store, "MyTsIndex/AllDocs");

                var terms = store.Maintenance.Send(new GetTermsOperation("MyTsIndex/AllDocs", "HeartBeat", null));
                Assert.Equal(3, terms.Length);
                Assert.Contains("7", terms);
                Assert.Contains("3", terms);
                Assert.Contains("1", terms);

                terms = store.Maintenance.Send(new GetTermsOperation("MyTsIndex/AllDocs", "Name", null));
                Assert.Equal(3, terms.Length);
                Assert.Contains("heartrate", terms);
                Assert.Contains("likes", terms);
                Assert.Contains("dislikes", terms);

                store.Maintenance.ForTesting(() => new StopIndexingOperation()).ExecuteOnAll();

                using (var session = store.OpenSession())
                {
                    var company = session.Load<Company>("companies/1");
                    session.TimeSeriesFor(company, "HeartRate").Append(now2, new double[] { 2 }, "tag");

                    session.SaveChanges();
                }

                AssertIsStale(store, "MyTsIndex/AllDocs");

                store.Maintenance.ForTesting(() => new StartIndexingOperation()).ExecuteOnAll();

                Indexes.WaitForIndexing(store);

                AssertIsNotStale(store, "MyTsIndex/AllDocs");

                terms = store.Maintenance.Send(new GetTermsOperation("MyTsIndex/AllDocs", "HeartBeat", null));
                Assert.Equal(4, terms.Length);
                Assert.Contains("7", terms);
                Assert.Contains("3", terms);
                Assert.Contains("1", terms);
                Assert.Contains("2", terms);

                terms = store.Maintenance.Send(new GetTermsOperation("MyTsIndex/AllDocs", "Name", null));
                Assert.Equal(3, terms.Length);
                Assert.Contains("heartrate", terms);
                Assert.Contains("likes", terms);
                Assert.Contains("dislikes", terms);

                store.Maintenance.ForTesting(() => new StopIndexingOperation()).ExecuteOnAll();

                using (var session = store.OpenSession())
                {
                    var company = session.Load<Company>("companies/1");
                    session.TimeSeriesFor(company, "Likes").Delete(now1, now2);

                    session.SaveChanges();
                }

                AssertIsStale(store, "MyTsIndex/AllDocs");

                store.Maintenance.ForTesting(() => new StartIndexingOperation()).ExecuteOnAll();

                Indexes.WaitForIndexing(store);

                AssertIsNotStale(store, "MyTsIndex/AllDocs");

                terms = store.Maintenance.Send(new GetTermsOperation("MyTsIndex/AllDocs", "HeartBeat", null));
                Assert.Equal(3, terms.Length);
                Assert.Contains("7", terms);
                Assert.Contains("1", terms);
                Assert.Contains("2", terms);

                terms = store.Maintenance.Send(new GetTermsOperation("MyTsIndex/AllDocs", "Name", null));
                Assert.Equal(2, terms.Length);
                Assert.Contains("heartrate", terms);
                Assert.Contains("dislikes", terms);

                store.Maintenance.ForTesting(() => new StopIndexingOperation()).ExecuteOnAll();

                using (var session = store.OpenSession())
                {
                    session.Delete("companies/1");

                    session.SaveChanges();
                }

                AssertIsStale(store, "MyTsIndex/AllDocs");

                store.Maintenance.ForTesting(() => new StartIndexingOperation()).ExecuteOnAll();

                Indexes.WaitForIndexing(store);

                AssertIsNotStale(store, "MyTsIndex/AllDocs");

                terms = store.Maintenance.Send(new GetTermsOperation("MyTsIndex/AllDocs", "HeartBeat", null));
                Assert.Equal(1, terms.Length);
                Assert.Contains("1", terms);

                terms = store.Maintenance.Send(new GetTermsOperation("MyTsIndex/AllDocs", "Name", null));
                Assert.Equal(1, terms.Length);
                Assert.Contains("dislikes", terms);

                // now checking live updates (not stopping indexing)

                using (var session = store.OpenSession())
                {
                    var company = session.Load<Company>("employees/1");
                    session.TimeSeriesFor(company, "Dislikes").Append(now2, new double[] { 9 }, "tag");

                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);

                AssertIsNotStale(store, "MyTsIndex/AllDocs");

                terms = store.Maintenance.Send(new GetTermsOperation("MyTsIndex/AllDocs", "HeartBeat", null));
                Assert.Equal(2, terms.Length);
                Assert.Contains("1", terms);
                Assert.Contains("9", terms);

                terms = store.Maintenance.Send(new GetTermsOperation("MyTsIndex/AllDocs", "Name", null));
                Assert.Equal(1, terms.Length);
                Assert.Contains("dislikes", terms);

                using (var session = store.OpenSession())
                {
                    session.Delete("employees/1");

                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);

                AssertIsNotStale(store, "MyTsIndex/AllDocs");

                terms = store.Maintenance.Send(new GetTermsOperation("MyTsIndex/AllDocs", "HeartBeat", null));
                Assert.Equal(0, terms.Length);

                terms = store.Maintenance.Send(new GetTermsOperation("MyTsIndex/AllDocs", "Name", null));
                Assert.Equal(0, terms.Length);
            }
        }

        [RavenTheory(RavenTestCategory.Querying | RavenTestCategory.Indexes | RavenTestCategory.TimeSeries | RavenTestCategory.JavaScript)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All)]
        public async Task BasicMultiMapIndex(Options options)
        {
            var now = DateTime.UtcNow.Date;

            using (var store = GetDocumentStore(options))
            {
                var timeSeriesIndex = new MyMultiMapTsIndex();
                await timeSeriesIndex.ExecuteAsync(store);

                using (var session = store.OpenSession())
                {
                    var company = new Company();
                    session.Store(company);

                    session.TimeSeriesFor(company, "HeartRate").Append(now, new[] { 2.5d }, "tag1");
                    session.TimeSeriesFor(company, "HeartRate2").Append(now, new[] { 3.5d }, "tag2");

                    var user = new User();
                    session.Store(user);
                    session.TimeSeriesFor(user, "HeartRate").Append(now, new[] { 4.5d }, "tag3");

                    session.SaveChanges();
                }

                await Indexes.WaitForIndexingAsync(store);

                using (var session = store.OpenSession())
                {
                    var results = session.Query<MyMultiMapTsIndex.Result, MyMultiMapTsIndex>()
                        .ToList();

                    Assert.Equal(3, results.Count);
                }

                using (var session = store.OpenAsyncSession())
                {
                    var results = await session.Query<MyMultiMapTsIndex.Result, MyMultiMapTsIndex>()
                        .ToListAsync();

                    Assert.Equal(3, results.Count);
                }

                using (var session = store.OpenSession())
                {
                    var results = session.Advanced.DocumentQuery<MyMultiMapTsIndex.Result, MyMultiMapTsIndex>()
                        .ToList();

                    Assert.Equal(3, results.Count);
                }

                using (var session = store.OpenAsyncSession())
                {
                    var results = await session.Advanced.AsyncDocumentQuery<MyMultiMapTsIndex.Result, MyMultiMapTsIndex>()
                        .ToListAsync();

                    Assert.Equal(3, results.Count);
                }

                using (var session = store.OpenAsyncSession())
                {
                    var results = await session.Advanced.AsyncDocumentQuery<MyMultiMapTsIndex.Result, MyMultiMapTsIndex>()
                        .OrderByDescending(x => x.HeartBeat)
                        .ToListAsync();

                    var orderedResults = results.OrderByDescending(x => x.HeartBeat);

                    Assert.Equal(3, results.Count);
                    Assert.Equal(orderedResults, results);
                }
            }
        }

        [RavenTheory(RavenTestCategory.Indexes | RavenTestCategory.TimeSeries)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.Lucene)]
        public void TimeSeriesNamesFor(Options options)
        {
            var now = DateTime.UtcNow.Date;

            using (var store = GetDocumentStore(options))
            {
                var index = new Companies_ByTimeSeriesNames();
                index.Execute(store);

                using (var session = store.OpenSession())
                {
                    var company = new Company();
                    session.Store(company, "companies/1");

                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);
                RavenTestHelper.AssertNoIndexErrors(store);
                WaitForUserToContinueTheTest(store);
                var terms = store.Maintenance.Send(new GetTermsOperation(index.IndexName, "Names", null));
                Assert.Equal(0, terms.Length);

                if (options.SearchEngineMode == RavenSearchEngineMode.Lucene)
                {
                    terms = store.Maintenance.Send(new GetTermsOperation(index.IndexName, "Names_IsArray", null));
                    Assert.Equal(1, terms.Length);
                    Assert.Contains("true", terms);
                }

                using (var session = store.OpenSession())
                {
                    var company = session.Load<Company>("companies/1");

                    session.TimeSeriesFor(company, "HeartRate").Append(now, new[] { 2.5d }, "tag1");
                    session.TimeSeriesFor(company, "HeartRate2").Append(now, new[] { 3.5d }, "tag2");

                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);
                RavenTestHelper.AssertNoIndexErrors(store);

                terms = store.Maintenance.Send(new GetTermsOperation(index.IndexName, "Names", null));
                Assert.Equal(2, terms.Length);
                Assert.Contains("heartrate", terms);
                Assert.Contains("heartrate2", terms);

                terms = store.Maintenance.Send(new GetTermsOperation(index.IndexName, "Names_IsArray", null));
                Assert.Equal(1, terms.Length);
                Assert.Contains("true", terms);
            }
        }

        void AssertIsStale(DocumentStore store, string indexName)
        {
            store.Maintenance.ForTesting(() => new GetIndexStalenessOperation(indexName))
                .AssertAny((key, staleness) =>
                {
                    Assert.True(staleness.IsStale);
                    Assert.Equal(1, staleness.StalenessReasons.Count);
                    Assert.True(staleness.StalenessReasons.Any(x => x.Contains("There are still")));
                });
        }

        void AssertIsNotStale(DocumentStore store, string indexName)
        {
            store.Maintenance.ForTesting(() => new GetIndexStalenessOperation(indexName))
                .AssertAll((key, staleness) => { Assert.False(staleness.IsStale); });
        }
    }
}
